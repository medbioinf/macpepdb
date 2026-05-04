use std::{
    collections::HashMap,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
};

use clap::Parser;
use macpepdb::{
    blob::Blob,
    client::Client,
    mass_counter::MassCounter,
    mass_index::MassIndex,
    mass_partitioning::MassPartitioning,
    monitoring::{MetricTarget, Monitoring, TracingLogRotation, TracingTarget},
    peptide_table::PeptideTable,
    protease::Protease,
    protein_table::ProteinTable,
    sequence::{IsSequence, PeptideSequence},
};
use macpepdb_tui::{MetricConfig, Tui, TuiHandle};
use url::Url;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional path to save index
    // #[arg(short, long)]
    // index_file_path: Option<PathBuf>,
    /// Database URL, format `scylla://[<user:string>[:<url-safe-password:string>]@]<host[:<port>]>[,<host[:<port>]>...]/<keyspace>`
    #[arg(short, long, default_value_t = String::from("scylla://127.0.0.1:9042,127.0.0.1:9043/macpepdb"))]
    database_url: String,
    /// Batch size of records to insert concurrently
    #[arg(short, long, default_value_t = NonZeroUsize::new(100).unwrap())]
    insert_batch_size: NonZeroUsize,
    /// Path to optional log file
    #[arg(long)]
    log_file: Option<PathBuf>,
    #[arg(long, default_value_t = TracingLogRotation::Daily)]
    log_rotation: TracingLogRotation,
    /// Endpoint for Loki API to send logs to
    #[arg(long)]
    loki: Option<Url>,
    /// Label to  distinguish logs from this app instance in Loki
    #[arg(long, default_value_t = String::from(env!("CARGO_CRATE_NAME")))]
    loki_label: String,
    /// Number of mass partition
    #[arg(short, long, default_value_t = NonZeroU16::new(1000).unwrap())]
    partitions: NonZeroU16,
    /// Socket for prometheus collection endpoint
    #[arg(long)]
    prometheus: Option<SocketAddr>,
    /// Batch size of peptides to insert
    #[arg(long, default_value_t = NonZeroUsize::new(1000).unwrap())]
    protien_reader_cache_size: NonZeroUsize,
    /// Flag to show tracing on the stdout, no tui, no metrics
    #[arg(long, default_value_t = false, conflicts_with = "tui")]
    terminal: bool,
    /// Flag to show a terminal UI for tracing and metics
    #[arg(long, default_value_t = false, conflicts_with = "terminal")]
    tui: bool,
    /// Batch size of records to insert concurrently
    #[arg(short, long, default_value_t = NonZeroUsize::new(16).unwrap())]
    threads: NonZeroUsize,
    /// Increases log level each time it is used. 10 and above will activating level tracing for all crates included.
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    /// Protein files
    #[arg(value_delimiter = ' ', num_args = 0..)]
    protein_file_paths: Vec<PathBuf>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let client = Arc::new(Client::new(&cli.database_url).await.unwrap());
    let protease = Protease::get_by_name(
        "trypsin",
        Some(PeptideSequence::MIN_LENGTH.get()),
        Some(PeptideSequence::MAX_LENGTH.get()),
        Some(2),
    )
    .unwrap();

    let mut tracing_targets: Vec<TracingTarget> = Vec::new();
    let mut metric_targets: Vec<MetricTarget> = Vec::new();

    let tui = cli.tui.then(|| {
        let tui = Tui::builder().title(env!("CARGO_CRATE_NAME")).build();
        tracing_targets.push(TracingTarget::Tui(tui.layer()));
        metric_targets.push(MetricTarget::Tui(tui.recorder()));
        tui.run_raw()
    });

    if cli.terminal {
        tracing_targets.push(TracingTarget::Terminal);
    }

    if let Some(log_file_path) = cli.log_file {
        tracing_targets.push(TracingTarget::File(
            log_file_path.clone(),
            cli.log_rotation.into(),
        ));
    }

    if let Some(loki_url) = cli.loki {
        tracing_targets.push(TracingTarget::Loki(loki_url, cli.loki_label));
    }

    if let Some(prometheus_socket) = cli.prometheus {
        metric_targets.push(MetricTarget::Prometheus(
            prometheus_socket,
            Box::pin(axum_shutdown_signal()),
        ));
    }

    let _monitoring = Monitoring::new(
        cli.verbose,
        tracing_targets.into_iter(),
        metric_targets.into_iter(),
        HashMap::new(),
    )
    .await
    .unwrap();

    build_db(
        client,
        &cli.protein_file_paths,
        cli.insert_batch_size,
        &protease,
        cli.threads,
        cli.partitions,
        tui.as_ref(),
    )
    .await;
}

async fn build_db(
    client: Arc<Client>,
    protein_file_paths: &[PathBuf],
    insert_batch_size: NonZeroUsize,
    protease: &Protease,
    num_threads: NonZeroUsize,
    num_partitions: NonZeroU16,
    tui: Option<&TuiHandle>,
) {
    // 1. set insert proteins
    if let Some(tui) = &tui {
        tui.add_metric(MetricConfig::rate(
            macpepdb::protein_table::INSERTED_PROTEINS_METRIC,
            "Inserted proteins",
        ));
    }
    let protein_ctr =
        build_db_proteins(client.clone(), protein_file_paths, insert_batch_size).await;
    if let Some(tui) = &tui {
        tui.remove_metric(macpepdb::protein_table::INSERTED_PROTEINS_METRIC);
    }

    // 2. step create mass to protein index
    if let Some(tui) = &tui {
        tui.add_metric(MetricConfig::progress(
            macpepdb::mass_index::PROGRESS_METRIC,
            macpepdb::mass_index::PROGRESS_METRIC,
            protein_ctr as f64,
        ));
    }
    let mass_index = build_db_mass_index(client.clone(), protease, num_threads).await;
    if let Some(tui) = &tui {
        tui.remove_metric(macpepdb::mass_index::PROGRESS_METRIC);
    }

    // 3. count masses for partitions
    if let Some(tui) = &tui {
        tui.add_metric(MetricConfig::progress(
            macpepdb::mass_counter::PROGESS_METRIC,
            macpepdb::mass_counter::PROGESS_METRIC,
            mass_index.len() as f64,
        ));
        tui.add_metric(MetricConfig::counter(
            macpepdb::mass_counter::PEPTIDES_METRIC,
            macpepdb::mass_counter::PEPTIDES_METRIC,
        ));
    }
    let mass_counter =
        build_db_mass_counter(client.clone(), &mass_index, protease, num_threads).await;
    if let Some(tui) = &tui {
        tui.remove_metric(macpepdb::mass_counter::PROGESS_METRIC);
        tui.remove_metric(macpepdb::mass_counter::PEPTIDES_METRIC);
    }

    let peptides_len = mass_counter.peptides_len();

    // 4. caluclate partitioning by going through masses and count peptides
    // partitioning is currently not implemented, let's see first if we need it.
    let partitioning = build_db_parititoning(
        client.clone(),
        mass_counter,
        insert_batch_size,
        num_partitions,
    )
    .await;

    // 5. go through masses and digest the proteins collect distinct peptides and upsert them with proteins
    if let Some(tui) = &tui {
        tui.add_metric(MetricConfig::progress(
            macpepdb::peptide_table::INSERTED_PEPTIDES_METRIC,
            macpepdb::peptide_table::INSERTED_PEPTIDES_METRIC,
            peptides_len as f64,
        ));
    }
    build_db_peptides(
        client.clone(),
        insert_batch_size,
        protease,
        &partitioning,
        mass_index,
        num_threads,
    )
    .await;
    if let Some(tui) = &tui {
        tui.remove_metric(macpepdb::peptide_table::INSERTED_PEPTIDES_METRIC);
    }

    // 6. Collect metadata
}

async fn build_db_proteins(
    client: Arc<Client>,
    protein_file_paths: &[PathBuf],
    insert_batch_size: NonZeroUsize,
) -> usize {
    let now = std::time::Instant::now();
    let protein_ctr = ProteinTable::new(client)
        .build(protein_file_paths.iter(), insert_batch_size)
        .await
        .unwrap();
    tracing::info!(
        "db proteins: time = {:.2?} s; #proteins = {protein_ctr}",
        now.elapsed().as_secs_f32(),
    );
    protein_ctr
}

async fn build_db_mass_index(
    client: Arc<Client>,
    protease: &Protease,
    num_threads: NonZeroUsize,
) -> MassIndex {
    let now = std::time::Instant::now();
    let index = MassIndex::build_concurrently(client.as_ref(), protease, num_threads)
        .await
        .unwrap();
    tracing::info!(
        "db mass index: time = {:.2?} s; #masses = {}",
        now.elapsed().as_secs_f32(),
        index.len()
    );

    index
}

async fn build_db_mass_counter(
    client: Arc<Client>,
    mass_index: &MassIndex,
    protease: &Protease,
    threads: NonZeroUsize,
) -> MassCounter {
    let now = std::time::Instant::now();

    let counter = MassCounter::count_concurrently(client.clone(), protease, mass_index, threads)
        .await
        .unwrap();

    tracing::info!(
        "db mass counter: time = {:.2?} s; #peptides = {}",
        now.elapsed().as_secs_f32(),
        counter.peptides_len()
    );

    counter
}

async fn build_db_parititoning(
    client: Arc<Client>,
    mass_counter: MassCounter,
    insert_batch_size: NonZeroUsize,
    num_partitions: NonZeroU16,
) -> MassPartitioning {
    let now = std::time::Instant::now();
    let partitioning = match Blob::select(client.as_ref(), MassPartitioning::BLOB_KEY)
        .await
        .unwrap()
    {
        Some(partitioning) => {
            tracing::info!("db partitioning: loaded from db;");
            partitioning
        }
        None => {
            let partitioning = MassPartitioning::build(mass_counter, num_partitions)
                .await
                .unwrap();
            Blob::insert(client.as_ref(), &partitioning, insert_batch_size)
                .await
                .unwrap();

            partitioning
        }
    };

    tracing::info!(
        "db partitioning: time = {:.2?} s;",
        now.elapsed().as_secs_f32(),
    );

    partitioning
}

async fn build_db_peptides(
    client: Arc<Client>,
    insert_batch_size: NonZeroUsize,
    protease: &Protease,
    partitioning: &MassPartitioning,
    mass_index: MassIndex,
    num_threads: NonZeroUsize,
) {
    let now = std::time::Instant::now();
    PeptideTable::new(client)
        .build_concurrently(
            protease,
            insert_batch_size,
            num_threads,
            partitioning,
            mass_index,
        )
        .await
        .unwrap();
    tracing::info!("db peptides = {:.2?} s;", now.elapsed().as_secs_f32(),);
}

/// Axum shutdown signal handler for ctrl-c and terminate signals
///
async fn axum_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
