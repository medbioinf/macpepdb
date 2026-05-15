use std::{
    collections::HashMap,
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use clap::{Parser, Subcommand};
use futures::StreamExt;
use macpepdb::{
    blob::Blob,
    client::Client,
    configuration::Configuration,
    mass::to_float as mass_to_float,
    mass_index::MassIndex,
    mass_to_int,
    monitoring::{MetricTarget, Monitoring, TracingLogRotation, TracingTarget},
    peptide::Peptidoform,
    peptide_search::{MultiTaskSearch, Search},
    peptide_table::PeptideTable,
    post_translational_modification::{PTMCollection, PostTranslationalModification},
    protease::{Protease, Trypsin},
    protein_table::ProteinTable,
    sequence::{IsBitSequence, PeptideSequence},
};
use macpepdb_tui::{MetricConfig, Tui, TuiHandle};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use url::Url;

// Allocator
// jemalloc
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// mimalloc
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// tcmalloc
#[cfg(feature = "tcmalloc")]
use tcmalloc2::TcMalloc;

#[cfg(feature = "tcmalloc")]
#[global_allocator]
static GLOBAL: TcMalloc = TcMalloc;

#[derive(Debug, Error)]
enum Error {
    #[error("Glob pattern error: {0}")]
    GlobPattern(#[from] glob::PatternError),
    #[error("Glob error: {0}")]
    Glob(#[from] glob::GlobError),
}

#[derive(Subcommand)]
enum ConfigCommand {
    Show,
}

#[derive(Subcommand)]
enum Command {
    /// Web api
    Api {
        // Optional and default arguments
        #[arg(long, default_value_t = NonZeroUsize::new(16).unwrap())]
        concurrent_searches: NonZeroUsize,

        #[arg(default_value_t = SocketAddr::from_str("127.0.0.1:8080").unwrap())]
        socket: SocketAddr,
    },
    /// Build the database
    Build {
        // Optional and default arguments
        /// Concurrent number of inserts for non-partitioned batches to insert
        #[arg(long, default_value_t = NonZeroUsize::new(100).unwrap())]
        concurrent_batch_size: NonZeroUsize,
        /// commitlog_segment_size_in_mb as set in the cassandra cluster settingss.
        /// This controls how large CQL-batch inserts can be.
        #[arg(short, long, default_value_t = NonZeroUsize::new(16).unwrap())]
        commitlog_segment_size_in_mb: NonZeroUsize,
        /// If set, peptides with unknown amino acid (X) ar kept. Be aware that X has no mass.
        #[arg(short, long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        keep_unknown: bool,
        /// Max peptide length
        #[arg(long, default_value_t = PeptideSequence::MAX_LENGTH)]
        max_length: NonZeroUsize,
        /// Min peptide length
        #[arg(long, default_value_t = NonZeroUsize::new(6).unwrap())]
        min_length: NonZeroUsize,
        /// Missed cleavages
        #[arg(short, long)]
        max_missed_cleavages: Option<usize>,
        /// Print configuration to the given file
        #[arg(long)]
        print_config: Option<PathBuf>,
        /// Protease name
        #[arg(long, default_value_t = Trypsin::NAME.to_string())]
        protease: String,
        /// If set no taxonomies will be collected on peptide level
        #[arg(short, long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        skip_protein_associations: bool,
        /// If set no taxonomies will be collected on peptide level
        #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        skip_taxonomies: bool,
        /// Batch size of records to insert concurrently
        #[arg(short, long, default_value_t = NonZeroUsize::new(16).unwrap())]
        threads: NonZeroUsize,
        // Positional default arguments
        /// Protein files
        #[arg(value_delimiter = ' ', num_args = 0..)]
        protein_file_paths: Vec<String>,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Search for a specific mass
    Search {
        // Optional and default arguments
        /// Flag allows duplicates in results can lead to lower memory usage.
        #[arg(short, long, default_value_t = true, action = clap::ArgAction::SetTrue)]
        allow_duplicates: bool,
        /// Controlls which peptides are returned.
        /// true: Only SwissProt; false: Only TrEMBL
        #[arg(long)]
        is_reviewed: Option<bool>,
        /// Lower mass tolerance in PPM
        #[arg(short, long, default_value_t = 10)]
        lower_mass_tolerance_ppm: i64,
        /// Maximum variable modifiction considered per peptides
        #[arg(short, long, default_value_t = 3)]
        max_variable_modifications: usize,
        /// Optional PTM file format, TODO add format
        #[arg(short, long)]
        ptm_file_path: Option<PathBuf>,
        /// Stops returning ProForma compliant sequences
        /// and fall back to canonical sequences only
        #[arg(short, long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        only_canonical: bool,
        /// Proteome IDs to filter for, can be used multiple times, if not set, all proteome IDs are included
        #[arg(short, long, action = clap::ArgAction::Append)]
        proteome_ids: Vec<String>,
        /// Taxanomy IDs to filter for, can be used multiple times, if not set, all taxonomy IDs are included
        #[arg(short, long, action = clap::ArgAction::Append)]
        taxonomy_ids: Vec<i32>,
        /// Concurrent searches of condition
        #[arg(long, default_value_t = NonZeroUsize::new(16).unwrap())]
        threads: NonZeroUsize,
        /// Upper mass tolerance in PPM
        #[arg(short, long, default_value_t = 10)]
        upper_mass_tolerance_ppm: i64,

        // Positional arguments
        /// Canoncial mass to search for
        mass: f64,
        /// Path to output file
        output_file_path: PathBuf,
    },
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Flag to start tokio console API
    #[arg(long, default_value_t = false)]
    console: bool,
    /// Socket for tokio console API
    #[arg(long)]
    console_socket: Option<SocketAddr>,
    /// Database URL, format `scylla://[<user:string>[:<url-safe-password:string>]@]<host[:<port>]>[,<host[:<port>]>...]/<keyspace>`
    #[arg(short, long, default_value_t = String::from("scylla://127.0.0.1:9042,127.0.0.1:9043/macpepdb"))]
    database_url: String,
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
    /// Socket for prometheus collection endpoint
    #[arg(long)]
    prometheus: Option<SocketAddr>,
    /// Flag to show tracing on the stdout, no tui, no metrics
    #[arg(long, default_value_t = false, conflicts_with = "tui")]
    terminal: bool,
    /// If set
    #[arg(long)]
    metrics_to_logs: Option<u64>,
    /// Flag to show a terminal UI for tracing and metics
    #[arg(long, default_value_t = false, conflicts_with = "terminal")]
    tui: bool,
    /// Increases log level each time it is used. 10 and above will activating level tracing for all crates included.
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[command(subcommand)]
    command: Command,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

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

    #[cfg(feature = "tokio-console")]
    if cli.console {
        tracing_targets.push(TracingTarget::Console(cli.console_socket));
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
            Box::pin(shutdown_signal()),
        ));
    }

    if let Some(metrics_to_logs_period) = cli.metrics_to_logs {
        metric_targets.push(MetricTarget::Tracing(metrics_to_logs_period));
    }

    let _monitoring = Monitoring::new(
        cli.verbose,
        tracing_targets.into_iter(),
        metric_targets.into_iter(),
        HashMap::new(),
    )
    .await
    .unwrap();

    match cli.command {
        Command::Api {
            concurrent_searches,
            socket,
        } => {
            let client = Client::new(&cli.database_url).await.unwrap();

            macpepdb::web::server::start(
                client,
                socket,
                false,
                concurrent_searches,
                None,
                Box::pin(shutdown_signal()),
            )
            .await
            .unwrap();
        }
        Command::Build {
            concurrent_batch_size,
            commitlog_segment_size_in_mb,
            keep_unknown,
            max_length,
            min_length,
            max_missed_cleavages,
            print_config,
            protease,
            protein_file_paths,
            skip_protein_associations,
            skip_taxonomies,
            threads,
        } => {
            let client = Arc::new(Client::new(&cli.database_url).await.unwrap());
            let protein_file_paths =
                convert_str_paths_and_resolve_globs(protein_file_paths).unwrap();

            // let's use 3/4 of the max batch size limit of commitlog_segment_size_in_mb / 2 to accomodate for overhead
            let batch_size_limit =
                NonZeroUsize::new((commitlog_segment_size_in_mb.get() * 1000 * 1000 * 3) / (2 * 4))
                    .unwrap();

            tracing::info!(
                "Resolved protein files:\n\t, {}",
                protein_file_paths
                    .iter()
                    .map(|path| format!("{}", path.display()))
                    .collect::<Vec<String>>()
                    .join("\n\t")
            );

            let protease = Protease::by_name(
                &protease,
                Some(min_length),
                Some(max_length),
                max_missed_cleavages,
                keep_unknown,
            )
            .unwrap();

            build_db(
                client,
                &protein_file_paths,
                protease,
                batch_size_limit,
                concurrent_batch_size,
                skip_protein_associations,
                skip_taxonomies,
                threads,
                print_config,
                tui.as_ref(),
            )
            .await;

            tracing::error!(
                "Done. Hit ctrl-c twice to end. (yes this is a bug I need to solve. first is shutting down TUI, second is stoping the rest)"
            );
            shutdown_signal().await;
        }
        Command::Config { command } => match command {
            ConfigCommand::Show => {
                let client = Client::new(&cli.database_url).await.unwrap();
                let configuration: Configuration = Blob::select(&client, Configuration::BLOB_KEY)
                    .await
                    .unwrap()
                    .unwrap();
                println!("{}", serde_json::to_string_pretty(&configuration).unwrap());
            }
        },
        Command::Search {
            allow_duplicates,
            is_reviewed,
            lower_mass_tolerance_ppm,
            max_variable_modifications,
            ptm_file_path,
            only_canonical,
            proteome_ids,
            taxonomy_ids,
            threads,
            upper_mass_tolerance_ppm,
            mass,
            output_file_path,
        } => {
            let client = Arc::new(Client::new(&cli.database_url).await.unwrap());
            let mass = mass_to_int!(mass);

            peptide_search(
                client,
                allow_duplicates,
                is_reviewed,
                lower_mass_tolerance_ppm,
                max_variable_modifications,
                ptm_file_path,
                only_canonical,
                proteome_ids,
                taxonomy_ids,
                threads,
                upper_mass_tolerance_ppm,
                mass,
                output_file_path,
                tui.as_ref(),
            )
            .await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn build_db(
    client: Arc<Client>,
    protein_file_paths: &[PathBuf],
    protease: Protease,
    batch_size_limit: NonZeroUsize,
    concurrent_batch_size: NonZeroUsize,
    skip_protein_associations: bool,
    skip_taxonomies: bool,
    num_threads: NonZeroUsize,
    print_config: Option<PathBuf>,
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
        build_db_proteins(client.clone(), protein_file_paths, concurrent_batch_size).await;
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
    let mass_index = build_db_mass_index(client.clone(), &protease, num_threads).await;
    if let Some(tui) = &tui {
        tui.remove_metric(macpepdb::mass_index::PROGRESS_METRIC);
    }

    // 5. go through masses and digest the proteins collect distinct peptides and upsert them with proteins
    if let Some(tui) = &tui {
        tui.add_metric(MetricConfig::progress(
            macpepdb::peptide_table::PROGRESS_METRIC,
            macpepdb::peptide_table::PROGRESS_METRIC,
            mass_index.len() as f64,
        ));
        tui.add_metric(MetricConfig::counter(
            macpepdb::peptide_table::INSERTED_PEPTIDES_METRIC,
            macpepdb::peptide_table::INSERTED_PEPTIDES_METRIC,
        ));
        tui.add_metric(MetricConfig::gauge(
            macpepdb::peptide_table::QUEUE_METRIC,
            macpepdb::peptide_table::QUEUE_METRIC,
        ));
    }

    let mass_to_partitions_map = build_db_peptides(
        client.clone(),
        skip_protein_associations,
        skip_taxonomies,
        Arc::new(protease.clone()),
        batch_size_limit,
        mass_index,
        num_threads,
    )
    .await;
    if let Some(tui) = &tui {
        tui.remove_metric(macpepdb::peptide_table::PROGRESS_METRIC);
        tui.remove_metric(macpepdb::peptide_table::INSERTED_PEPTIDES_METRIC);
        tui.remove_metric(macpepdb::peptide_table::QUEUE_METRIC);
    }
    let configuration = Configuration::new(mass_to_partitions_map, protease);

    if let Some(print_config_path) = print_config {
        tokio::fs::write(
            print_config_path,
            serde_json::to_string_pretty(&configuration).unwrap(),
        )
        .await
        .unwrap();
    }

    Blob::insert(client.as_ref(), &configuration, concurrent_batch_size)
        .await
        .unwrap();
}

async fn build_db_proteins(
    client: Arc<Client>,
    protein_file_paths: &[PathBuf],
    concurrent_batch_size: NonZeroUsize,
) -> usize {
    let now = std::time::Instant::now();
    let protein_ctr = ProteinTable::new(client)
        .build(protein_file_paths.iter(), concurrent_batch_size)
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
    let index = MassIndex::build_concurrently(client, protease, num_threads)
        .await
        .unwrap();
    tracing::info!(
        "db mass index: time = {:.2?} s; #masses = {}",
        now.elapsed().as_secs_f32(),
        index.len()
    );

    index
}

async fn build_db_peptides(
    client: Arc<Client>,
    skip_protein_associations: bool,
    skip_taxonomies: bool,
    protease: Arc<Protease>,
    batch_size_limit: NonZeroUsize,
    mass_index: MassIndex,
    num_threads: NonZeroUsize,
) -> HashMap<i64, Vec<i64>> {
    let now = std::time::Instant::now();
    let mass_to_partitions_map = PeptideTable::new(client)
        .build_concurrently(
            skip_protein_associations,
            skip_taxonomies,
            protease,
            batch_size_limit,
            num_threads,
            mass_index,
        )
        .await
        .unwrap();
    tracing::info!("db peptides = {:.2?} s;", now.elapsed().as_secs_f32(),);

    mass_to_partitions_map
}

/// Axum shutdown signal handler for ctrl-c and terminate signals
///
async fn shutdown_signal() {
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

#[allow(clippy::too_many_arguments)]
async fn peptide_search(
    client: Arc<Client>,
    allow_duplicates: bool,
    is_reviewed: Option<bool>,
    lower_mass_tolerance_ppm: i64,
    max_variable_modifications: usize,
    ptm_file_path: Option<PathBuf>,
    only_canonical: bool,
    proteome_ids: Vec<String>,
    taxonomy_ids: Vec<i32>,
    threads: NonZeroUsize,
    upper_mass_tolerance_ppm: i64,
    mass: i64,
    output_file_path: PathBuf,
    tui: Option<&TuiHandle>,
) {
    let configuration: Arc<Configuration> = Arc::new(
        Blob::select(client.as_ref(), Configuration::BLOB_KEY)
            .await
            .unwrap()
            .unwrap(),
    );

    let taxonomy_ids = if taxonomy_ids.is_empty() {
        None
    } else {
        Some(taxonomy_ids)
    };
    let proteome_ids = if proteome_ids.is_empty() {
        None
    } else {
        Some(proteome_ids)
    };

    let ptms = ptm_file_path
        .map(|path| {
            csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .has_headers(true)
                .from_path(path)
                .unwrap()
                .deserialize()
                .map(|result| result.map(Arc::new))
                .collect::<Result<Vec<Arc<PostTranslationalModification>>, csv::Error>>()
                .unwrap()
        })
        .unwrap_or_default();

    let ptm_collection = Arc::new(PTMCollection::new(ptms).unwrap());

    tracing::info!("[main::peptide_search] PTMs {ptm_collection}");

    let mut outfile = tokio::io::BufWriter::new(
        tokio::fs::File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_file_path)
            .await
            .unwrap(),
    );

    let mut peptide_stream = MultiTaskSearch::search(
        client,
        configuration,
        mass,
        lower_mass_tolerance_ppm,
        upper_mass_tolerance_ppm,
        max_variable_modifications,
        !allow_duplicates,
        taxonomy_ids,
        proteome_ids,
        is_reviewed,
        ptm_collection,
        !only_canonical,
        threads,
    )
    .await
    .unwrap();

    if let Some(tui) = &tui {
        tui.add_metric(MetricConfig::counter(
            peptide_stream.matching_peptide_metric(),
            macpepdb::peptide_search::MATCHING_PEPTIDE_METRIC,
        ));
    }

    tracing::info!("[main::peptide_search] Start streaming peptides");

    let mut peptide_counter: usize = 0;
    while let Some(result) = peptide_stream.next().await {
        match result {
            Ok(peptidoforms) => {
                for peptidoform in peptidoforms {
                    write_peptidoform(&mut outfile, peptide_counter, peptidoform)
                        .await
                        .unwrap();
                    peptide_counter += 1;
                }
            }
            Err(e) => {
                tracing::error!("error searching for peptides: {e}");
            }
        }
        if peptide_counter.is_multiple_of(1000) {
            outfile.flush().await.unwrap();
        }
    }

    outfile.flush().await.unwrap();

    if let Some(tui) = &tui {
        tui.remove_metric(peptide_stream.matching_peptide_metric());
    }
}

async fn write_peptidoform<T: tokio::io::AsyncWrite + Unpin>(
    writer: &mut T,
    peptide_counter: usize,
    peptide: Peptidoform,
) -> Result<(), std::io::Error> {
    writer.write_all(b">mdb|").await?;
    writer
        .write_all(peptide_counter.to_string().as_bytes())
        .await?;
    writer.write_all(b"|").await?;
    writer
        .write_all(mass_to_float(peptide.mass()).to_string().as_bytes())
        .await?;
    writer.write_all(b"\n").await?;
    writer
        .write_all(peptide.sequence().to_string().as_bytes())
        .await?;
    writer.write_all(b"\n").await?;

    Ok(())
}

/// Converts a vector of strings to a vector of paths and resolves glob patterns.
///
/// # Arguments
/// * `paths` - Vector of paths as strings
///
fn convert_str_paths_and_resolve_globs(paths: Vec<String>) -> Result<Vec<PathBuf>, Error> {
    Ok(paths
        .into_iter()
        .map(|path| {
            if !path.contains("*") {
                // Return plain path in vector if no glob pattern is found
                Ok(vec![Path::new(&path).to_path_buf()])
            } else {
                // Resolve glob pattern and return array of paths
                Ok(glob::glob(&path)?
                    .map(|x| x.map_err(Error::Glob))
                    .collect::<Result<Vec<PathBuf>, Error>>()?)
            }
        })
        .collect::<Result<Vec<_>, Error>>()? // Collect and resolve errors from parsing/resolving
        .into_iter()
        .flatten() // flatten the vectors which
        .filter(|path| {
            path.is_file() // Filter out directories, only include files
        })
        .collect())
}
