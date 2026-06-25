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
    blob_table::BlobTable,
    client::Client,
    configuration::RuntimeConfiguration,
    database_build::DatabaseBuild,
    mass::to_float as mass_to_float,
    mass_to_int,
    monitoring::{MetricTarget, Monitoring, TracingLogRotation, TracingTarget},
    peptide::{Peptide, Peptidoform},
    peptide_search::{MultiTaskSearch, PeptideSearchType, Search, UnionAllSearch},
    peptide_table::PeptideTable,
    post_translational_modification::{PTMCollection, PostTranslationalModification},
    protease::{Protease, Trypsin},
    protein_table::ProteinTable,
    sequence::{IsBitSequence, IsSimpleSequence, PeptideSequence},
    stats_table::StatsTable,
};
use macpepdb_tui::{MetricConfig, Tui, TuiHandle};
use postgres_types::ToSql;
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
    #[error("Blob table error: {0}")]
    BlobTable(Box<macpepdb::blob_table::Error>),
    #[error("Client error: {0}")]
    Client(#[from] macpepdb::client::Error),
    #[error("Database build error: {0}")]
    DatabaseBuild(Box<macpepdb::database_build::Error>),
    #[error("Glob pattern error: {0}")]
    GlobPattern(#[from] glob::PatternError),
    #[error("Glob error: {0}")]
    Glob(#[from] glob::GlobError),
    #[error("Missing mass in partitioning, this indicates he peptide is not in the database.")]
    MissingMass,
    #[error("Missing mass count in stats table. Are you sure the database was build correctly?")]
    MissingMassCount,
    #[error(
        "Missing runtime configuration in blob table. Are you sure the database was build correctly?"
    )]
    MissingRuntimeConfiguration,
    #[error("Sequence error: {0}")]
    Sequence(Box<macpepdb::sequence::Error>),
    #[error("Peptide not found")]
    PeptideNotFound,
    #[error("Peptide table error: {0}")]
    PeptideTable(#[from] macpepdb::peptide_table::Error),
    #[error("proteins_memory_limit should be between 0.0 and 1.0")]
    ProteinsMemoryLimit,
    #[error("Protein table error: {0}")]
    ProteinTable(#[from] macpepdb::protein_table::Error),
    #[error("Stats table error: {0}")]
    StatsTable(Box<macpepdb::stats_table::Error>),
}

#[derive(Subcommand)]
enum ConfigCommand {
    Show,
    Fix,
}

#[derive(Subcommand)]
enum PeptideCommand {
    Show {
        /// Sequence to select
        sequence: String,
    },
}

#[derive(Subcommand)]
enum Command {
    /// Web api
    Api {
        // Optional and default arguments
        #[arg(long, default_value_t = NonZeroUsize::new(16).unwrap())]
        concurrent_searches: NonZeroUsize,
        /// Type of search to perform, multi-task search can be faster but also more memory intensive
        #[arg(long, default_value_t = PeptideSearchType::MultiTask)]
        search_type: PeptideSearchType,
        // Positional default arguments
        #[arg(default_value_t = SocketAddr::from_str("127.0.0.1:8080").unwrap())]
        socket: SocketAddr,
    },
    /// Build the database
    Build {
        // Optional and default arguments
        /// Concurrent number of inserts for non-partitioned batches to insert
        #[arg(long, default_value_t = NonZeroUsize::new(100).unwrap())]
        concurrent_batch_size: NonZeroUsize,
        /// This controls how large CQL-batch inserts can be in kb.
        #[arg(short, long, default_value_t = NonZeroUsize::new(512).unwrap())]
        batch_size_limit: NonZeroUsize,
        /// If set, peptides with unknown amino acid (X) are kept. Be aware that X has no mass.
        #[arg(short, long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        keep_unknown: bool,
        /// Directory for the mass-index build scratch files (scattered buckets + the on-disk
        /// protein_ids store). Place it on a roomy disk; the whole tree is removed after the build.
        /// Defaults to the system temp directory.
        #[arg(long)]
        build_scratch_dir: Option<PathBuf>,
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
        /// Fraction of free memory to use as limit for keeping proteins in memory.
        /// Keeping the proteins in memory can significantly speed up the digestion.
        /// The mass index no longer competes for this RAM (it spills to disk under
        /// `--build-scratch-dir`); set this to 0.0 to read proteins from the database instead,
        /// which lowers peak memory further at the cost of slower digestion.
        #[arg(long, default_value_t = 0.8)]
        proteins_memory_limit: f64,
        /// If set protein inserstion will be skipped
        #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        skip_proteins: bool,
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
        /// Maximum variable modification considered per peptides
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
        /// Taxonomy IDs to filter for, can be used multiple times, if not set, all taxonomy IDs are included
        #[arg(short, long, action = clap::ArgAction::Append)]
        taxonomy_ids: Vec<i32>,
        /// Concurrent searches of condition
        #[arg(long, default_value_t = NonZeroUsize::new(16).unwrap())]
        threads: NonZeroUsize,
        /// Type of search to perform, multi-task search can be faster but also more memory intensive
        #[arg(long, default_value_t = PeptideSearchType::UnionAll)]
        r#type: PeptideSearchType,
        /// Upper mass tolerance in PPM
        #[arg(short, long, default_value_t = 10)]
        upper_mass_tolerance_ppm: i64,
        // Positional arguments
        /// Canonical mass to search for
        mass: f64,
        /// Path to output file
        output_file_path: PathBuf,
    },
    Peptide {
        #[command(subcommand)]
        command: PeptideCommand,
    },
    /// Prints stats
    Stats {},
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
    /// Database URL, format `postgresql://[<user>[:<url-safe-password>]@]<host[:<port>]>[,<host[:<port>]>...]/<dbname>[?pool_size=N&...]`
    #[arg(short, long, default_value_t = String::from("postgresql://postgres@127.0.0.1:5432/postgres"))]
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
async fn main() -> Result<(), Error> {
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
            search_type,
            socket,
        } => {
            let client = Client::new(&cli.database_url).await.unwrap();

            macpepdb::web::server::start(
                client,
                socket,
                false,
                concurrent_searches,
                search_type,
                None,
                Box::pin(shutdown_signal()),
            )
            .await
            .unwrap();
        }
        Command::Build {
            concurrent_batch_size,
            batch_size_limit,
            keep_unknown,
            build_scratch_dir,
            max_length,
            min_length,
            max_missed_cleavages,
            print_config,
            protease,
            protein_file_paths,
            proteins_memory_limit,
            skip_proteins,
            skip_protein_associations,
            skip_taxonomies,
            threads,
        } => {
            if !(0.0..=1.0).contains(&proteins_memory_limit) {
                return Err(Error::ProteinsMemoryLimit);
            }

            let scratch_dir = build_scratch_dir.unwrap_or_else(std::env::temp_dir);

            let client = Arc::new(Client::new(&cli.database_url).await.unwrap());
            let protein_file_paths =
                convert_str_paths_and_resolve_globs(protein_file_paths).unwrap();

            tracing::info!("Resolved {} protein files", protein_file_paths.len());

            let protease = Protease::by_name(
                &protease,
                Some(min_length),
                Some(max_length),
                max_missed_cleavages,
                keep_unknown,
            )
            .unwrap();

            let configuration = DatabaseBuild::new(
                client,
                &protein_file_paths,
                protease,
                batch_size_limit,
                concurrent_batch_size,
                proteins_memory_limit,
                skip_proteins,
                skip_protein_associations,
                skip_taxonomies,
                threads,
                scratch_dir,
                tui.as_ref(),
            )
            .start()
            .await
            .map_err(|err| Error::DatabaseBuild(Box::new(err)))?;

            if let Some(mut tui) = tui {
                tracing::info!("Done. Press Ctrl+C or q to exit.");
                tui.wait().await;
            }

            if let Some(print_config_path) = print_config.as_ref() {
                tokio::fs::write(
                    print_config_path,
                    serde_json::to_string_pretty(&configuration).unwrap(),
                )
                .await
                .unwrap();
            }
        }
        Command::Config { command } => match command {
            ConfigCommand::Show => {
                let client = Client::new(&cli.database_url).await.unwrap();
                let configuration: RuntimeConfiguration =
                    BlobTable::select(&client, RuntimeConfiguration::BLOB_KEY)
                        .await
                        .unwrap()
                        .unwrap();
                println!("{}", serde_json::to_string_pretty(&configuration).unwrap());
            }
            ConfigCommand::Fix => {
                macpepdb::temp::convert_old_runtime_config_to_new(&cli.database_url).await
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
            r#type,
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
                r#type,
                upper_mass_tolerance_ppm,
                mass,
                output_file_path,
                tui.as_ref(),
            )
            .await;

            // Keep TUI open so the user can review logs before exiting with q or Ctrl+C
            if let Some(mut tui) = tui {
                tui.wait().await;
            }
        }
        Command::Peptide { command } => match command {
            PeptideCommand::Show { sequence } => {
                let client = Arc::new(Client::new(&cli.database_url).await.unwrap());
                let config: RuntimeConfiguration =
                    BlobTable::select(client.as_ref(), RuntimeConfiguration::BLOB_KEY)
                        .await
                        .map_err(|err| Error::BlobTable(Box::new(err)))?
                        .ok_or(Error::MissingRuntimeConfiguration)?;

                let sequence = PeptideSequence::try_from(sequence.as_str())
                    .map_err(|err| Error::Sequence(Box::new(err)))?;

                let mass =
                    Peptide::peptide_mass_from_amino_acid_bits(sequence.amino_acid_bit_codes());

                let partitions: Vec<i64> = config
                    .mass_partitioning()
                    .partition_by_mass(mass)
                    .map(|(_, partition)| partition)
                    .collect();

                if partitions.is_empty() {
                    return Err(Error::MissingMass);
                }

                let params: Vec<Box<dyn ToSql + Sync + Send>> =
                    vec![Box::new(partitions), Box::new(mass)];

                let peptide = PeptideTable::new(client)
                    .select("WHERE partition = ANY($1) AND mass = $2 LIMIT 1", params)
                    .await?
                    .next()
                    .await
                    .ok_or(Error::PeptideNotFound)??;

                if cli.terminal || cli.tui {
                    tracing::info!("{peptide}");
                } else {
                    println!("{peptide}");
                }
            }
        },
        Command::Stats {} => {
            let client = Arc::new(Client::new(&cli.database_url).await?);
            let protein_count = ProteinTable::new(client.clone()).count().await?;
            let peptide_count = PeptideTable::new(client.clone()).count().await?;
            let mass_count = StatsTable::new(client.clone())
                .select_mass_count()
                .await
                .map_err(|e| Error::StatsTable(Box::new(e)))?
                .ok_or(Error::MissingMassCount)?;
            if cli.terminal || cli.tui {
                tracing::info!("protein count: {protein_count}");
                tracing::info!("peptide count: {peptide_count}");
                tracing::info!("mass count: {mass_count}");
            } else {
                println!("protein count: {protein_count}");
                println!("peptide count: {peptide_count}");
                println!("mass count: {mass_count}");
            }

            if let Some(mut tui) = tui {
                tracing::info!("Done. Press Ctrl+C or q to exit.");
                tui.wait().await;
            }
        }
    }

    Ok(())
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
    r#type: PeptideSearchType,
    upper_mass_tolerance_ppm: i64,
    mass: i64,
    output_file_path: PathBuf,
    tui: Option<&TuiHandle>,
) {
    let configuration: Arc<RuntimeConfiguration> = Arc::new(
        BlobTable::select(client.as_ref(), RuntimeConfiguration::BLOB_KEY)
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

    let mut peptide_stream = match r#type {
        PeptideSearchType::MultiTask => MultiTaskSearch::search(
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
        .unwrap(),
        PeptideSearchType::UnionAll => UnionAllSearch::search(
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
        .unwrap(),
    };

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
