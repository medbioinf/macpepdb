#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// std imports
use std::collections::HashSet;
use std::path::PathBuf;
use std::process;
use std::{path::Path, time::Duration};

// 3rd party imports
use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use dihardts_omicstools::proteomics::proteases::functions::get_by_name as get_protease_by_name;
use futures::StreamExt;
use glob::glob;
use metrics_exporter_prometheus::PrometheusBuilder;
use reqwest::Url;
use tokio::net::TcpListener;
use tokio::time::sleep;
use tracing::{debug, error, info, info_span, Instrument, Level};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

// internal imports
use macpepdb::database::generic_client::GenericClient;
use macpepdb::database::scylla::client::Client;
use macpepdb::database::scylla::peptide_table::PeptideTable;
use macpepdb::database::scylla::peptide_table::SELECT_COLS as PEPTIDE_SELECT_COLS;
use macpepdb::database::table::Table;
use macpepdb::entities::peptide::Peptide;
use macpepdb::tools::peptide_mass_counter::PeptideMassCounter;
use macpepdb::tools::peptide_partitioner::PeptidePartitioner;
use macpepdb::web::server::start as start_web_server;
use macpepdb::{
    database::scylla::database_build::DatabaseBuild as ScyllaBuild,
    entities::configuration::Configuration,
};

// Protease choices depends on dihardts_omicstools::proteomics::proteases::functions::ALL which is a str-vector.
// In order to use it with clap it needs to be an enum which is created via a template and build rs
include!(concat!(env!("OUT_DIR"), "/protease_choice.rs"));

/// Default min Peptide length
///
const DEFAULT_MIN_PEPTIDE_LENGTH: usize = 6;

/// Default max peptide length
///
const DEFAULT_MAX_PEPTIDE_LENGTH: usize = 50;

/// Default max number of missed cleavages
///
const DEFAULT_MAX_NUMBER_OF_MISSED_CLEAVAGES: usize = 2;

/// Default false positive probability for bloom filters
///
const DEFAULT_FALSE_POSITIVE_PROBABILITY: f64 = 0.01;

/// Default protease
///
const DEFAULT_PROTEASE: ProteaseChoice = ProteaseChoice::Trypsin;

/// Target for tracing
///
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum TracingTarget {
    Loki,
    File,
    Terminal,
    All,
}

/// Target for metrics
///
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum MetricTarget {
    Terminal,
    Prometheus,
    All,
}

/// Log rotation values for CLI
///
#[derive(clap::ValueEnum, Clone, Debug)]
enum TracingLogRotation {
    Minutely,
    Hourly,
    Daily,
    Never,
}

impl From<TracingLogRotation> for Rotation {
    fn from(rotation: TracingLogRotation) -> Self {
        match rotation {
            TracingLogRotation::Minutely => Rotation::MINUTELY,
            TracingLogRotation::Hourly => Rotation::HOURLY,
            TracingLogRotation::Daily => Rotation::DAILY,
            TracingLogRotation::Never => Rotation::NEVER,
        }
    }
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Builds a new MaCPepDB or updates an existing one
    Build {
        // Optional arguments
        /// Path taxdmp.zip from [NCBI](https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdmp.zip)
        #[arg(long)]
        taxonomy_file: Option<String>,
        /// Min peptide length
        /// Can be skipped once the database is built the first time
        #[arg(long, default_value_t = DEFAULT_MIN_PEPTIDE_LENGTH)]
        min_peptide_length: usize,
        /// Maximum peptide length, max: 60
        /// Can be skipped once the database is built the first time
        #[arg(long, default_value_t = DEFAULT_MAX_PEPTIDE_LENGTH)]
        max_peptide_length: usize,
        /// Maximum number of missed cleavages (ignored for unspecific proteases)
        #[arg(long, default_value_t = DEFAULT_MAX_NUMBER_OF_MISSED_CLEAVAGES)]
        max_number_of_missed_cleavages: usize,
        /// False positive probability for the bloom filter used for partitioning
        #[arg(long, default_value_t = DEFAULT_FALSE_POSITIVE_PROBABILITY)]
        partitioner_false_positive_probability: f64,
        /// If set, peptides containing unknown amino acids will be kept. Keep in mind that X's mass is
        /// 0 and searches might be incorrect
        #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        keep_peptides_containing_unknown: bool,
        /// If set, domains will be added to the database
        #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        include_domains: bool, // this is a flag now `--include-domains`
        /// Protease used for digestion
        #[arg(long, value_enum, default_value_t = DEFAULT_PROTEASE)]
        protease: ProteaseChoice,
        /// Fraction of usable memory for the bloom filter for counting peptides
        /// For a tryptic digest of the complete Uniprot database 16 GB is recommended
        #[arg(long, default_value_t = 0.3)]
        usable_memory_fraction: f64,

        // Positional arguments
        /// Database URL to connect e.g. scylla://host1,host2/keyspace
        database_url: String,
        /// Number of threads to use for building the database
        num_threads: usize,
        /// Number of partitions or a pre generated partition limits file (see mass-counter and partitioning)
        partitions: String,
        /// Path to the log folder
        log_folder: String,
        /// Protein files in UniProt text format (txt or dat).
        /// Each file can be compressed with gzip (has last extension `.gz` e.g. `txt.gz``).
        /// Glob patterns are allowed. e.g. /path/to/**/*.dat, put them in quotes if your shell expands them.
        #[arg(value_delimiter = ' ', num_args = 0..)]
        protein_file_paths: Vec<String>,
    },
    Web {
        /// Setting this flag disables the creation of the taxonomy name search index,
        /// disables the taxonomy search end point and reduces the memory usage of the web server
        /// by ~1GB
        #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        no_taxonomy_search: bool,

        // positional arguments
        /// Database URL to connect e.g. scylla://host1,host2/keyspace
        database_url: String,
        /// Interface (IP) to bind the web server to
        interface: String,
        /// Port to bind the web server to
        port: u16,
    },
    DomainTypes {
        /// Database URL to connect e.g. scylla://host1,host2/keyspace
        database_url: String,
    },
    MassCounter {
        // Optional arguments
        /// Can be skipped once the database is built the first time
        #[arg(long, default_value_t = DEFAULT_MIN_PEPTIDE_LENGTH)]
        min_peptide_length: usize,
        /// Maximum peptide length, max: 60
        /// Can be skipped once the database is built the first time
        #[arg(long, default_value_t = DEFAULT_MAX_PEPTIDE_LENGTH)]
        max_peptide_length: usize,
        /// Maximum number of missed cleavages (ignored for unspecific proteases)
        #[arg(long, default_value_t = DEFAULT_MAX_NUMBER_OF_MISSED_CLEAVAGES)]
        max_number_of_missed_cleavages: usize,
        /// False positive probability for the bloom filter for counting peptides
        #[arg(long, default_value_t = DEFAULT_FALSE_POSITIVE_PROBABILITY)]
        false_positive_probability: f64,
        /// Protease used for digestion
        #[arg(long, value_enum, default_value_t = DEFAULT_PROTEASE)]
        protease: ProteaseChoice,
        /// Initial number of partitions [default: 4 * num_threads]
        #[arg(long)]
        initial_num_partitions: Option<usize>,

        // Positional arguments
        /// Number of threads for counting the masses (10-20 recommended, as there are some mutexes involved which introduce some wait times)
        num_threads: usize,
        /// Fraction of usable memory for the bloom filter for counting peptides
        /// For a tryptic digest of the complete Uniprot database 16 GB is recommended
        usable_memory_fraction: f64,
        /// Optional path to store the peptide per mass table
        out_file: String,
        /// Protein files in UniProt text format (txt or dat).
        /// Each file can be compressed with gzip (has last extension `.gz` e.g. `txt.gz``).
        /// Glob patterns are allowed. e.g. /path/to/**/*.dat, put them in quotes if your shell expands them.
        #[arg(value_delimiter = ' ', num_args = 1..)]
        protein_file_paths: Vec<String>,
    },
    Partitioning {
        // Optional arguments
        /// Optional partition tolerance (default: 0.01)
        #[arg(long)]
        partition_tolerance: Option<f64>,

        // Positional arguments
        /// Number of partitions
        num_partitions: u64,
        /// Mass counts file
        mass_counts_file: String,
        /// Path to store the partitioning TSV file
        out_file: String,
    },
    Version {},
}

#[derive(Debug, Parser)]
#[command(name = "macpepdb")]
struct Cli {
    /// Verbosity level
    /// 0 - Error,
    /// 1 - Warn,
    /// 2 - Info,
    /// 3 - Debug,
    /// > 3 - Trace
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    /// How to log tracing. Can be used multiple times
    #[arg(short, long, value_enum, action = clap::ArgAction::Append)]
    tracing_target: Vec<TracingTarget>,
    /// Tracing log file. Only used if `file` is set in `tracing_target`.
    #[arg(short, long, default_value = "./logs/macpepdb.log")]
    file: PathBuf,
    /// Tracing log rotation. Only used if `file` is set in `tracing_target`.
    #[arg(short, long, value_enum, default_value = "never")]
    rotation: TracingLogRotation,
    /// Remote address of Loki endpoint for logging.
    /// Only used if `loki` is set in `tracing_target`.
    #[arg(short, long, default_value = "127.0.0.1:3100")]
    loki: String,
    /// How to log metrics. Can be used multiple times
    #[arg(short, long, value_enum, action = clap::ArgAction::Append)]
    metric_target: Vec<MetricTarget>,
    /// Local address to serve the Prometheus metrics endpoint.
    /// Port zero will automatically use a free port.
    /// Only used if `prometheus` is set in `metric_target`.
    #[arg(short, long, default_value = "127.0.0.1:9494")]
    prometheus: String,
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    //// Set up tracing
    let verbosity = match args.verbose {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        3 => Level::DEBUG,
        _ => Level::TRACE,
    };

    let filter = EnvFilter::from_default_env()
        .add_directive(verbosity.into())
        .add_directive("scylla=error".parse().unwrap())
        .add_directive("tokio_postgres=error".parse().unwrap())
        .add_directive("hyper=error".parse().unwrap())
        .add_directive("reqwest=error".parse().unwrap());

    // Tracing layers
    let mut tracing_indicatif_layer = None;
    let mut tracing_terminal_layer = None;
    let mut tracing_loki_layer = None;
    let mut tracing_file_layer = None;

    // Tracing guards/tasks
    let mut _tracing_loki_task = None;
    let mut _tracing_log_writer_guard = None;

    if args.tracing_target.contains(&TracingTarget::Terminal)
        || args.metric_target.contains(&MetricTarget::Terminal)
        || args.tracing_target.contains(&TracingTarget::All)
        || args.metric_target.contains(&MetricTarget::All)
    {
        let layer = IndicatifLayer::new()
            .with_span_child_prefix_symbol("\t")
            .with_span_child_prefix_indent("")
            .with_max_progress_bars(10, None);
        tracing_indicatif_layer = Some(layer);
    }

    if args.tracing_target.contains(&TracingTarget::Terminal)
        || args.tracing_target.contains(&TracingTarget::All)
    {
        let parent_layer = tracing_indicatif_layer.as_ref().unwrap();
        tracing_terminal_layer =
            Some(tracing_subscriber::fmt::layer().with_writer(parent_layer.get_stderr_writer()));
    }

    if args.tracing_target.contains(&TracingTarget::File)
        || args.tracing_target.contains(&TracingTarget::All)
    {
        let file_appender = RollingFileAppender::new(
            args.rotation.into(),
            args.file.parent().unwrap(),
            args.file.file_name().unwrap(),
        );
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        tracing_file_layer = Some(tracing_subscriber::fmt::layer().with_writer(non_blocking));
        _tracing_log_writer_guard = Some(guard);
    }

    if args.tracing_target.contains(&TracingTarget::Loki)
        || args.tracing_target.contains(&TracingTarget::All)
    {
        let (layer, task) = tracing_loki::builder()
            .label("macpepdb", "development")?
            .extra_field("pid", format!("{}", process::id()))?
            .build_url(Url::parse(&format!("http://{}", args.loki)).unwrap())?;
        tracing_loki_layer = Some(layer);
        _tracing_loki_task = Some(tokio::spawn(task));
    }

    tracing_subscriber::registry()
        .with(tracing_terminal_layer)
        .with(tracing_indicatif_layer)
        .with(tracing_file_layer)
        .with(tracing_loki_layer)
        .with(filter)
        .init();

    //// Setup (prometheus) metrics

    let mut _prometheus_scrape_address = None;

    if args.metric_target.contains(&MetricTarget::Prometheus)
        || args.metric_target.contains(&MetricTarget::Terminal)
        || args.metric_target.contains(&MetricTarget::All)
    {
        let prometheus_metrics_builder = PrometheusBuilder::new();

        // Create TCP listener for Prometheus metrics to check if port is available.
        // When Port 0 is given, the OS will choose a free port.
        let prometheus_scrape_socket_tmp = TcpListener::bind(&args.prometheus)
            .await
            .context("Creating TCP listener for Prometheus scrape endpoint")?;
        // Copy socket address to be able to use it for the endpoint
        let prometheus_scrape_socket = prometheus_scrape_socket_tmp.local_addr()?;
        _prometheus_scrape_address = Some(format!(
            "{}:{}",
            prometheus_scrape_socket.ip(),
            prometheus_scrape_socket.port()
        ));
        // Drop listener to make port available for the web server
        drop(prometheus_scrape_socket_tmp);

        prometheus_metrics_builder
            .with_http_listener(prometheus_scrape_socket)
            .install()?;
    }

    info!("Welcome to MaCPepDB!");

    match args.command {
        Commands::Build {
            database_url,
            num_threads,
            partitions,
            usable_memory_fraction,
            log_folder,
            taxonomy_file,
            min_peptide_length,
            max_peptide_length,
            max_number_of_missed_cleavages,
            partitioner_false_positive_probability,
            keep_peptides_containing_unknown,
            include_domains,
            protease,
            protein_file_paths,
        } => {
            if max_peptide_length > 60 {
                bail!("Max peptide lengths cannot be greater than 60");
            }

            let protein_file_paths = convert_str_paths_and_resolve_globs(protein_file_paths)?;

            let taxonomy_file_path =
                taxonomy_file.map(|taxonomy_file| Path::new(&taxonomy_file).to_path_buf());

            let log_folder = Path::new(&log_folder).to_path_buf();

            let num_partitions = partitions.parse::<u64>().ok();

            // Default partition limits (empty if created)
            let mut partition_limits: Vec<i64> = Vec::with_capacity(0);

            // Try to read partition limits from file
            let partition_limits_file_path = Path::new(&partitions).to_path_buf();
            if partition_limits_file_path.is_file() {
                // TSV reader
                let mut reader = csv::ReaderBuilder::new()
                    .delimiter(b'\t')
                    .has_headers(true)
                    .from_path(partition_limits_file_path)?;
                // Deserialize each line
                partition_limits = reader
                    .deserialize()
                    .map(|line| Ok(line?))
                    .collect::<Result<Vec<i64>>>()?;
            }

            if num_partitions.is_none() && partition_limits.is_empty() {
                bail!("Partitions was no number or a file. Without any partitions the database cannot be built.");
            }

            if database_url.starts_with("scylla://") {
                let start = std::time::Instant::now();
                let builder = ScyllaBuild::new(&database_url);

                match builder
                    .build(
                        &protein_file_paths,
                        &taxonomy_file_path,
                        num_threads,
                        num_partitions.unwrap_or(0),
                        usable_memory_fraction,
                        partitioner_false_positive_probability,
                        Some(Configuration::new(
                            protease.to_str().to_lowercase(),
                            Some(max_number_of_missed_cleavages),
                            Some(min_peptide_length),
                            Some(max_peptide_length),
                            !keep_peptides_containing_unknown,
                            partition_limits,
                        )),
                        &log_folder,
                        include_domains,
                    )
                    .await
                {
                    Ok(_) => info!("Database build completed successfully!"),
                    Err(e) => error!("Database build failed: {:?}", e),
                }
                info!("Database build took: {:?}", start.elapsed());
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
        Commands::Web {
            database_url,
            interface,
            port,
            no_taxonomy_search,
        } => {
            if database_url.starts_with("scylla://") {
                start_web_server(&database_url, interface, port, !no_taxonomy_search).await?;
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
        Commands::DomainTypes { database_url } => {
            if database_url.starts_with("scylla://") {
                let client = Client::new(&database_url).await?;

                let not_updated_peptides = info_span!("not_updated_peptides");
                let not_updated_peptides_enter = not_updated_peptides.enter();

                let mut domains: HashSet<String> = HashSet::new();

                for partition in 0_i64..100_i64 {
                    let query_statement = format!(
                "SELECT {} FROM {} WHERE partition = ? AND is_metadata_updated = true ALLOW FILTERING",
                PEPTIDE_SELECT_COLS.join(","),
                PeptideTable::table_name()
            );

                    debug!("Streaming rows of partition {}", partition);

                    let mut row_stream = client
                        .query_iter(query_statement, (partition,))
                        .await?
                        .rows_stream::<(Peptide,)>()?;

                    while let Some(row_opt) = row_stream.next().await {
                        if row_opt.is_err() {
                            debug!("Row opt err");
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        let row = row_opt.unwrap().0;
                        let peptide = row;

                        let a = peptide.get_domains().iter().map(|x| x.get_name());

                        for name in a {
                            if name.is_empty() {
                                info!("{:?}", peptide.get_proteins());
                            }
                            domains.insert(name.to_string());
                        }
                    }
                }

                info!("{:?}", domains);
                std::mem::drop(not_updated_peptides_enter);
                std::mem::drop(not_updated_peptides);
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
        Commands::MassCounter {
            num_threads,
            usable_memory_fraction,
            out_file,
            min_peptide_length,
            max_peptide_length,
            max_number_of_missed_cleavages,
            false_positive_probability,
            protease,
            protein_file_paths,
            initial_num_partitions,
        } => {
            // Create span for this function
            let info_span = info_span!("counting peptide masses");

            let protease = get_protease_by_name(
                protease.to_str(),
                Some(min_peptide_length),
                Some(max_peptide_length),
                Some(max_number_of_missed_cleavages),
            )?;
            let protein_file_paths = convert_str_paths_and_resolve_globs(protein_file_paths)?;
            let initial_num_partitions = match initial_num_partitions {
                Some(num) => num,
                None => 4 * num_threads,
            };

            let mass_counts = PeptideMassCounter::count(
                &protein_file_paths,
                protease.as_ref(),
                true,
                false_positive_probability,
                usable_memory_fraction,
                num_threads,
                initial_num_partitions,
            )
            .instrument(info_span)
            .await?;

            let num_peptides: u64 = mass_counts.iter().map(|x| x.1).sum();
            info!("Total number of peptides: {}", num_peptides);
            info!("Total number of masses: {}", mass_counts.len());

            let mut writer = csv::WriterBuilder::new()
                .delimiter(b'\t')
                .from_path(Path::new(&out_file))?;

            writer.serialize(("mass", "count"))?;
            for (mass, count) in mass_counts {
                writer.serialize((mass, count))?;
            }
        }
        Commands::Partitioning {
            num_partitions,
            mass_counts_file,
            out_file,
            partition_tolerance,
        } => {
            let mut reader = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .has_headers(true)
                .from_path(Path::new(&mass_counts_file))?;

            let mass_counts: Vec<(i64, u64)> = reader
                .deserialize()
                .map(|line| Ok(line?))
                .collect::<Result<Vec<(i64, u64)>>>()?;

            let partition_limits = PeptidePartitioner::create_partition_limits(
                &mass_counts,
                num_partitions,
                partition_tolerance,
            )?;

            let mut writer = csv::WriterBuilder::new()
                .delimiter(b'\t')
                .from_path(Path::new(&out_file))?;

            writer.serialize("partition_limit")?;
            for limit in partition_limits.iter() {
                writer.serialize(limit)?;
            }
        }
        Commands::Version {} => {
            println!(
                "{}",
                include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/logo.txt"))
            );
            println!("-- Mass Centric Peptide Database --");
            println!("Version: {}", env!("CARGO_PKG_VERSION"));
            println!("Repository: {}", env!("CARGO_PKG_REPOSITORY"));
            sleep(Duration::from_secs(20)).await;
        }
    };

    Ok(())
}

/// Converts a vector of strings to a vector of paths and resolves glob patterns.
///
/// # Arguments
/// * `paths` - Vector of paths as strings
///
fn convert_str_paths_and_resolve_globs(paths: Vec<String>) -> Result<Vec<PathBuf>> {
    Ok(paths
        .into_iter()
        .map(|path| {
            if !path.contains("*") {
                // Return plain path in vecotor if no glob pattern is found
                Ok(vec![Path::new(&path).to_path_buf()])
            } else {
                // Resolve glob pattern and return array of paths
                Ok(glob(&path)?
                    .map(|x| Ok(x?))
                    .collect::<Result<Vec<PathBuf>>>()?)
            }
        })
        .collect::<Result<Vec<_>>>()? // Collect and resolve errors from parsing/resolving
        .into_iter()
        .flatten() // flatten the vectors which
        .collect())
}
