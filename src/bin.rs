// std imports
use std::collections::HashSet;
use std::fs::read_to_string;
use std::path::PathBuf;
use std::{path::Path, thread::sleep, time::Duration};

// 3rd party imports
use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use dihardts_omicstools::proteomics::proteases::functions::get_by_name as get_protease_by_name;
use futures::StreamExt;
use glob::glob;
use indicatif::ProgressStyle;
use macpepdb::database::scylla::client::{Client, GenericClient};
use macpepdb::database::scylla::peptide_table::{PeptideTable, SELECT_COLS};
use macpepdb::database::table::Table;
use macpepdb::entities::peptide::Peptide;
use macpepdb::tools::peptide_mass_counter::PeptideMassCounter;
use macpepdb::tools::peptide_partitioner::PeptidePartitioner;
use tracing::{debug, error, info, info_span, Level};
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

// internal imports
use macpepdb::functions::performance_measurement::scylla as scylla_performance;
use macpepdb::io::post_translational_modification_csv::reader::Reader as PtmReader;
use macpepdb::mass::convert::to_int as mass_to_int;
use macpepdb::web::server::start as start_web_server;
use macpepdb::{
    database::{
        database_build::DatabaseBuild, scylla::database_build::DatabaseBuild as ScyllaBuild,
    },
    entities::configuration::Configuration,
};

// Protease choices depends on dihardts_omicstools::proteomics::proteases::functions::ALL which is a str-vector.
// In order to use it with clap it needs to be an enum which is created via a template and build rs
include!(concat!(env!("OUT_DIR"), "/protease_choice.rs"));

const DEFAULT_MIN_PEPTIDE_LENGTH: usize = 6;
const DEFAULT_MAX_PEPTIDE_LENGTH: usize = 50;
const DEFAULT_MAX_NUMBER_OF_MISSED_CLEAVAGES: usize = 2;
/// Default false positive probability for bloom filters
///
const DEFAULT_FALSE_POSITIVE_PROBABILITY: f64 = 0.01;

/// Default protease
///
const DEFAULT_PROTEASE: ProteaseChoice = ProteaseChoice::Trypsin;

#[derive(Debug, Subcommand)]
enum Commands {
    /// Builds a new MaCPepDB or updates an existing one
    Build {
        /// Database URL to connect e.g. scylla://host1,host2/keyspace
        database_url: String,
        /// Number of threads to use for building the database
        num_threads: usize,
        /// Number of partitions or a pre generated partition limits file (see mass-counter and partitioning)
        ///
        partitions: String,
        /// Fraction of usable memory for the bloom filter for counting peptides
        /// For a tryptic digest of the complete Uniprot database 16 GB is recommended
        usable_memory_fraction: f64,
        /// Path to the log folder
        log_folder: String,
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
        /// Interval in seconds at which the metrics are logged to file
        #[arg(long, default_value_t = 900)]
        metrics_log_interval: u64,
        /// Protease used for digestion
        #[arg(long, value_enum, default_value_t = DEFAULT_PROTEASE)]
        protease: ProteaseChoice,
        /// Protein files in UniProt text format (txt or dat). Glob patterns are allowed. e.g. /path/to/**/*.dat, put them in quotes if your shell expands them.
        #[arg(value_delimiter = ' ', num_args = 0..)]
        protein_file_paths: Vec<String>,
    },
    QueryPerformance {
        /// Database URL to connect e.g. scylla://host1,host2/keyspace
        database_url: String,
        /// Input file with masses to query
        masses_file: String,
        /// PTM file
        ptm_file: String,
        /// Lower mass tolerance (ppm)
        lower_mass_tolerance: i64,
        /// Upper mass tolerance (ppm)
        upper_mass_tolerance: i64,
        /// Maximum number of variable modifications
        max_variable_modifications: i16,
    },
    Web {
        /// Database URL to connect e.g. scylla://host1,host2/keyspace
        database_url: String,
        /// Interface (IP) to bind the web server to
        interface: String,
        /// Port to bind the web server to
        port: u16,
        /// Setting this flag disables the creation of the taxonomy name search index,
        /// disables the taxonomy search end point and reduces the memory usage of the web server
        /// by ~1GB
        #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
        no_taxonomy_search: bool,
    },
    DomainTypes {
        /// Database URL to connect e.g. scylla://host1,host2/keyspace
        database_url: String,
    },
    MassCounter {
        /// Number of threads for counting the masses (10-20 recommended, as there are some mutexes involved which introduce some wait times)
        num_threads: usize,
        /// Fraction of usable memory for the bloom filter for counting peptides
        /// For a tryptic digest of the complete Uniprot database 16 GB is recommended
        usable_memory_fraction: f64,
        /// Optional path to store the peptide per mass table
        out_file: String,
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
        /// Protein files in UniProt text format (txt or dat). Glob patterns are allowed. e.g. /path/to/**/*.dat, put them in quotes if your shell expands them.
        #[arg(value_delimiter = ' ', num_args = 1..)]
        protein_file_paths: Vec<String>,
    },
    Partitioning {
        /// Number of partitions
        num_partitions: u64,
        /// Mass counts file
        mass_counts_file: String,
        /// Path to store the partitioning TSV file
        out_file: String,
        /// Optional partition tolerance (default: 0.01)
        #[arg(long)]
        partition_tolerance: Option<f64>,
    },
    Version {},
}

#[derive(Debug, Parser)]
#[command(name = "macpepdb")]
struct Cli {
    /// Verbosity level
    /// 0 - Error
    /// 1 - Warn
    /// 2 - Info
    /// 3 - Debug
    /// > 3 - Trace
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    let verbosity = match args.verbose {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        3 => Level::DEBUG,
        _ => Level::TRACE,
    };

    let filter = EnvFilter::from_default_env()
        .add_directive(verbosity.into())
        .add_directive("scylla=info".parse().unwrap())
        .add_directive("tokio_postgres=info".parse().unwrap());

    let indicatif_layer = IndicatifLayer::new()
        .with_progress_style(
            ProgressStyle::with_template(
                "{spinner:.cyan} {span_child_prefix} {span_name} {span_fields} {wide_msg} {elapsed}",
            )
            .unwrap(),
        )
        .with_span_child_prefix_symbol("↳ ")
        .with_span_child_prefix_indent(" ");

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
        .with(indicatif_layer)
        .with(filter)
        .init();

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
            metrics_log_interval,
            protease,
            protein_file_paths,
        } => {
            if max_peptide_length > 60 {
                bail!("Max peptide lengths cannot be greater than 60");
            }

            let protein_file_paths = convert_str_paths_and_resolve_globs(protein_file_paths)?;

            let taxonomy_file_path = match taxonomy_file {
                Some(taxonomy_file) => Some(Path::new(&taxonomy_file).to_path_buf()),
                None => None,
            };

            let log_folder = Path::new(&log_folder).to_path_buf();

            let num_partitions = match partitions.parse::<u64>() {
                Ok(num_partitions) => Some(num_partitions),
                Err(_) => None,
            };

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
                        metrics_log_interval,
                    )
                    .await
                {
                    Ok(_) => info!("Database build completed successfully!"),
                    Err(e) => error!("Database build failed: {:?}", e),
                }
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
        Commands::QueryPerformance {
            database_url,
            masses_file,
            ptm_file,
            lower_mass_tolerance,
            upper_mass_tolerance,
            max_variable_modifications,
        } => {
            let masses: Vec<i64> = read_to_string(Path::new(&masses_file))?
                .split("\n")
                .filter_map(|line| {
                    if !line.is_empty() {
                        let mass = mass_to_int(line.parse::<f64>().unwrap());
                        Some(mass)
                    } else {
                        None
                    }
                })
                .collect();

            let ptms = PtmReader::read(Path::new(&ptm_file))?;

            if database_url.starts_with("scylla://") {
                scylla_performance::query_performance(
                    &database_url,
                    masses,
                    lower_mass_tolerance,
                    upper_mass_tolerance,
                    max_variable_modifications,
                    ptms,
                )
                .await?;
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
                let session = client.get_session();

                let not_updated_peptides = info_span!("not_updated_peptides");
                let not_updated_peptides_enter = not_updated_peptides.enter();

                let mut domains: HashSet<String> = HashSet::new();

                for partition in 0_i64..100_i64 {
                    let query_statement = format!(
                "SELECT {} FROM {}.{} WHERE partition = ? AND is_metadata_updated = true ALLOW FILTERING",
                SELECT_COLS,
                client.get_database(),
                PeptideTable::table_name()
            );

                    debug!("Streaming rows of partition {}", partition);

                    let mut rows_stream = session
                        .query_iter(query_statement, (partition,))
                        .await
                        .unwrap();

                    while let Some(row_opt) = rows_stream.next().await {
                        if row_opt.is_err() {
                            debug!("Row opt err");
                            sleep(Duration::from_millis(100));
                            continue;
                        }
                        let row = row_opt.unwrap();
                        let peptide = Peptide::from(row);

                        let a = peptide.get_domains().iter().map(|x| x.get_name());

                        for name in a {
                            if name == "" {
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
        } => {
            let protease = get_protease_by_name(
                protease.to_str(),
                Some(min_peptide_length),
                Some(max_peptide_length),
                Some(max_number_of_missed_cleavages),
            )?;
            let protein_file_paths = convert_str_paths_and_resolve_globs(protein_file_paths)?;

            let mass_counts = PeptideMassCounter::count(
                &protein_file_paths,
                protease.as_ref(),
                true,
                false_positive_probability,
                usable_memory_fraction,
                num_threads,
            )
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
