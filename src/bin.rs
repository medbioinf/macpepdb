use std::collections::HashSet;
// std imports
use std::fs::read_to_string;
use std::{path::Path, thread::sleep, time::Duration};

// 3rd party imports
use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use indicatif::ProgressStyle;
use macpepdb::database::scylla::client::{Client, GenericClient};
use macpepdb::database::scylla::peptide_table::{PeptideTable, SELECT_COLS};
use macpepdb::database::table::Table;
use macpepdb::entities::peptide::Peptide;
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

#[derive(Debug, Subcommand)]
enum Commands {
    Build {
        database_url: String,
        num_threads: usize,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        #[clap(value_delimiter = ',', num_args = 1..)]
        protein_file_paths: Vec<String>,
        log_folder: String,
        #[clap(long)]
        only_metadata: bool,
    },
    QueryPerformance {
        database_url: String,
        masses_file: String,
        ptm_file: String,
        lower_mass_tolerance: i64,
        upper_mass_tolerance: i64,
        max_variable_modifications: i16,
    },
    Web {
        database_url: String,
        interface: String,
        port: u16,
    },
    DomainTypes {
        database_url: String,
    },
}

#[derive(Debug, Parser)]
#[command(name = "macpepdb")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::from_default_env()
        .add_directive(Level::DEBUG.into())
        .add_directive("scylla=info".parse().unwrap())
        .add_directive("tokio_postgres=info".parse().unwrap());

    let indicatif_layer = IndicatifLayer::new().with_progress_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} {span_child_prefix}{span_name}{{{span_fields}}} {wide_msg} {elapsed}",
        )
        .unwrap()
    ).with_span_child_prefix_symbol("↳ ").with_span_child_prefix_indent(" ");

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
        .with(indicatif_layer)
        .with(filter)
        .init();

    info!("Welcome to MaCPepDB!");

    let args = Cli::parse();

    match args.command {
        Commands::Build {
            database_url,
            protein_file_paths,
            num_threads,
            num_partitions,
            allowed_ram_usage,
            partitioner_false_positive_probability,
            log_folder,
            only_metadata,
        } => {
            let protein_file_paths = protein_file_paths
                .into_iter()
                .map(|x| Path::new(&x).to_path_buf())
                .collect();

            let log_folder = Path::new(&log_folder).to_path_buf();

            if database_url.starts_with("scylla://") {
                let builder = ScyllaBuild::new(&database_url);

                match builder
                    .build(
                        &protein_file_paths,
                        num_threads,
                        num_partitions,
                        allowed_ram_usage,
                        partitioner_false_positive_probability,
                        Some(Configuration::new(
                            "trypsin".to_owned(),
                            2,
                            5,
                            60,
                            true,
                            Vec::with_capacity(0),
                        )),
                        &log_folder,
                        false,
                        only_metadata,
                    )
                    .await
                {
                    Ok(_) => info!("Database build completed successfully!"),
                    Err(e) => info!("Database build failed: {}", e),
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
        } => {
            if database_url.starts_with("scylla://") {
                start_web_server(&database_url, interface, port).await?;
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
        Commands::DomainTypes { database_url } => {
            if database_url.starts_with("scylla://") {
                let client = Client::new(&database_url).await?;
                let session = client.get_session();

                let peptide_domains_span = info_span!("peptide_domains");
                let peptide_domains_enter = peptide_domains_span.enter();

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

                        let domains = peptide.get_domains().iter();

                        for domain in domains {
                            info!(
                                "Domain {:?} Protein {:?}",
                                domain.get_name(),
                                domain.get_protein_opt().unwrap().get_accession()
                            );
                            domains.insert(domain.get_name().to_string());
                        }
                    }
                }

                info!("{:?}", domains);
                std::mem::drop(peptide_domains_enter);
                std::mem::drop(peptide_domains_span);
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
    };

    Ok(())
}
