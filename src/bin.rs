// std imports
use std::fs::read_to_string;
use std::{path::Path, thread::sleep, time::Duration};

// 3rd party imports
use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use indicatif::ProgressStyle;
use macpepdb::database::scylla::client::GenericClient;
use macpepdb::database::scylla::peptide_table::{PeptideTable, SELECT_COLS};
use macpepdb::database::scylla::protein_table::ProteinTable;
use macpepdb::database::scylla::{get_client, SCYLLA_KEYSPACE_NAME};
use macpepdb::database::selectable_table::SelectableTable;
use macpepdb::database::table::Table;
use macpepdb::entities::peptide::Peptide;
use tracing::{debug, error, info, info_span, Level};
use tracing_indicatif::{span_ext::IndicatifSpanExt, IndicatifLayer};
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
    LETSGO {
        name: String,
    },
    Build {
        database_url: String,
        num_threads: usize,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        #[clap(value_delimiter = ',', num_args = 1..)]
        protein_file_paths: Vec<String>,
        #[clap(long)]
        show_progress: bool,
        #[clap(long)]
        verbose: bool,
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
    CheckIsUpdated {
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
        Commands::LETSGO { name } => {
            info!("Hello, {}", name);
            info!("Test");
            let mut i = 0;

            let loop_span = info_span!("looping");
            let _loop_span_enter = loop_span.enter();

            let loop_span2 = info_span!("looping_inner");
            let _loop_span_enter2 = loop_span2.enter();

            loop {
                i += 1;

                loop_span.pb_set_message(format!("i is {}", i).as_str());

                sleep(Duration::from_secs(1));
            }
        }
        Commands::Build {
            database_url,
            protein_file_paths,
            num_threads,
            num_partitions,
            allowed_ram_usage,
            partitioner_false_positive_probability,
            show_progress,
            verbose,
            log_folder,
            only_metadata,
        } => {
            let protein_file_paths = protein_file_paths
                .into_iter()
                .map(|x| Path::new(&x).to_path_buf())
                .collect();

            let log_folder = Path::new(&log_folder).to_path_buf();

            if database_url.starts_with("scylla://") {
                // remove protocol
                let plain_database_url = database_url[9..].to_string();

                let builder = ScyllaBuild::new(plain_database_url);

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
                // remove protocol
                let plain_database_url = database_url[9..].to_string();
                let database_hosts = &plain_database_url.split(",").map(|x| x).collect();

                scylla_performance::query_performance(
                    &database_hosts,
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
                let plain_database_url = database_url[9..].to_string();
                let database_hosts = plain_database_url
                    .split(",")
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>();
                start_web_server(database_hosts, interface, port).await?;
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
        Commands::CheckIsUpdated { database_url } => {
            if database_url.starts_with("scylla://") {
                let plain_database_url = database_url[9..].to_string();
                let database_hosts = plain_database_url
                    .split(",")
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>();

                let client = get_client(Some(&database_hosts)).await?;
                let session = client.get_session();

                let not_updated_peptides = info_span!("not_updated_peptides");
                let not_updated_peptides_enter = not_updated_peptides.enter();

                for partition in 0_i64..100_i64 {
                    let query_statement = format!(
                "SELECT {} FROM {}.{} WHERE partition = ? AND is_metadata_updated = false ALLOW FILTERING",
                SELECT_COLS,
                SCYLLA_KEYSPACE_NAME,
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
                        info!(
                            "peptide {} {:?}",
                            peptide.get_sequence(),
                            peptide.get_proteins()
                        );
                        break;
                    }
                }
                std::mem::drop(not_updated_peptides_enter);
                std::mem::drop(not_updated_peptides);
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
    };

    Ok(())
}
