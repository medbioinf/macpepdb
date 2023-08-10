use std::{path::Path, thread::sleep, time::Duration};

// 3rd party imports
use clap::{Parser, Subcommand};
use indicatif::ProgressStyle;
use macpepdb::{
    database::{
        citus::database_build::DatabaseBuild as CitusBuild, database_build::DatabaseBuild,
        scylla::database_build::DatabaseBuild as ScyllaBuild,
    },
    entities::configuration::Configuration,
};
use tracing::{info, info_span, Level};
use tracing_indicatif::{span_ext::IndicatifSpanExt, IndicatifLayer};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

#[derive(Debug, Subcommand)]
enum Commands {
    LETSGO {
        name: String,
    },
    Build {
        database_urls: String,
        num_threads: usize,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        #[clap(value_delimiter = ',', num_args = 1..)]
        protein_file_paths: Vec<String>,
        #[clap(long)]
        scylla: bool,
        #[clap(long)]
        show_progress: bool,
        #[clap(long)]
        verbose: bool,
        log_folder: String,
    },
}

#[derive(Debug, Parser)]
#[command(name = "macpepdb")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() {
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
            database_urls,
            protein_file_paths,
            num_threads,
            num_partitions,
            allowed_ram_usage,
            partitioner_false_positive_probability,
            scylla,
            show_progress,
            verbose,
            log_folder,
        } => {
            let protein_file_paths = protein_file_paths
                .into_iter()
                .map(|x| Path::new(&x).to_path_buf())
                .collect();

            let log_folder = Path::new(&log_folder).to_path_buf();

            if scylla {
                let builder = ScyllaBuild::new(database_urls);

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
                    )
                    .await
                {
                    Ok(_) => info!("Database build completed successfully!"),
                    Err(e) => info!("Database build failed: {}", e),
                }
            } else {
                let builder = CitusBuild::new(database_urls);

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
                            6,
                            50,
                            true,
                            Vec::with_capacity(0),
                        )),
                        &log_folder,
                    )
                    .await
                {
                    Ok(_) => info!("Database build completed successfully!"),
                    Err(e) => info!("Database build failed: {}", e),
                }
            }
        }
    }
}
