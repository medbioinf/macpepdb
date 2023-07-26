use std::{
    path::{Path, PathBuf},
    time::Duration,
};

// 3rd party imports
use clap::{Parser, Subcommand};
use futures::{stream, StreamExt};
use indicatif::ProgressStyle;
use macpepdb::{
    database::{
        citus::database_build::DatabaseBuild as CitusBuild, database_build::DatabaseBuild,
        scylla::database_build::DatabaseBuild as ScyllaBuild,
    },
    entities::{configuration::Configuration, protein},
};
use tracing::{event, info, info_span, instrument, metadata::LevelFilter, Level, Span};
use tracing_indicatif::{span_ext::IndicatifSpanExt, IndicatifLayer};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

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
        scylla: bool,
        #[clap(long)]
        show_progress: bool,
        #[clap(long)]
        verbose: bool,
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
    let mut filter = EnvFilter::from_default_env()
        .add_directive(Level::DEBUG.into())
        .add_directive("scylla=info".parse().unwrap());

    let indicatif_layer = IndicatifLayer::new();

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
        }
        Commands::Build {
            database_url,
            protein_file_paths,
            num_threads,
            num_partitions,
            allowed_ram_usage,
            partitioner_false_positive_probability,
            scylla,
            show_progress,
            verbose,
        } => {
            let protein_file_paths = protein_file_paths
                .into_iter()
                .map(|x| Path::new(&x).to_path_buf())
                .collect();

            if scylla {
                let builder = ScyllaBuild::new(database_url);

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
                        show_progress,
                        verbose,
                    )
                    .await
                {
                    Ok(_) => info!("Database build completed successfully!"),
                    Err(e) => info!("Database build failed: {}", e),
                }
            } else {
                let builder = CitusBuild::new(database_url);

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
                        show_progress,
                        verbose,
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
