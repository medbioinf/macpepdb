use std::path::PathBuf;

// 3rd party imports
use clap::{Parser, Subcommand};
use log::info;
use macpepdb::{
    database::citus::database_build::DatabaseBuild,
    database::database_build::DatabaseBuild as DatabaseBuildTrait,
    entities::configuration::Configuration,
};

#[derive(Debug, Subcommand)]
enum Commands {
    LETSGO {
        name: String,
    },
    Build {
        database_url: String,
        protein_file_paths: Vec<PathBuf>,
        num_threads: usize,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        show_progress: bool,
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
    env_logger::init();
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
            show_progress,
            verbose,
        } => {
            let builder = DatabaseBuild::new(database_url);
            match builder
                .build(
                    &protein_file_paths,
                    num_threads,
                    num_partitions,
                    allowed_ram_usage,
                    partitioner_false_positive_probability,
                    None,
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
