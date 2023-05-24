use log::info;
use clap::{Parser, Subcommand};

#[derive(Debug, Subcommand)]
enum Commands {
    LETSGO {
        name: String,
    },
}


#[derive(Debug, Parser)]
#[command(name = "macpepdb")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}


fn main() {
    env_logger::init();
    info!("Welcome to MaCPepDB!");

    let args = Cli::parse();

    match args.command {
        Commands::LETSGO { name } => {
            info!("Hello, {}", name);
        }
    }
}
