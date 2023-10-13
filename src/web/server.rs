// std imports
use std::net::SocketAddr;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use axum::routing::{get, post};
use axum::Router;

// internal imports
use crate::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use crate::database::scylla::client::{Client, GenericClient};
use crate::database::scylla::configuration_table::ConfigurationTable;
use crate::entities::configuration::Configuration;
use crate::web::peptide_controller::{get_peptide, search as peptide_search};

pub async fn start(
    database_nodes: Vec<String>,
    interface: String,
    port: u16,
    is_verbose: bool,
) -> Result<()> {
    // Create a database client
    // Session maintains it own connection pool internally: https://github.com/scylladb/scylla-rust-driver/issues/724
    // A single client with a session should be sufficient for the entire application
    let db_client = Client::new(
        &database_nodes
            .iter()
            .map(|node| node.as_str())
            .collect::<Vec<&str>>(),
    )
    .await?;
    let db_client = Arc::new(db_client);

    // Load configuration
    let configuration: Configuration = ConfigurationTable::select(db_client.as_ref()).await?;
    let configuration = Arc::new(configuration);

    tracing::info!("Start MaCPepDB web server");

    // Build our application with route
    let app = Router::new()
        // Peptide routes
        .route("/api/peptides/search", post(peptide_search))
        .with_state((db_client.clone(), configuration.clone()))
        .route("/api/peptides/:sequence", get(get_peptide))
        .with_state((db_client.clone(), configuration.clone()));

    // Run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr: SocketAddr = format!("{}:{}", interface, port).parse()?;
    tracing::info!("ready for connections, listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}
