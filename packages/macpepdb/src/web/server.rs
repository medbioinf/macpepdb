use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

use axum::routing::{get, post};
use axum::{Router, middleware};
use http::Method;
use thiserror::Error;
use tower_http::cors::{Any, CorsLayer};

use crate::blob_table::BlobTable;
use crate::client::Client;
use crate::configuration::RuntimeConfiguration;
use crate::web::configuration_controller::get_configuration;
use crate::web::error_controller::page_not_found;
use crate::web::headers::X_DO_NOT_TRACK;
use crate::web::middleware::tracking_middleware;
use crate::web::peptide_controller::{
    get_peptide, get_peptide_existence, get_search as get_peptide_search,
    post_search as post_peptide_search,
};
use crate::web::server_state::{MatomoInfo, ServerState};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Blob error in web server: {0}")]
    Blob(Box<crate::blob_table::Error>),
    #[error("Client error in web server: {0}")]
    Client(Box<crate::client::Error>),
    #[error("Missing configuration, are you sure the database is build correctly and finished?")]
    MissingConfiguration,
    #[error("Error binding TCP listener: {0}")]
    TcpListener(std::io::Error),
}

impl From<crate::blob_table::Error> for Error {
    fn from(value: crate::blob_table::Error) -> Self {
        Self::Blob(Box::new(value))
    }
}

impl From<crate::client::Error> for Error {
    fn from(value: crate::client::Error) -> Self {
        Self::Client(Box::new(value))
    }
}

/// Starts the MaCPepDB web server on the given interface and port.
///
/// # Arguments
/// * `database_nodes` - List of database nodes
/// * `interface` - Interface to listen on
/// * `port` - Port to listen on
/// * `with_taxonomy_search` - If taxonomy search index should be built
/// * `num_search_threads` - Number of concurrent search threads (and connections)
/// * `matomo_info` - Optional Matomo tracking information
///
pub async fn start(
    client: Client,
    socket: SocketAddr,
    _with_taxonomy_search: bool,
    concurrent_searches: NonZeroUsize,
    matomo_info: Option<MatomoInfo>,
    shutdown_signal: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
) -> Result<(), Error> {
    tracing::info!("Start MaCPepDB web server");
    // Load configuration
    tracing::debug!("Loading configuration...");
    let configuration: RuntimeConfiguration =
        BlobTable::select(&client, RuntimeConfiguration::BLOB_KEY)
            .await?
            .ok_or(Error::MissingConfiguration)?;

    let server_state = Arc::new(ServerState::new(
        client,
        configuration,
        matomo_info,
        concurrent_searches,
    ));

    // Add CORS layer
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(vec![
            http::header::ACCEPT,
            http::header::CONTENT_TYPE,
            http::header::DNT,
            X_DO_NOT_TRACK,
        ])
        .allow_origin(Any);

    tracing::debug!("Create router...");
    // Build our application with route
    let mut app = Router::new()
        // Peptide routes
        .route(
            "/api/peptides/search/{payload}/{accept}",
            get(get_peptide_search),
        )
        .route("/api/peptides/search", post(post_peptide_search))
        .route(
            "/api/peptides/{sequence}/exists",
            get(get_peptide_existence),
        )
        .route("/api/peptides/{sequence}", get(get_peptide))
        // Configuration routes
        .route("/api/configuration", get(get_configuration))
        .with_state(server_state.clone())
        .fallback(page_not_found)
        .layer(cors);

    if server_state.matomo_info().is_some() {
        tracing::info!("Add tracking middleware...");
        app = app.layer(middleware::from_fn_with_state(
            server_state.clone(),
            tracking_middleware::track_middleware,
        ));
    }

    tracing::debug!("Bind listener...");
    let listener = tokio::net::TcpListener::bind(socket)
        .await
        .map_err(Error::TcpListener)?;
    tracing::info!(
        "Ready for connections, listening on {}:{}",
        socket.ip(),
        socket.port()
    );
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal)
    .await
    .unwrap();

    Ok(())
}
