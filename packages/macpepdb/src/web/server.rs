use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;

use axum::{Router, middleware};
use http::Method;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use thiserror::Error;
use tower::Service;
use tower_http::cors::{Any, CorsLayer};

use crate::blob_table::BlobTable;
use crate::client::Client;
use crate::configuration::RuntimeConfiguration;
use crate::peptide_search::PeptideSearchType;
#[cfg(feature = "admin-api")]
use crate::web::admin_controller::AdminController;
use crate::web::chemistry_controller::ChemistryController;
use crate::web::configuration_controller::ConfigurationController;
use crate::web::error_controller::page_not_found;
use crate::web::headers::X_DO_NOT_TRACK;
use crate::web::middleware::tracking_middleware;
use crate::web::peptide_controller::PeptideController;
use crate::web::protein_controller::ProteinController;
use crate::web::server_state::{MatomoInfo, ServerState};
use crate::web::taxonomy_controller::TaxonomyController;

/// Errors that can occur while starting or running the web server.
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
/// * `socket`- Socket to use for the server
/// * `max_concurrent_streams_per_connection` - Max HTTP/2 streams (in-flight requests)
///   allowed per connection; HTTP/2 has no default limit otherwise (RFC 7540 §6.5.2)
/// * `search_type` - Type of peptide search to use
/// * `matomo_info` - Optional Matomo tracking information
/// * `shutdown_signal` - Future that resolves when the server should shut down
///
#[allow(clippy::too_many_arguments)]
pub async fn start(
    client: Client,
    socket: SocketAddr,
    concurrent_searches: NonZeroUsize,
    max_concurrent_streams_per_connection: NonZeroUsize,
    search_type: PeptideSearchType,
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
        search_type,
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
    #[allow(unused_mut)]
    let mut app = Router::new()
        // Peptide routes
        .nest(
            PeptideController::controller_path(),
            PeptideController::routes(server_state.clone()),
        )
        // Configuration routes
        .nest(
            ConfigurationController::controller_path(),
            ConfigurationController::routes(server_state.clone()),
        )
        // Taxonomy routes
        .nest(
            TaxonomyController::controller_path(),
            TaxonomyController::routes(server_state.clone()),
        )
        // Protein routes
        .nest(
            ProteinController::controller_path(),
            ProteinController::routes(server_state.clone()),
        )
        // Protein routes
        .nest(
            ChemistryController::controller_path(),
            ChemistryController::routes(server_state.clone()),
        );

    #[cfg(feature = "admin-api")]
    {
        tracing::warn!(
            "admin-api feature enabled: exposing DB client rebuild endpoint at {}{} \
             — never expose this build to the internet",
            AdminController::controller_path(),
            "/client"
        );
        app = app.nest(
            AdminController::controller_path(),
            AdminController::routes(server_state.clone()),
        );
    }

    let mut app = app
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

    // `axum::serve` doesn't expose HTTP/2 tuning, so the connection handler is built manually
    // here to bound concurrent streams per connection. Without this, HTTP/2 has no default
    // limit on concurrent requests per connection (RFC 7540 §6.5.2), so a handful of
    // multiplexed client/proxy connections can drive an unbounded number of concurrent
    // searches (each holding its own dedup set and DB query fan-out).
    let mut h2_builder = auto::Builder::new(TokioExecutor::new());
    h2_builder
        .http2()
        .max_concurrent_streams(Some(max_concurrent_streams_per_connection.get() as u32));
    let h2_builder = Arc::new(h2_builder);

    let mut make_service = app.into_make_service_with_connect_info::<SocketAddr>();
    let mut connections = tokio::task::JoinSet::new();
    let mut shutdown_signal = shutdown_signal;

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, remote_addr) = match accepted {
                    Ok(accepted) => accepted,
                    Err(err) => {
                        tracing::warn!("Failed to accept connection: {err}");
                        continue;
                    }
                };
                let tower_service = make_service.call(remote_addr).await.unwrap();
                let hyper_service = TowerToHyperService::new(tower_service);
                let h2_builder = h2_builder.clone();
                connections.spawn(async move {
                    let io = TokioIo::new(stream);
                    if let Err(err) = h2_builder.serve_connection_with_upgrades(io, hyper_service).await {
                        tracing::debug!("Connection closed with error: {err}");
                    }
                });
            }
            _ = &mut shutdown_signal => {
                tracing::info!("Shutdown signal received, stopping listener...");
                break;
            }
        }
    }

    tracing::info!("Draining {} in-flight connection(s)...", connections.len());
    while connections.join_next().await.is_some() {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    Ok(())
}
