use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::State,
    http::{Method, StatusCode},
    routing::get,
};
use metrics_exporter_prometheus::PrometheusHandle;
use thiserror::Error;
use tokio::task::JoinHandle;
use tower_http::cors::CorsLayer;

static UPDATE_INTERVAL: Duration = Duration::from_millis(1000);

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unable to open socket for prometheus scrape endoint: {0}")]
    TcpListener(std::io::Error),
    #[error("Unable to serve endpoint: {0}")]
    Serve(std::io::Error),
}

pub struct PrometheusHandler {
    _exporter: JoinHandle<Result<(), Error>>,
    _upkeeper: JoinHandle<()>,
}

impl PrometheusHandler {
    pub async fn new(
        recorder_handle: PrometheusHandle,
        socket_addr: SocketAddr,
        shutdown_signal: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> Result<Self, Error> {
        Ok(PrometheusHandler {
            _exporter: Self::scrape_endpoint(recorder_handle.clone(), socket_addr, shutdown_signal)
                .await?,
            _upkeeper: Self::upkeeper(recorder_handle.clone()).await,
        })
    }

    async fn metrics_endoint(state: State<Arc<PrometheusHandle>>) -> String {
        state.0.render()
    }

    async fn not_found_endpoint() -> (StatusCode, &'static str) {
        (StatusCode::NOT_FOUND, "Not Found")
    }

    async fn scrape_endpoint(
        recorder_handle: PrometheusHandle,
        socket_addr: SocketAddr,
        shutdown_signal: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> Result<JoinHandle<Result<(), Error>>, Error> {
        let state = Arc::new(recorder_handle);

        // Add CORS layer
        let cors = CorsLayer::new().allow_methods([Method::GET]);

        tracing::debug!("Create prometheus router...");
        // Build our application with route
        let app = Router::new()
            // Peptide routes
            .route("/metrics", get(Self::metrics_endoint))
            .with_state(state.clone())
            .fallback(Self::not_found_endpoint)
            .layer(cors);

        tracing::debug!("Bind listener...");
        let listener = tokio::net::TcpListener::bind(socket_addr)
            .await
            .map_err(Error::TcpListener)?;

        Ok(tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown_signal)
            .await
            .map_err(Error::Serve)
        }))
    }

    async fn upkeeper(recorder_handle: PrometheusHandle) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(UPDATE_INTERVAL).await;
                recorder_handle.run_upkeep();
            }
        })
    }
}
