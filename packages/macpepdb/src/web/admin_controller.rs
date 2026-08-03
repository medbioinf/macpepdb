// std imports
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;

// 3rd party imports
use axum::Router;
use axum::body::Body;
use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use serde::Deserialize;
use thiserror::Error;

// internal imports
use crate::blob_table::BlobTable;
use crate::client::Client;
use crate::configuration::RuntimeConfiguration;
use crate::web::DEFAULT_ERROR_HEADER_MAP;
use crate::web::server_state::ServerState;

pub static CONTROLLER_PATH: &str = "/api/admin";
pub static REBUILD_CLIENT_PATH: &str = "/client";

/// Body of `POST /api/admin/client`. Kept local to this module rather than in the shared,
/// published `macpepdb_web_common` crate, since it's only meaningful behind `admin-api`.
#[derive(Deserialize)]
struct RebuildClientRequest {
    database_url: String,
    concurrent_searches: usize,
}

/// Errors that can occur while rebuilding the db client.
#[derive(Debug, Error)]
pub enum Error {
    #[error("concurrent_searches must be greater than 0")]
    InvalidConcurrentSearches,
    #[error("Client error: {0}")]
    Client(#[from] crate::client::Error),
    #[error("Error reading the configuration from the new database: {0}")]
    Blob(#[from] crate::blob_table::Error),
    #[error(
        "The new database has no configuration, are you sure it is built correctly and finished? \
         A database built before the mass partitioning switched to per-partition ranges needs \
         `config migrate`."
    )]
    MissingConfiguration,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (
            StatusCode::BAD_REQUEST,
            DEFAULT_ERROR_HEADER_MAP.deref().clone(),
            Body::from(format!("{self}")),
        )
            .into_response()
    }
}

/// Controller exposing endpoints to rebuild the server's db client at runtime, under
/// `/api/admin`. Only compiled with the `admin-api` feature; never enable this on an
/// internet-facing deployment as it accepts an arbitrary PostgreSQL URL from the caller.
pub struct AdminController;

impl AdminController {
    /// Builds the router for this controller's routes, nested under
    /// [`Self::controller_path`].
    ///
    /// # Arguments
    /// * `state` - The state of the server.
    pub fn routes(state: Arc<ServerState>) -> Router<Arc<ServerState>> {
        let router: Router<Arc<ServerState>> =
            Router::new().route(REBUILD_CLIENT_PATH, post(Self::rebuild_client));

        router.with_state(state)
    }

    /// Returns the base path this controller's routes are nested under.
    pub fn controller_path() -> &'static str {
        CONTROLLER_PATH
    }

    /// Rebuilds the server's db client from a new PostgreSQL URL, reloads the runtime
    /// configuration from that database and swaps the concurrent-search limit, replacing all
    /// three in place for every handler.
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/admin/client`
    /// * Method: `POST`
    /// * Body:
    /// ```json
    /// {
    ///     "database_url": "postgresql://user:pass@host:port/dbname?pool_size=16",
    ///     "concurrent_searches": 16
    /// }
    /// ```
    ///
    /// ## Response
    /// * `204 No Content` on success.
    ///
    /// The configuration blob is read from the new database before anything is swapped, so a
    /// `204` means the new URL was reachable and holds a usable configuration; on any error
    /// the server keeps running against the previous client and configuration.
    async fn rebuild_client(
        State(server_state): State<Arc<ServerState>>,
        Json(payload): Json<RebuildClientRequest>,
    ) -> Result<StatusCode, Error> {
        let concurrent_searches = NonZeroUsize::new(payload.concurrent_searches)
            .ok_or(Error::InvalidConcurrentSearches)?;
        let new_client = Client::new(&payload.database_url).await?;
        let configuration: RuntimeConfiguration =
            BlobTable::select(&new_client, RuntimeConfiguration::BLOB_KEY)
                .await?
                .ok_or(Error::MissingConfiguration)?;
        server_state.rebuild_db_client(new_client, configuration, concurrent_searches);
        Ok(StatusCode::NO_CONTENT)
    }
}
