use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};

use crate::web::server_state::ServerState;

/// Controller providing a simple liveness endpoint for the web server.
pub struct StatusController {}

impl StatusController {
    /// Route handler that reports the server is reachable. Always returns
    /// `"Status OK"`; per-node status details are not implemented yet.
    pub async fn status(_server_state: State<Arc<ServerState>>) -> impl IntoResponse {
        // TODO send node status
        "Status OK".to_string()
    }
}
