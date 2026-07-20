use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};

use crate::web::server_state::ServerState;

pub struct StatusController {}

impl StatusController {
    pub async fn status(_server_state: State<Arc<ServerState>>) -> impl IntoResponse {
        // TODO send node status
        "Status OK".to_string()
    }
}
