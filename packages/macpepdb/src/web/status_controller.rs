use std::ops::Deref;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Json, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use http::StatusCode;
use macpepdb_web_common::responses::cluster_health::{
    ClusterEdgeResponse, ClusterHealthResponse, ClusterNodeResponse,
};
use thiserror::Error;

use crate::cluster_health;
use crate::web::DEFAULT_ERROR_HEADER_MAP;
use crate::web::server_state::ServerState;

static CONTROLLER_PATH: &str = "/api/status";
static CLUSTER_HEALTH_PATH: &str = "/cluster-health";

/// Errors that can occur while handling status endpoints.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Cluster health error: {0}")]
    ClusterHealth(Box<cluster_health::Error>),
}

into_thiserror_boxed!(cluster_health::Error, Error, ClusterHealth);

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let uuid = uuid::Uuid::now_v7();
        tracing::error!("[{uuid}] {self}");

        (
            StatusCode::INTERNAL_SERVER_ERROR,
            DEFAULT_ERROR_HEADER_MAP.deref().clone(),
            Body::from(format!(
                "Internal server error. Contact the admin and provide this UUID `{uuid}` to help identifying the error."
            )),
        )
            .into_response()
    }
}

/// Builds a response node from a graph node, replacing its real hostname with a
/// generic, position-based label so cluster server names never leave the backend.
fn anonymized_node(node: cluster_health::GraphNode, index: usize) -> ClusterNodeResponse {
    ClusterNodeResponse {
        name: format!("Node {}", index + 1),
        port: node.port,
        x: node.x,
        y: node.y,
    }
}

impl From<cluster_health::GraphEdge> for ClusterEdgeResponse {
    fn from(edge: cluster_health::GraphEdge) -> Self {
        Self {
            source: edge.source,
            target: edge.target,
            healthy: edge.healthy,
        }
    }
}

/// Controller providing a simple liveness endpoint and Citus cluster health/connectivity graph
/// data under `/api/status`.
pub struct StatusController {}

impl StatusController {
    /// Builds the axum router for the status endpoints, mounted onto the given server state.
    pub fn routes(state: Arc<ServerState>) -> Router<Arc<ServerState>> {
        Router::new()
            .route("/", get(Self::status))
            .route(CLUSTER_HEALTH_PATH, get(Self::cluster_health))
            .with_state(state)
    }

    /// Returns the base path this controller is mounted on (`/api/status`).
    pub fn controller_path() -> &'static str {
        CONTROLLER_PATH
    }

    /// Route handler that reports the server is reachable. Always returns
    /// `"Status OK"`; per-node status details are not implemented yet.
    pub async fn status(_server_state: State<Arc<ServerState>>) -> impl IntoResponse {
        // TODO send node status
        "Status OK".to_string()
    }

    /// Runs `citus_check_cluster_node_health()` and returns the overall health flag plus a
    /// node/edge connectivity graph (with precomputed circular-layout coordinates) for the
    /// client to render with Plotly.
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/status/cluster-health`
    /// * Method: `GET`
    ///
    /// ## Response
    /// ```json
    /// {
    ///     "healthy": true,
    ///     "nodes": [
    ///         { "name": "Node 1", "port": 5432, "x": 1.0, "y": 0.0 },
    ///         { "name": "Node 2", "port": 5432, "x": -0.5, "y": 0.87 }
    ///     ],
    ///     "edges": [
    ///         { "source": 0, "target": 1, "healthy": true }
    ///     ]
    /// }
    /// ```
    ///
    pub async fn cluster_health(
        State(state): State<Arc<ServerState>>,
    ) -> Result<Json<ClusterHealthResponse>, Error> {
        let connections = cluster_health::check(&state.db_client()).await?;
        let healthy = cluster_health::is_healthy(&connections);
        let (nodes, edges) = cluster_health::build_graph(&connections);

        Ok(Json(ClusterHealthResponse {
            healthy,
            nodes: nodes
                .into_iter()
                .enumerate()
                .map(|(index, node)| anonymized_node(node, index))
                .collect(),
            edges: edges.into_iter().map(Into::into).collect(),
        }))
    }
}
