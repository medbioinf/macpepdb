use serde::{Deserialize, Serialize};

/// A Citus cluster node, positioned for rendering (circular layout) in the connectivity graph
/// returned by `GET /api/status/cluster-health`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterNodeResponse {
    pub name: String,
    pub port: i32,
    pub x: f64,
    pub y: f64,
}

/// A connection between two nodes. `source`/`target` are indices into
/// `ClusterHealthResponse::nodes`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterEdgeResponse {
    pub source: usize,
    pub target: usize,
    pub healthy: bool,
}

/// Wire shape for `GET /api/status/cluster-health`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterHealthResponse {
    pub healthy: bool,
    pub nodes: Vec<ClusterNodeResponse>,
    pub edges: Vec<ClusterEdgeResponse>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_health_response_round_trips() {
        let response = ClusterHealthResponse {
            healthy: false,
            nodes: vec![
                ClusterNodeResponse {
                    name: "coordinator".to_string(),
                    port: 5432,
                    x: 0.0,
                    y: 1.0,
                },
                ClusterNodeResponse {
                    name: "worker-1".to_string(),
                    port: 5432,
                    x: 0.0,
                    y: -1.0,
                },
            ],
            edges: vec![ClusterEdgeResponse {
                source: 0,
                target: 1,
                healthy: false,
            }],
        };

        let json = serde_json::to_string(&response).unwrap();
        let round_tripped: ClusterHealthResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, response);
    }
}
