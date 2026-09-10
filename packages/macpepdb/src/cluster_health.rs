use thiserror::Error;

use crate::client::Client;

static SELECT_STATEMENT: &str = "SELECT * FROM citus_check_cluster_node_health()";

/// Radius (in plot units) of the circle nodes are laid out on.
const LAYOUT_RADIUS: f64 = 1.0;

/// Errors that can occur while checking Citus cluster node health.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error while checking cluster health: {0}")]
    Client(#[from] crate::client::Error),
    #[error("Row decoding error while checking cluster health: {0}")]
    Row(#[from] tokio_postgres::Error),
}

/// A Citus node identified by its hostname and port.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeRef {
    pub name: String,
    pub port: i32,
}

/// One row of `citus_check_cluster_node_health()`: whether `from` could reach `to`.
#[derive(Debug, Clone)]
pub struct NodeConnection {
    pub from: NodeRef,
    pub to: NodeRef,
    pub healthy: bool,
}

/// A node in the connectivity graph, laid out on a circle for rendering.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphNode {
    pub name: String,
    pub port: i32,
    pub x: f64,
    pub y: f64,
}

/// An edge between two nodes, `source`/`target` are indices into the [`GraphNode`] list
/// returned alongside it.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphEdge {
    pub source: usize,
    pub target: usize,
    pub healthy: bool,
}

/// Runs `citus_check_cluster_node_health()` and returns one [`NodeConnection`] per row.
pub async fn check(client: &Client) -> Result<Vec<NodeConnection>, Error> {
    let rows = client.query(SELECT_STATEMENT, &[]).await?;

    rows.into_iter()
        .map(|row| {
            Ok(NodeConnection {
                from: NodeRef {
                    name: row.try_get::<_, String>(0)?,
                    port: row.try_get::<_, i32>(1)?,
                },
                to: NodeRef {
                    name: row.try_get::<_, String>(2)?,
                    port: row.try_get::<_, i32>(3)?,
                },
                healthy: row.try_get::<_, bool>(4)?,
            })
        })
        .collect()
}

/// Whether every reported connection is healthy.
pub fn is_healthy(connections: &[NodeConnection]) -> bool {
    connections.iter().all(|connection| connection.healthy)
}

/// Builds a node/edge graph from the raw connection rows: unique nodes laid out on a circle,
/// and one edge per unordered node pair (self-loops dropped, both directions merged — an edge
/// is only healthy if every direction reported for that pair was healthy).
pub fn build_graph(connections: &[NodeConnection]) -> (Vec<GraphNode>, Vec<GraphEdge>) {
    let mut node_refs: Vec<NodeRef> = Vec::new();
    for connection in connections {
        if !node_refs.contains(&connection.from) {
            node_refs.push(connection.from.clone());
        }
        if !node_refs.contains(&connection.to) {
            node_refs.push(connection.to.clone());
        }
    }

    let node_count = node_refs.len().max(1);
    let nodes: Vec<GraphNode> = node_refs
        .iter()
        .enumerate()
        .map(|(index, node_ref)| {
            let angle = 2.0 * std::f64::consts::PI * index as f64 / node_count as f64
                - std::f64::consts::FRAC_PI_2;
            GraphNode {
                name: node_ref.name.clone(),
                port: node_ref.port,
                x: LAYOUT_RADIUS * angle.cos(),
                y: LAYOUT_RADIUS * angle.sin(),
            }
        })
        .collect();

    let mut edges: Vec<GraphEdge> = Vec::new();
    for connection in connections {
        if connection.from == connection.to {
            continue;
        }

        let source = node_refs
            .iter()
            .position(|node_ref| *node_ref == connection.from)
            .expect("node was just inserted above");
        let target = node_refs
            .iter()
            .position(|node_ref| *node_ref == connection.to)
            .expect("node was just inserted above");
        let (source, target) = if source <= target {
            (source, target)
        } else {
            (target, source)
        };

        match edges
            .iter_mut()
            .find(|edge| edge.source == source && edge.target == target)
        {
            Some(edge) => edge.healthy &= connection.healthy,
            None => edges.push(GraphEdge {
                source,
                target,
                healthy: connection.healthy,
            }),
        }
    }

    (nodes, edges)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connection(from: (&str, i32), to: (&str, i32), healthy: bool) -> NodeConnection {
        NodeConnection {
            from: NodeRef {
                name: from.0.to_string(),
                port: from.1,
            },
            to: NodeRef {
                name: to.0.to_string(),
                port: to.1,
            },
            healthy,
        }
    }

    #[test]
    fn is_healthy_true_when_all_connections_healthy() {
        let connections = vec![
            connection(("coordinator", 5432), ("worker-1", 5432), true),
            connection(("worker-1", 5432), ("coordinator", 5432), true),
        ];
        assert!(is_healthy(&connections));
    }

    #[test]
    fn is_healthy_false_when_any_connection_failed() {
        let connections = vec![
            connection(("coordinator", 5432), ("worker-1", 5432), true),
            connection(("worker-1", 5432), ("coordinator", 5432), false),
        ];
        assert!(!is_healthy(&connections));
    }

    #[test]
    fn build_graph_dedups_nodes_and_skips_self_loops() {
        let connections = vec![
            connection(("coordinator", 5432), ("coordinator", 5432), true),
            connection(("coordinator", 5432), ("worker-1", 5432), true),
            connection(("worker-1", 5432), ("coordinator", 5432), true),
            connection(("worker-1", 5432), ("worker-1", 5432), true),
        ];

        let (nodes, edges) = build_graph(&connections);

        assert_eq!(nodes.len(), 2);
        assert_eq!(edges.len(), 1);
        assert!(edges[0].healthy);
    }

    #[test]
    fn build_graph_marks_edge_unhealthy_if_either_direction_failed() {
        let connections = vec![
            connection(("coordinator", 5432), ("worker-1", 5432), true),
            connection(("worker-1", 5432), ("coordinator", 5432), false),
        ];

        let (_, edges) = build_graph(&connections);

        assert_eq!(edges.len(), 1);
        assert!(!edges[0].healthy);
    }

    #[test]
    fn build_graph_lays_nodes_on_a_circle() {
        let connections = vec![
            connection(("a", 1), ("b", 1), true),
            connection(("b", 1), ("c", 1), true),
        ];

        let (nodes, _) = build_graph(&connections);

        assert_eq!(nodes.len(), 3);
        for node in &nodes {
            let radius = (node.x * node.x + node.y * node.y).sqrt();
            assert!((radius - LAYOUT_RADIUS).abs() < 1e-9);
        }
    }
}
