use dioxus::prelude::*;

use crate::{
    api_client::Client, components::configuration::*, components::spinner::Spinner,
    configuration::Configuration as AppConfiguration, errors::general_error::GeneralError,
    tracking::track_page_visit,
};
use macpepdb_web_common::responses::cluster_health::ClusterHealthResponse;

/// Builds the Plotly traces/layout from the received graph and renders them into
/// `#cluster-health-plot`. Plotly.js has no built-in graph-layout algorithm, so the backend
/// already precomputed node positions (`x`/`y`); this script only shapes them into traces.
/// `null` entries in the `x`/`y` arrays are Plotly's standard "break the line here" gap marker,
/// used to draw one disconnected line segment per edge within a single trace.
const CLUSTER_HEALTH_PLOT_SCRIPT: &str = r#"
    const data = await dioxus.recv();
    const okX = [], okY = [], badX = [], badY = [];
    for (const e of data.edges) {
        const a = data.nodes[e.source];
        const b = data.nodes[e.target];
        const xs = e.healthy ? okX : badX;
        const ys = e.healthy ? okY : badY;
        xs.push(a.x, b.x, null);
        ys.push(a.y, b.y, null);
    }
    const traces = [
        { x: okX, y: okY, mode: "lines", line: { color: "blue" }, name: "Working" },
        { x: badX, y: badY, mode: "lines", line: { color: "green" }, name: "Failed" },
        {
            x: data.nodes.map(n => n.x),
            y: data.nodes.map(n => n.y),
            mode: "markers",
            marker: { color: "gray" },
            name: "Nodes",
        },
    ];
    const annotations = data.nodes.map(n => ({
        x: n.x,
        y: n.y,
        text: `${n.name}:${n.port}`,
        showarrow: false,
        yshift: 16,
        font: { size: 16, weight: "bold" },
        bgcolor: "rgba(255, 255, 255, 0.75)",
    }));
    const layout = {
        title: data.healthy ? "Cluster health: OK" : "Cluster health: DEGRADED",
        xaxis: { visible: false },
        yaxis: { visible: false },
        showlegend: true,
        annotations: annotations,
    };
    Plotly.newPlot("cluster-health-plot", traces, layout);
"#;

pub fn Status() -> Element {
    use_future(move || async move { track_page_visit(vec![]).await });

    let app_config = use_context::<Resource<AppConfiguration>>();

    let cluster_health: Resource<Result<ClusterHealthResponse, GeneralError>> =
        use_resource(move || async move {
            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Err(GeneralError::ConfigurationNotLoaded),
            };

            let client = Client::new(macpepdb_base_url)?;
            Ok(client.get_cluster_health().await?)
        });

    use_effect(move || {
        if let Some(Ok(response)) = &*cluster_health.read_unchecked() {
            let eval = document::eval(CLUSTER_HEALTH_PLOT_SCRIPT);
            if let Err(err) = eval.send(response) {
                error!("Failed to send cluster health data to Plotly: {err}");
            }
        }
    });

    rsx! {
        div {
            h1 { "Welcome to MaCPepDB - Mass Centric Peptide Database" }
            div {
                p { "Quickly build and access the digest of a large proteome." }
            }
        }
        Configuration {}
        div {
            h2 { "Inter-node connections" }
            match &*cluster_health.read_unchecked() {
                Some(Ok(response)) => {
                    let status_text = if response.healthy { "OK" } else { "DEGRADED" };
                    rsx! {
                        p { "Status: {status_text}" }
                        div { id: "cluster-health-plot" }
                    }
                }
                Some(Err(err)) => rsx! {
                    div { class: "alert alert-danger", "Error getting cluster health: {err}" }
                },
                None => rsx! {
                    if cluster_health.pending() {
                        Spinner {}
                    }
                },
            }
        }
    }
}
