use dioxus::prelude::*;
use plotly::{
    common::{Font, Line, Marker, Mode, Title},
    layout::{Annotation, Axis},
    Layout, Plot, Scatter,
};

use crate::{
    api_client::Client, components::configuration::*, components::spinner::Spinner,
    configuration::Configuration as AppConfiguration, errors::general_error::GeneralError,
    tracking::track_page_visit,
};
use macpepdb_web_common::responses::cluster_health::ClusterHealthResponse;

/// Builds the Plotly traces/layout from the received graph and renders them into
/// `#cluster-health-plot`. Plotly.js has no built-in graph-layout algorithm, so the backend
/// already precomputed node positions (`x`/`y`); this only shapes them into traces.
/// `None` entries in the `x`/`y` vectors are Plotly's standard "break the line here" gap marker,
/// used to draw one disconnected line segment per edge within a single trace.
fn build_cluster_health_plot(data: &ClusterHealthResponse) -> Plot {
    let (mut ok_x, mut ok_y, mut bad_x, mut bad_y) = (vec![], vec![], vec![], vec![]);
    for edge in &data.edges {
        let a = &data.nodes[edge.source];
        let b = &data.nodes[edge.target];
        let (xs, ys) = if edge.healthy {
            (&mut ok_x, &mut ok_y)
        } else {
            (&mut bad_x, &mut bad_y)
        };
        xs.extend([Some(a.x), Some(b.x), None]);
        ys.extend([Some(a.y), Some(b.y), None]);
    }

    let ok_trace = Scatter::new(ok_x, ok_y)
        .mode(Mode::Lines)
        .line(Line::new().color("blue"))
        .name("Working");
    let bad_trace = Scatter::new(bad_x, bad_y)
        .mode(Mode::Lines)
        .line(Line::new().color("green"))
        .name("Failed");
    let node_trace = Scatter::new(
        data.nodes.iter().map(|n| n.x).collect::<Vec<_>>(),
        data.nodes.iter().map(|n| n.y).collect::<Vec<_>>(),
    )
    .mode(Mode::Markers)
    .marker(Marker::new().color("gray"))
    .name("Nodes");

    let annotations = data
        .nodes
        .iter()
        .map(|n| {
            Annotation::new()
                .x(n.x)
                .y(n.y)
                .text(format!("<b>{}:{}</b>", n.name, n.port))
                .show_arrow(false)
                .y_shift(16.0)
                .font(Font::new().size(16))
                .background_color("rgba(255, 255, 255, 0.75)")
        })
        .collect();

    let layout = Layout::new()
        .title(Title::from("Inter-cluster connections"))
        .x_axis(Axis::new().visible(false))
        .y_axis(Axis::new().visible(false))
        .show_legend(true)
        .annotations(annotations);

    let mut plot = Plot::new();
    plot.add_trace(ok_trace);
    plot.add_trace(bad_trace);
    plot.add_trace(node_trace);
    plot.set_layout(layout);
    plot
}

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
            let plot = build_cluster_health_plot(response);
            spawn(async move {
                plotly::bindings::new_plot("cluster-health-plot", &plot).await;
            });
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
            h2 { "Database cluster" }
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
