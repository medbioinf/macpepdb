use anyhow::Result;
use dioxus::prelude::*;
use plotly::{layout::Axis, Layout, Plot, Scatter};

use crate::{
    components::spinner::Spinner, configuration::Configuration as AppConfiguration,
    entities::configuration::Configuration as MacPepDBConfiguration,
};

const PARTITION_PLOT_ID: &str = "partition-plot";

/// Fetch MaCPepDB configuration from the servers
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
///
pub async fn get_macpepdb_configuration(
    macpepdb_base_url: Signal<String>,
) -> Result<MacPepDBConfiguration> {
    let url = format!("{}/api/configuration", macpepdb_base_url);
    Ok(reqwest::get(&url)
        .await?
        .json::<MacPepDBConfiguration>()
        .await?)
}

/// Component for rendering MaCPepDB configuration
///
pub fn Configuration() -> Element {
    let app_config = use_context::<AppConfiguration>();
    let macpepdb_base_url = use_signal(|| app_config.get_macpepdb_base_url().to_owned());
    let macpepdb_configuration =
        use_resource(move || get_macpepdb_configuration(macpepdb_base_url));
    let mut is_partition_plot_element_mounted = use_signal(|| false);

    // Need to be made reactive to be trigger the effect
    use_effect(move || {
        if let Some(Ok(config)) = &*macpepdb_configuration.read_unchecked() {
            if is_partition_plot_element_mounted() {
                let mut plot = Plot::new();
                let trace = Scatter::new(
                    config
                        .get_partition_limits()
                        .iter()
                        .enumerate()
                        .map(|(idx, _)| idx)
                        .collect::<Vec<usize>>(),
                    config.get_partition_limits().clone(),
                );
                plot.add_trace(trace);
                let x_axis = Axis::new().title("Partition Index");
                let y_axis = Axis::new().title("Mass limit (Da)");
                let layout = Layout::new().x_axis(x_axis).y_axis(y_axis);
                plot.set_layout(layout);

                let eval = document::eval(
                    r#"
                    let plot_data = JSON.parse(await dioxus.recv());
                    Plotly.newPlot("partition-plot", plot_data);
                    "#,
                );

                match eval.send(plot.to_json()) {
                    Ok(_) => {}
                    Err(err) => {
                        web_sys::console::error_1(
                            &format!("Error when rendering partition plot: {:?}", err).into(),
                        );
                    }
                }

                // I expected this two work as well, but it produced an JS `arg0 is undefined`
                // which I was not able to debug. I suspect that the div-element is sometimes not
                // mounted. I tried several things to sync it but without success.
                // spawn(async move {
                //     plotly::bindings::new_plot("partition-plot", &plot).await;
                // });
            }
        }
    });

    rsx! {
        div {
            h2 { "Settings" }
            match &*macpepdb_configuration.read_unchecked() {
                Some(Ok(config)) => {
                    rsx! {
                        table { class: "table table-striped",
                            thead {
                                tr {
                                    th { "Attribute" }
                                    th { "Value" }
                                }
                            }
                            tbody {
                                tr {
                                    td { "Protease" }
                                    td { "{config.get_protease_name()}" }
                                }
                                if let Some(max_number_of_missed_cleavages) = config
                                    .get_max_number_of_missed_cleavages()
                                {
                                    tr {
                                        td { "Max. number of missed cleavages" }
                                        td { "{max_number_of_missed_cleavages}" }
                                    }
                                }
                                tr {
                                    td { "Min. peptide length" }
                                    td {
                                        match config.get_min_peptide_length() {
                                            Some(min_peptide_length) => min_peptide_length.to_string(),
                                            None => "None".to_string(),
                                        }
                                    }
                                }
                                tr {
                                    td { "Max. peptide length" }
                                    td {
                                        match config.get_max_peptide_length() {
                                            Some(max_peptide_length) => max_peptide_length.to_string(),
                                            None => "None".to_string(),
                                        }
                                    }
                                }
                                tr {
                                    td { "Contain peptides with X" }
                                    td {
                                        i { class: if !config.get_remove_peptides_containing_unknown() { "fas fa-check" } else { "fas fa-times" } }
                                    }
                                }
                            }
                        }
                    }
                }
                Some(Err(err)) => rsx! {
                    div { class: "alert alert-danger", "{err}" }
                },
                None => rsx! {
                    Spinner {}
                },
            }
            div {
                h2 { "Mass partitions" }
                p {
                    r#"Peptides in MaCPepDB are partitioned and distributed equally over the scylla cluster based on their theoretical mass in Dalton.
                    The number of partitions and mass limits can be different for each MaCPepDB instance and depends on the number of initial proteins
                    and the used protease."#
                }
                div { id: PARTITION_PLOT_ID, onmounted: move |_| is_partition_plot_element_mounted.set(true) }
            }
        }
    }
}
