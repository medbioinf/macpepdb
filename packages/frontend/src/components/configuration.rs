use dioxus::prelude::*;
use plotly::{layout::Axis, Layout, Plot, Scatter};

use crate::{
    api_client::Client, components::spinner::Spinner,
    configuration::Configuration as AppConfiguration,
    entities::configuration::Configuration as MacPepDBConfiguration,
    errors::general_error::GeneralError,
};

const PARTITION_PLOT_ID: &str = "partition-plot";

/// Component for rendering MaCPepDB configuration
///
pub fn Configuration() -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();
    let macpepdb_configuration: Resource<Result<MacPepDBConfiguration, GeneralError>> =
        use_resource(move || async move {
            let app_config = app_config.read();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Err(GeneralError::ConfigurationNotLoaded),
            };

            let client = Client::new(macpepdb_base_url)?;

            Ok(client.get_configuration().await?)
        });

    use_effect(move || {
        let config = macpepdb_configuration.read();
        let partition_limits = match config.as_ref() {
            Some(Ok(c)) => c.get_partition_limits().to_vec(),
            _ => return,
        };
        drop(config);

        spawn(async move {
            // The Plotly JS library is fetched asynchronously from CDN after WASM starts,
            // so we poll until it's available before trying to render.
            loop {
                let ready = js_sys::eval("typeof window.Plotly !== 'undefined'")
                    .ok()
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if ready {
                    break;
                }
                gloo_timers::future::TimeoutFuture::new(50).await;
            }

            let mut plot = Plot::new();
            let trace = Scatter::new(
                partition_limits
                    .iter()
                    .enumerate()
                    .map(|(idx, _)| idx)
                    .collect::<Vec<usize>>(),
                partition_limits.to_vec(),
            );

            plot.add_trace(trace);
            let x_axis = Axis::new().title("Partition Index");
            let y_axis = Axis::new().title("Mass limit (Da)");
            let layout = Layout::new().x_axis(x_axis).y_axis(y_axis);
            plot.set_layout(layout);

            #[cfg(target_arch = "wasm32")]
            plotly::bindings::react(PARTITION_PLOT_ID, &plot).await;
        });
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
                    div { class: "alert alert-danger", "Error getting configuration: {err}" }
                },
                None => rsx! {
                    if macpepdb_configuration.pending() {
                        Spinner {}
                    }
                },
            }
            div {
                h2 { "Mass partitions" }
                p {
                    r#"Peptides in MaCPepDB are partitioned and distributed equally over the scylla cluster based on their theoretical mass in Dalton.
                    The number of partitions and mass limits can be different for each MaCPepDB instance and depends on the number of initial proteins
                    and the used protease."#
                }
                div {
                    id: PARTITION_PLOT_ID,
                }
            }
        }
    }
}
