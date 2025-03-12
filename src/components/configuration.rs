use std::rc::Rc;
// 3rd party imports
use dioxus::prelude::*;
use plotly::{layout::Axis, Layout, Plot, Scatter};

// internal imports
use crate::entities::configuration::Configuration as MacPepDBConfiguration;

const PARTITION_PLOT_ID: &str = "partition-plot";

#[derive(Clone, PartialEq, Props)]
pub struct ConfigurationProps {
    pub macpepdb_configuration: Rc<MacPepDBConfiguration>,
}

/// Component for rendering MaCPepDB configuration
///
pub fn Configuration(props: ConfigurationProps) -> Element {
    // Rc clone to send to the use_effect closure
    let macpepdb_config = props.macpepdb_configuration.clone();
    // Need to be made reactive to be trigger the effect
    use_effect(use_reactive(
        (macpepdb_config.as_ref(),),
        |(macpepdb_config,)| {
            document::eval(&format!(
                r#"
                        var c = document.getElementById("{}");
                        console.log(c);
                        "#,
                PARTITION_PLOT_ID
            ));
            let mut plot = Plot::new();
            let trace = Scatter::new(
                macpepdb_config
                    .get_partition_limits()
                    .iter()
                    .enumerate()
                    .map(|(idx, _)| idx)
                    .collect::<Vec<usize>>(),
                macpepdb_config.get_partition_limits().clone(),
            );
            plot.add_trace(trace);
            let x_axis = Axis::new().title("Partition Index");
            let y_axis = Axis::new().title("Mass limit (Da)");
            let layout = Layout::new().x_axis(x_axis).y_axis(y_axis);
            plot.set_layout(layout);
            spawn(async move {
                plotly::bindings::new_plot(PARTITION_PLOT_ID, &plot).await;
            });
        },
    ));

    rsx! {
        div {
            h2 { "Settings" }
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
                        td { "{props.macpepdb_configuration.get_protease_name()}" }
                    }
                    if let Some(max_number_of_missed_cleavages) = props
                        .macpepdb_configuration
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
                            match props.macpepdb_configuration.get_min_peptide_length() {
                                Some(min_peptide_length) => min_peptide_length.to_string(),
                                None => "None".to_string(),
                            }
                        }
                    }
                    tr {
                        td { "Max. peptide length" }
                        td {
                            match props.macpepdb_configuration.get_max_peptide_length() {
                                Some(max_peptide_length) => max_peptide_length.to_string(),
                                None => "None".to_string(),
                            }
                        }
                    }
                    tr {
                        td { "Contain peptides with X" }
                        td {
                            i { class: if !props.macpepdb_configuration.get_remove_peptides_containing_unknown() { "fas fa-check" } else { "fas fa-times" } }
                        }
                    }
                }
            }
        }
        div {
            h2 { "Mass partitions" }
            p {
                r#"Peptides in MaCPepDB are partitioned and distributed equally over the scylla cluster based on their theoretical mass in Dalton.
                The number of partitions and mass limits can be different for each MaCPepDB instance and depends on the number of initial proteins
                and the used protease."#
            }
            div { id: PARTITION_PLOT_ID }
        }
    }
}
