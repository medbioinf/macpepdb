use std::rc::Rc;

// 3rd party imports
use dioxus::prelude::*;

// internal imports
use crate::entities::configuration::Configuration as MacPepDBConfiguration;

#[derive(Clone, PartialEq, Props)]
pub struct ConfigurationProps {
    pub macpepdb_configuration: Rc<MacPepDBConfiguration>,
}

/// Component for rendering MaCPepDB configuration
///
pub fn Configuration(props: ConfigurationProps) -> Element {
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
            h2 { "Distribution" }
            table { class: "table table-striped table-sm",
                thead {
                    tr {
                        th { "Partition" }
                        th { "Upper limit" }
                    }
                }
                tbody {
                    for (i , limit) in props.macpepdb_configuration.get_partition_limits().iter().enumerate() {
                        tr {
                            td { "{(i + 1)}" }
                            td { "{limit}" }
                        }
                    }
                }
            }
        }
    }
}
