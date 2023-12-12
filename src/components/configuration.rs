// 3rd party imports
use dioxus::prelude::*;

// internal imports
use crate::entities::configuration::Configuration as MacPepDBConfiguration;

#[derive(Props)]
pub struct ConfigurationProps<'a> {
    pub macpepdb_configuration: &'a MacPepDBConfiguration,
}

/// Component for rendering MaCPepDB configuration
///
pub fn Configuration<'a>(cx: Scope<'a, ConfigurationProps<'a>>) -> Element {
    render! {
        div {
            h2 { "Settings" }
            table {
                tr {
                    th { "Property" }
                    th { "Value" }
                }
                tr {
                    td { "Protease" }
                    td { cx.props.macpepdb_configuration.get_protease_name() }
                }
                if let Some(max_number_of_missed_cleavages) = cx.props.macpepdb_configuration.get_max_number_of_missed_cleavages() {
                    render!{
                        tr {
                            td { "Max. number of missed cleavages" }
                            td { max_number_of_missed_cleavages.to_string() }
                        }
                    }
                }
                tr {
                    td { "Min. peptide length" }
                    td {
                        match cx.props.macpepdb_configuration.get_min_peptide_length() {
                            Some(min_peptide_length) => {
                                min_peptide_length.to_string()
                            }
                            None => {
                                "None".to_string()
                            }
                        }
                    }
                }
                tr {
                    td { "Max. peptide length" }
                    td {
                        match cx.props.macpepdb_configuration.get_max_peptide_length() {
                            Some(max_peptide_length) => {
                                max_peptide_length.to_string()
                            }
                            None => {
                                "None".to_string()
                            }
                        }
                    }
                }
                tr {
                    td { "Contain peptides with X" }
                    td {
                        match cx.props.macpepdb_configuration.get_remove_peptides_containing_unknown() {
                            true => {
                                "No".to_string()
                            }
                            false => {
                                "Yes".to_string()
                            }
                        }
                    }
                }
            }
        }
        div {
            h2 { "Distribution" }
            table {
                tr {
                    th { "Partition" }
                    th { "Upper limit" }
                }
                for (i, limit) in cx.props.macpepdb_configuration.get_partition_limits().iter().enumerate() {
                    render!{
                        tr {
                            td { (i + 1).to_string() }
                            td { limit.to_string() }
                        }
                    }
                }
            }
        }
    }
}
