// 3rd party imports
use dioxus::prelude::*;

// internal imports
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::configuration::Configuration as MacPepDBConfiguration;

/// Fetch MaCPepDB configuration from the servers
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
///
pub async fn get_macpepdb_configuration(
    macpepdb_base_url: String,
) -> Result<MacPepDBConfiguration, reqwest::Error> {
    let url = format!("{}/api/configuration", macpepdb_base_url);
    reqwest::get(&url).await?.json().await
}

/// Component for rendering MaCPepDB configuration
///
pub fn Configuration(cx: Scope) -> Element {
    let app_config = use_shared_state::<AppConfiguration>(cx).unwrap().read();
    let mdb_config = use_future(cx, (), |_| {
        get_macpepdb_configuration(app_config.get_macpepdb_base_url().to_owned())
    });

    match mdb_config.value() {
        Some(Ok(mdb_config)) => {
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
                            td { mdb_config.get_protease_name() }
                        }
                        if let Some(max_number_of_missed_cleavages) = mdb_config.get_max_number_of_missed_cleavages() {
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
                                match mdb_config.get_min_peptide_length() {
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
                                match mdb_config.get_max_peptide_length() {
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
                                match mdb_config.get_remove_peptides_containing_unknown() {
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
                        for (i, limit) in mdb_config.get_partition_limits().iter().enumerate() {
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
        Some(Err(e)) => {
            render! {
                div { "{e}" }
            }
        }
        None => {
            render! {
                div { "Loading ..." }
            }
        }
    }
}
