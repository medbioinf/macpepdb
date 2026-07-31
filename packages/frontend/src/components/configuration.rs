use dioxus::prelude::*;

use crate::{
    api_client::Client, components::spinner::Spinner,
    configuration::Configuration as AppConfiguration, errors::general_error::GeneralError,
};
use macpepdb_web_common::responses::configuration::RuntimeConfigurationResponse;

/// Component for rendering MaCPepDB configuration
///
pub fn Configuration() -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();
    let macpepdb_configuration: Resource<Result<RuntimeConfigurationResponse, GeneralError>> =
        use_resource(move || async move {
            let app_config = app_config.read();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Err(GeneralError::ConfigurationNotLoaded),
            };

            let client = Client::new(macpepdb_base_url)?;

            Ok(client.get_configuration().await?)
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
                                if let Some(comment) = config.comment.as_ref() {
                                    tr {
                                        td { "Comment" }
                                        td { "{comment}" }
                                    }
                                }
                                tr {
                                    td { "Protease" }
                                    td { "{config.protease.name}" }
                                }
                                tr {
                                    td { "Semi-specific cleavage" }
                                    td {
                                        i { class: if config.protease.semi_specific { "fas fa-check" } else { "fas fa-times" } }
                                    }
                                }
                                tr {
                                    td { "Max. number of missed cleavages" }
                                    td { "{config.protease.max_missed_cleavages}" }
                                }
                                tr {
                                    td { "Min. peptide length" }
                                    td { "{config.protease.min_length}" }
                                }
                                tr {
                                    td { "Max. peptide length" }
                                    td { "{config.protease.max_length}" }
                                }
                                tr {
                                    td { "Contain peptides with X" }
                                    td {
                                        i { class: if config.protease.keep_unknown { "fas fa-check" } else { "fas fa-times" } }
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
        }
    }
}
