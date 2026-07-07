use dioxus::prelude::*;

use crate::api_client::Client;
use crate::components::rounded_mass::RoundedMass;
use crate::components::spinner::Spinner;
use crate::configuration::Configuration as AppConfiguration;
use crate::errors::general_error::GeneralError;
use crate::errors::peptide_search_page_error::PeptideSearchPageError;
use crate::routes::Routes;
use macpepdb_web_common::responses::peptide::PeptideResponse;

/// Default minimum sequence length to search for
///
const DEFAULT_MINIMUM_SEQUENCE_LENGTH_TO_SEARCH: usize = 6;

pub fn SequenceSearch() -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();
    let mut sequence = use_signal(|| "".to_string());

    let minimum_sequence_length_to_search = use_resource(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Err(GeneralError::ConfigurationNotLoaded),
        };

        let client = Client::new(macpepdb_base_url)?;

        Ok(client
            .get_configuration()
            .await
            .map_or(DEFAULT_MINIMUM_SEQUENCE_LENGTH_TO_SEARCH, |config| {
                config.protease.min_length
            }))
    });

    // Action to search for peptide
    let mut peptide: Action<(), PeptideResponse> = use_action(move || async move {
        let min_peptide_length = match &*minimum_sequence_length_to_search.read_unchecked() {
            Some(Ok(length)) => *length,
            _ => DEFAULT_MINIMUM_SEQUENCE_LENGTH_TO_SEARCH,
        };

        if sequence.read_unchecked().len() < min_peptide_length {
            return Err(GeneralError::from(
                PeptideSearchPageError::PeptideTooShortError(min_peptide_length),
            ));
        }

        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Err(GeneralError::ConfigurationNotLoaded),
        };

        let client = Client::new(macpepdb_base_url)?;

        client
            .get_peptide(&sequence.read_unchecked())
            .await
            .map_err(GeneralError::from)
    });

    rsx! {
        div { class: "input-group mb-3",
            input {
                class: "form-control",
                r#type: "text",
                placeholder: "Partial protein accession or full gene name",
                value: "{sequence}",
                oninput: move |evt| sequence.set(evt.value()),
                onkeyup: move |evt| {
                    if evt.code() == Code::Enter || evt.code() == Code::NumpadEnter {
                        peptide.call();
                    }
                },
            }
            button {
                class: "btn btn-primary",
                r#type: "button",
                onclick: move |_| peptide.call(),
                "Search"
            }
        }

        match peptide.value() {
            Some(Ok(peptide)) => rsx! {
                table { class: "table table-striped table-hover table-sm table-responsive",
                    thead {
                        tr {
                            th { "Mass (Da)" }
                            th { "Sequence" }
                        }
                    }
                    tbody {
                        tr {
                            td {
                                RoundedMass { mass: peptide.read().mass }
                            }
                            td { class: "text-break",
                                Link {
                                    to: Routes::Peptide {
                                        peptide_sequence: peptide.read().sequence.clone(),
                                    },
                                    "{peptide.read().sequence}"
                                }
                            }
                        }
                    }
                }
            },
            Some(Err(err)) => rsx! {
                div { class: "alert alert-danger", "Error searching for peptide: {err}" }
            },
            None => rsx! {
                if peptide.pending() {
                    Spinner {}
                }
            },
        }
    }
}
