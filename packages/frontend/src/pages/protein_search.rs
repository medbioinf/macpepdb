use std::collections::HashMap;
use std::sync::Arc;

use dioxus::html::input_data::keyboard_types::Code;
use dioxus::prelude::*;
use macpepdb_web_common::responses::protein::ProteinResponse;

use crate::api_client::Client;
use crate::components::protein_list::ProteinList;
use crate::components::spinner::Spinner;
use crate::configuration::Configuration as AppConfiguration;
use crate::errors::api_client_error::ApiClientError;
use crate::errors::general_error::GeneralError;
use crate::errors::protein_search_page_error::ProteinSearchPageError;
use crate::tracking::track_page_visit;

/// Minimum length of search term
///
const MIN_SEARCH_TERM_LENGTH: usize = 3;

/// Search for proteins by accession or gene name
///
pub fn ProteinSearch() -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();
    let mut protein_id = use_signal(|| "".to_string());

    let mut proteins = use_action(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Err(GeneralError::ConfigurationNotLoaded),
        };

        if protein_id.read().len() < MIN_SEARCH_TERM_LENGTH {
            return Err(ProteinSearchPageError::SearchTermTooShort(MIN_SEARCH_TERM_LENGTH).into());
        }

        let client = Client::new(macpepdb_base_url)?;

        let fetched_proteins: Result<Vec<ProteinResponse<String>>, ApiClientError> =
            client.search_protein(&protein_id.read()).await;

        match fetched_proteins {
            Ok(fetched_proteins) => Ok(Arc::new(fetched_proteins)),
            Err(err) => Err(err.into()),
        }
    });

    let taxonomy_names: Resource<Result<HashMap<i32, String>, GeneralError>> =
        use_resource(move || async move {
            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Err(GeneralError::ConfigurationNotLoaded),
            };

            let ids: Vec<i32> = match proteins.value() {
                Some(Ok(sig)) => {
                    let mut ids: Vec<i32> = sig.read().iter().map(|p| p.taxonomy_id).collect();
                    ids.sort_unstable();
                    ids.dedup();
                    ids
                }
                _ => return Ok(HashMap::new()),
            };

            if ids.is_empty() {
                return Ok(HashMap::new());
            }

            let client = Client::new(macpepdb_base_url)?;
            Ok(client.resolve_taxonomy_ids(ids).await?)
        });

    use_future(move || async move { track_page_visit(vec![]).await });

    rsx! {
        h3 { "Search for proteins" }
        div { class: "input-group mb-3",
            input {
                class: "form-control",
                r#type: "text",
                placeholder: "Protein accession or gene name",
                value: "{protein_id}",
                oninput: move |evt| protein_id.set(evt.value()),
                onkeyup: move |evt| {
                    if evt.code() == Code::Enter || evt.code() == Code::NumpadEnter {
                        proteins.call();
                    }
                },
            }
            button {
                class: "btn btn-primary",
                r#type: "button",
                onclick: move |_| proteins.call(),
                "Search"
            }
        }
        match proteins.value() {
            Some(Ok(proteins)) => rsx! {
                ProteinList { proteins: proteins.read().clone(), taxonomy_names }
            },
            Some(Err(err)) => rsx! {
                div { class: "alert alert-danger", "Error getting proteins: {err}" }
            },
            None => rsx! {
                if proteins.pending() {
                    Spinner {}
                }
            },
        }
    }
}
