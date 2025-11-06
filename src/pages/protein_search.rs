use std::rc::Rc;

use anyhow::{anyhow, Result};
use dioxus::html::input_data::keyboard_types::Code;
use dioxus::prelude::*;
use futures_util::StreamExt;

use crate::api_helpers::fetch_status::FetchStatus;
use crate::components::protein_list::ProteinList;
use crate::components::spinner::Spinner;
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::peptide::Peptide as MaCPepDBPeptide;
use crate::entities::protein::Protein as MaCPepDBProtein;
use crate::tracking::track_page_visit;

/// Proteins downloaded via the proteins endpoint contains full peptide entries instead of sequences,
/// but the peptide's proteins only contain protein accession.
type ProteinEntity = MaCPepDBProtein<MaCPepDBPeptide<String>>;

/// Minimum length of search term
///
const MIN_SEARCH_TERM_LENGTH: usize = 3;

/// Fetch MaCPepDB configuration from the servers
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
/// * `protein_id` - Protein accession or gene nam
///
pub async fn get_proteins(macpepdb_base_url: &str, protein_id: &str) -> Result<Vec<ProteinEntity>> {
    let url = format!("{macpepdb_base_url}/api/proteins/search/{protein_id}");
    Ok(reqwest::get(&url)
        .await?
        .json::<Vec<ProteinEntity>>()
        .await?)
}

/// Search for proteins by accession or gene name
///
pub fn ProteinSearch() -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();
    let mut protein_id = use_signal(|| "".to_string());

    // search peptides
    let mut proteins = use_signal(|| FetchStatus::None);
    let search_coroutine = use_coroutine(move |mut rx: UnboundedReceiver<()>| async move {
        while rx.next().await.is_some() {
            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => {
                    proteins.set(FetchStatus::Error(anyhow!("App configuration not loaded")));
                    continue;
                }
            };

            if protein_id.read().len() < MIN_SEARCH_TERM_LENGTH {
                proteins.set(FetchStatus::Error(anyhow!(
                    "Search term too short, must be at least {} characters",
                    MIN_SEARCH_TERM_LENGTH
                )));
                continue;
            }

            proteins.set(FetchStatus::Loading);
            match get_proteins(macpepdb_base_url, protein_id.read_unchecked().as_str()).await {
                Ok(fetched_proteins) => {
                    proteins.set(FetchStatus::Finished(Rc::new(fetched_proteins)));
                }
                Err(err) => {
                    proteins.set(FetchStatus::Error(err));
                }
            }
        }
    });

    let _ = use_resource(move || async move { track_page_visit(vec![]).await });

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
                        search_coroutine.send(())
                    }
                },
            }
            button {
                class: "btn btn-primary",
                r#type: "button",
                onclick: move |_| search_coroutine.send(()),
                "Search"
            }
        }
        match &*proteins.read_unchecked() {
            FetchStatus::None => {
                rsx! { "" }
            }
            FetchStatus::Loading => {
                rsx! {
                    Spinner {}
                }
            }
            FetchStatus::Finished(proteins) => {
                rsx! {
                    ProteinList { proteins: proteins.clone() }
                }
            }
            FetchStatus::Error(err) => {
                rsx! {
                    div { class: "alert alert-danger", "Error getting proteins: {err}" }
                }
            }
        }
    }
}
