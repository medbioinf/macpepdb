// 3rd party imports
use anyhow::Result;
use dioxus::html::input_data::keyboard_types::Code;
use dioxus::prelude::*;
use reqwest;

// internal imports
use crate::api_helpers::fetch_status::FetchStatus;
use crate::components::protein_list::ProteinList;
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::peptide::Peptide as MaCPepDBPeptide;
use crate::entities::protein::Protein as MaCPepDBProtein;

/// Proteins downloaded via the proteins endpoint contains full peptide entries instead of sequences,
/// but the peptide's proteins only contain protein accession.
type ProteinEntity = MaCPepDBProtein<MaCPepDBPeptide<String>>;

/// Fetch MaCPepDB configuration from the servers
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
/// * `protein_id` - Protein accession or gene nam
///
pub async fn get_proteins(macpepdb_base_url: &str, protein_id: &str) -> Result<Vec<ProteinEntity>> {
    let url = format!("{}/api/proteins/search/{}", macpepdb_base_url, protein_id);
    Ok(reqwest::get(&url).await?.json().await?)
}

/// Search for proteins by accession or gene name
///
pub fn ProteinSearch(cx: Scope) -> Element {
    let app_config = use_shared_state::<AppConfiguration>(cx).unwrap();
    let protein_id = use_state(cx, || "".to_string());
    let proteins = use_state(cx, || FetchStatus::<Result<Vec<ProteinEntity>>>::None);

    // Event handler for fetching proteins on button click or on enter
    //
    let fetch_proteins = move || {
        // prevent redundant requests
        match proteins.get() {
            FetchStatus::Loading => {
                return;
            }
            _ => {}
        }
        cx.spawn({
            let macpepdb_base_url = app_config.read().get_macpepdb_base_url().to_owned();
            let proteins = proteins.to_owned();
            let protein_id = protein_id.to_owned();

            async move {
                proteins.set(FetchStatus::Loading);
                proteins.set(FetchStatus::Finished(
                    get_proteins(&macpepdb_base_url, protein_id.get()).await,
                ));
            }
        });
    };

    render! {
        input {
            value: "{protein_id}",
            oninput: move |evt| protein_id.set(evt.value.clone()),
            onkeyup: move |evt| {
                if evt.code() == Code::Enter || evt.code() == Code::NumpadEnter {
                    fetch_proteins()
                }
            }
        }
        button {
            r#type: "button",
            onclick: move |_| fetch_proteins(),
            "Search"
        }

        match proteins.get() {
            FetchStatus::None => {
                render! {""}
            }
            FetchStatus::Loading => {
                render! {
                    div { "Loading..." }
                }
            }
            FetchStatus::Finished(Ok(ref proteins)) => {
                render! {
                    ProteinList {
                        proteins: proteins.iter().collect()
                    }
                }
            }
            FetchStatus::Finished(Err(ref err)) => {
                render! {
                    div { "Error fetching proteins: {err}" }
                }
            }
        }

    }
}
