use std::rc::Rc;

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
pub async fn get_proteins(
    macpepdb_base_url: Signal<String>,
    protein_id: Signal<String>,
) -> Result<Rc<Vec<ProteinEntity>>> {
    let url = format!("{macpepdb_base_url}/api/proteins/search/{protein_id}");
    Ok(Rc::new(
        reqwest::get(&url)
            .await?
            .json::<Vec<ProteinEntity>>()
            .await?,
    ))
}

/// Search for proteins by accession or gene name
///
pub fn ProteinSearch() -> Element {
    let app_config = use_context::<AppConfiguration>();
    let macpepdb_base_url = use_signal(|| app_config.get_macpepdb_base_url().to_owned());
    let mut protein_id = use_signal(|| "".to_string());
    let mut fetch_status: Signal<FetchStatus> = use_signal(|| FetchStatus::None);

    // Event handler for fetching proteins on button click or on enter
    //
    let mut proteins: Resource<Result<Option<Rc<Vec<ProteinEntity>>>>> =
        use_resource(move || async move {
            if protein_id.read_unchecked().is_empty() {
                fetch_status.set(FetchStatus::None);
                return Ok(None);
            }
            fetch_status.set(FetchStatus::Loading);
            let proteins = get_proteins(macpepdb_base_url, protein_id).await?;
            fetch_status.set(FetchStatus::Finished);
            Ok(Some(proteins))
        });

    rsx! {
        h3 { "Search for proteins" }
        div { class: "input-group mb-3",
            input {
                class: "form-control",
                r#type: "text",
                placeholder: "Partial protein accession or full gene name",
                value: "{protein_id}",
                oninput: move |evt| protein_id.set(evt.value()),
                onkeyup: move |evt| {
                    if evt.code() == Code::Enter || evt.code() == Code::NumpadEnter {
                        proteins.restart()
                    }
                },
            }
            button {
                class: "btn btn-primary",
                r#type: "button",
                onclick: move |_| proteins.restart(),
                "Search"
            }
        }

        match &*proteins.read_unchecked() {
            Some(Ok(None)) => {
                rsx! { "" }
            }
            Some(Ok(Some(proteins))) => {
                rsx! {
                    ProteinList { proteins: proteins.clone() }
                }
            }
            Some(Err(err)) => {
                rsx! {
                    div { "Error fetching proteins: {err}" }
                }
            }
            None => {
                rsx! {
                    div { "Loading..." }
                }
            }
        }
    }
}
