// 3rd party imports
use anyhow::Result;
use dioxus::html::input_data::keyboard_types::Code;
use dioxus::prelude::*;
use dioxus_router::components::Link;
use log;
use reqwest;

// internal imports
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::protein::Protein as MaCPepDBProtein;
use crate::routes::Routes;

/// Fetch MaCPepDB configuration from the servers
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
/// * `protein_id` - Protein accession or gene nam
///
pub async fn get_proteins(
    macpepdb_base_url: String,
    protein_id: UseState<String>,
    proteins: UseRef<Option<Vec<MaCPepDBProtein>>>,
    is_searching_protein: UseState<bool>,
) -> Result<()> {
    if *is_searching_protein.get() {
        return Ok(());
    }
    is_searching_protein.set(true);
    let url = format!("{}/api/proteins/search/{}", macpepdb_base_url, protein_id);
    let response = reqwest::get(&url).await?;
    if response.status().is_success() {
        proteins.set(Some(response.json().await?));
    } else if response.status() == reqwest::StatusCode::NOT_FOUND {
        proteins.set(Some(vec![]));
    }
    is_searching_protein.set(false);
    Ok(())
}

pub fn ProteinSearch(cx: Scope) -> Element {
    let app_config = use_shared_state::<AppConfiguration>(cx).unwrap();

    let protein_id: &UseState<String> = use_state(cx, || "".to_string());
    let is_searching_protein = use_state(cx, || false);
    let proteins: &UseRef<Option<Vec<MaCPepDBProtein>>> = use_ref(cx, || None);

    render! {
        input {
            value: "{protein_id}",
            oninput: move |evt| protein_id.set(evt.value.clone()),
            onkeyup: move |evt| {
                if evt.code() == Code::Enter || evt.code() == Code::NumpadEnter {
                    let future = use_future(cx, (), move |_| get_proteins(
                        app_config.clone().read().get_macpepdb_base_url().to_owned(),
                        protein_id.clone(),
                        proteins.clone(),
                        is_searching_protein.clone(),
                    ));
                    match future.value() {
                        Some(_) => {},
                        None => {
                            log::error!("Error searching for protein");
                        }
                    }
                }
            },
        }
        button {
            r#type: "button",
            onclick: move |_| {
                let future = use_future(cx, (), move |_| get_proteins(
                    app_config.clone().read().get_macpepdb_base_url().to_owned(),
                    protein_id.clone(),
                    proteins.clone(),
                    is_searching_protein.clone(),
                ));
                match future.value() {
                    Some(_) => {},
                    None => {
                        log::error!("Error searching for protein");
                    }
                }
            },
            "Search"
        }

        if *is_searching_protein.get() {
            render! {
                div { "Searching ..." }
            }
        }

        match &*proteins.read() {
            Some(proteins) => {
                if !proteins.is_empty() {
                    render!{
                        ul {
                            for protein in proteins {
                                render!{
                                    li {
                                        Link{
                                            to: Routes::Protein{
                                                protein_id: protein.get_accession().to_string()
                                            },
                                            "{protein.get_accession()}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    render! {
                        p { "No proteins found" }
                    }
                }
            }
            None => {render!{ "" }}
        }
    }
}
