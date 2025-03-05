use anyhow::{bail, Result};
use dioxus::prelude::*;
use reqwest::StatusCode;

use crate::components::rounded_mass::RoundedMass;
use crate::components::spinner::Spinner;
use crate::entities::configuration::Configuration as MacPepDBConfiguration;
use crate::entities::peptide::Peptide as MaCPepDBPeptide;
use crate::entities::protein::Protein as MaCPepDBProtein;
use crate::routes::Routes;
use crate::{
    api_helpers::fetch_status::FetchStatus, configuration::Configuration as AppConfiguration,
};

type PeptideEntity = MaCPepDBPeptide<MaCPepDBProtein<String>>;

/// Default minimum sequence length to search for
///
const DEFAULT_MINIMUM_SEQUENCE_LENGTH_TO_SEARCH: usize = 6;

/// Fetch MaCPepDB configuration from the servers
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
///
pub async fn get_macpepdb_configuration(
    macpepdb_base_url: Signal<String>,
) -> Result<MacPepDBConfiguration> {
    let url = format!("{macpepdb_base_url}/api/configuration",);
    Ok(reqwest::get(&url)
        .await?
        .json::<MacPepDBConfiguration>()
        .await?)
}

/// Get peptide
///
pub async fn get_peptide(
    macpepdb_base_url: Signal<String>,
    sequence: Signal<String>,
) -> Result<Option<PeptideEntity>> {
    let url = format!("{macpepdb_base_url}/api/peptides/{sequence}",);
    let response = reqwest::get(&url).await?;
    match response.status() {
        StatusCode::OK => Ok(Some(response.json::<PeptideEntity>().await?)),
        StatusCode::NOT_FOUND => Ok(None),
        _ => bail!(response.text().await?),
    }
}

pub fn SequenceSearch() -> Element {
    let app_config = use_context::<AppConfiguration>();
    let macpepdb_base_url = use_signal(|| app_config.get_macpepdb_base_url().to_owned());
    let mut sequence = use_signal(|| "".to_string());
    let mut fetch_status: Signal<FetchStatus<()>> = use_signal(|| FetchStatus::None);

    let minimum_sequence_length_to_search = use_resource(move || async move {
        match get_macpepdb_configuration(macpepdb_base_url).await {
            Ok(config) => config.get_min_peptide_length().unwrap_or(1),
            Err(_) => DEFAULT_MINIMUM_SEQUENCE_LENGTH_TO_SEARCH,
        }
    });

    // Event handler for fetching proteins on button click or on enter
    //
    let mut peptide: Resource<Result<Option<PeptideEntity>>> = use_resource(move || async move {
        if sequence.read_unchecked().len()
            < minimum_sequence_length_to_search
                .read_unchecked()
                .unwrap_or(DEFAULT_MINIMUM_SEQUENCE_LENGTH_TO_SEARCH)
        {
            fetch_status.set(FetchStatus::None);
            return Ok(None);
        }
        fetch_status.set(FetchStatus::Loading);
        let peptide = get_peptide(macpepdb_base_url, sequence).await?;
        fetch_status.set(FetchStatus::Finished(()));
        Ok(peptide)
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
                        peptide.restart()
                    }
                },
            }
            button {
                class: "btn btn-primary",
                r#type: "button",
                onclick: move |_| peptide.restart(),
                "Search"
            }
        }

        match &*fetch_status.read_unchecked() {
            FetchStatus::None => {
                rsx! { "" }
            }
            FetchStatus::Loading => {
                rsx! {
                    Spinner {}
                }
            }
            FetchStatus::Finished(()) => {
                rsx! {

                    match &*peptide.read_unchecked() {
                        Some(Ok(None)) => {
                            rsx! { "" }
                        }
                        Some(Ok(Some(peptide))) => {
                            rsx! {
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
                                                RoundedMass { mass: peptide.get_mass() }
                                            }
                                            td { class: "text-break",
                                                Link {
                                                    to: Routes::Peptide {
                                                        peptide_sequence: peptide.get_sequence().to_owned(),
                                                    },
                                                    "{peptide.get_sequence()}"
                                                }
                                            }
                                        }
                                    }
                                }
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
            FetchStatus::Error(err) => {
                rsx! {
                    div { "Error fetching proteins: {err}" }
                }
            }
        }
    }
}
