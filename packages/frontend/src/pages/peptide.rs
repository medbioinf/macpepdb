use std::collections::HashMap;
use std::sync::Arc;

use dioxus::prelude::*;

use crate::api_client::Client;
use crate::components::protein_list::ProteinList;
use crate::components::rounded_mass::RoundedMass;
use crate::components::sequence_block::SequenceBlock;
use crate::components::spinner::Spinner;
use crate::configuration::Configuration as AppConfiguration;
use crate::errors::general_error::GeneralError;
use crate::tracking::track_page_visit;
use macpepdb_web_common::responses::peptide::PeptideResponse;

/// Properties for peptide page
#[derive(Clone, PartialEq, Props)]
pub struct PeptideProps {
    /// Peptide sequence to fetch
    pub peptide_sequence: String,
}

/// Page for rendering a single peptide
///
// TODO: `PeptideResponse` (from `macpepdb_web_common`) no longer carries `missed_cleavages`,
// `proteome_ids`, or per-amino-acid `aa_counts` (all present on the old hand-rolled
// `entities::peptide::Peptide<T>`), so the "# missed cleavages", "Proteome IDs", and "Amino acid
// composition" rows/section that used to render them have been dropped. Amino acid composition
// could be recomputed client-side from `sequence` if the feature is wanted back.
pub fn Peptide(props: PeptideProps) -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();
    let peptide_sequence = use_signal(|| props.peptide_sequence.clone());

    let peptide: Resource<Result<PeptideResponse, GeneralError>> =
        use_resource(move || async move {
            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Err(GeneralError::ConfigurationNotLoaded),
            };

            let client = Client::new(macpepdb_base_url)?;

            Ok(client.get_peptide(peptide_sequence.read().as_str()).await?)
        });

    let hydrophobicity = use_resource(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Err(GeneralError::ConfigurationNotLoaded),
        };

        let client = Client::new(macpepdb_base_url)?;

        Ok(client
            .hydrophobicity_korkhin(peptide_sequence.read().as_str())
            .await?)
    });

    let taxonomy_names: Resource<Result<HashMap<i32, String>, GeneralError>> =
        use_resource(move || async move {
            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Err(GeneralError::ConfigurationNotLoaded),
            };

            let ids: Vec<i32> = match &*peptide.read_unchecked() {
                Some(Ok(p)) => {
                    let mut ids: Vec<i32> = p
                        .unique_taxonomy_ids
                        .iter()
                        .chain(p.non_unique_taxonomy_ids.iter())
                        .copied()
                        .collect();
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

    use_future(move || async move {
        track_page_visit(vec![(
            peptide_sequence.to_string(),
            ":peptide_sequence".to_string(),
        )])
        .await
    });

    rsx! {
        div {
            h2 { "Peptide: {peptide_sequence}" }
            match &*peptide.read_unchecked() {
                Some(Ok(peptide)) => rsx! {
                    table { class: "table table-striped mb-3",
                        thead {
                            tr {
                                th { "Attributes" }
                                th { "Value" }
                            }
                        }
                        tbody {
                            tr {
                                td { "Sequence" }
                                td {
                                    SequenceBlock { sequence: peptide.sequence.clone() }
                                }
                            }
                            tr { "data-partition": peptide.partition.map(|p| p.to_string()).unwrap_or_default(),
                                td { "Theoretical mass (Da)" }
                                td {
                                    RoundedMass { mass: peptide.mass }
                                }
                            }
                            tr {
                                td { "Length" }
                                td { "{peptide.sequence.len()}" }
                            }
                            tr {
                                td { "Taxonomy IDs" }
                                td {
                                    ul {
                                        for id in peptide.non_unique_taxonomy_ids.iter() {
                                            li {
                                                match &*taxonomy_names.read_unchecked() {
                                                    Some(Ok(names)) => match names.get(id) {
                                                        Some(name) => rsx! { "{name} (ID: {id})" },
                                                        None => rsx! { "{id}" },
                                                    },
                                                    _ => rsx! { "{id}" },
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            tr {
                                td {
                                    span { class: "d-block", "Unique taxonomy IDs" }
                                    small { class: "d-block",
                                        "(Taxonomies where this peptide is only present in one protein)"
                                    }
                                }
                                td {
                                    ul {
                                        for id in peptide.unique_taxonomy_ids.iter() {
                                            li {
                                                match &*taxonomy_names.read_unchecked() {
                                                    Some(Ok(names)) => match names.get(id) {
                                                        Some(name) => rsx! { "{name} (ID: {id})" },
                                                        None => rsx! { "{id}" },
                                                    },
                                                    _ => rsx! { "{id}" },
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            tr {
                                td { "SwissProt / TrEMBL " }
                                td {
                                    i { class: if peptide.is_swiss_prot { "fas fa-check" } else { "fas fa-times" } }
                                    " / "
                                    i { class: if peptide.is_trembl { "fas fa-check" } else { "fas fa-times" } }
                                }
                            }
                            tr {
                                td { "Hydrophobicity (Krokhin et al.)" }
                                td {
                                    match &*hydrophobicity.read_unchecked() {
                                        Some(Ok(hydrophobicity)) => rsx! {
                                            "{hydrophobicity:.3}"
                                        },
                                        Some(Err(err)) => rsx! {
                                            div { class: "alert alert-danger", "Error getting hydrophobicity: {err}" }
                                        },
                                        None => rsx! {
                                            if hydrophobicity.pending() {
                                                Spinner {}
                                            }
                                        },
                                    }
                                }
                            }

                        }
                    }
                    div {
                        class: "mb-3",
                        ProteinList {
                            proteins: peptide.proteins.clone().unwrap_or(Arc::new(Vec::new())),
                            taxonomy_names
                        }
                    }
                },
                Some(Err(err)) => rsx! {
                    div { class: "alert alert-danger", "Error getting proteins: {err}" }
                },
                None => rsx! {
                    if peptide.pending() {
                        Spinner {}
                    }
                },
            }
        }
    }
}
