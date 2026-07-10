use dioxus::prelude::*;

use crate::api_client::Client;
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
                    table { class: "table table-striped",
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
                                            li { "{id}" }
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
                                            li { "{id}" }
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
                                td { "Protein IDs" }
                                td {
                                    // TODO: `PeptideResponse.protein_ids` only carries the raw numeric
                                    // protein ids (no accessions), so we can no longer render a
                                    // `ProteinList` inline here like the old nested `Peptide<Protein<..>>`
                                    // entity allowed. Showing the raw ids for now; a nicer version would
                                    // resolve each id to an accession (there is currently no
                                    // `GET /api/proteins/by-id/{id}` endpoint, only lookup by accession)
                                    // and fetch full `ProteinResponse`s on demand, e.g. on click.
                                    ul {
                                        for id in peptide.protein_ids.iter() {
                                            li { "{id}" }
                                        }
                                    }
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
