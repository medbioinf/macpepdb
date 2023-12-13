// 3rd party imports
use anyhow::Result;
use dioxus::prelude::*;
use reqwest;

// internal imports
use crate::components::protein_list::ProteinList;
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::peptide::Peptide as MaCPepDBPeptide;
use crate::entities::protein::Protein as MaCPepDBProteins;

/// As proteins contain their peptides and peptides contain their protein of origin, MaCPepDB
/// stops the recursion on third level by only adding the peptide sequences to the protein
/// instead of the whole peptide.
type PeptideEntity = MaCPepDBPeptide<MaCPepDBProteins<String>>;

/// Fetches peptide from MaCPepDB
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
/// * `peptide_id` - peptide accession or gene name
///
pub async fn get_peptide(
    macpepdb_base_url: String,
    peptide_sequence: String,
) -> Result<PeptideEntity> {
    let url = format!("{}/api/peptides/{}", macpepdb_base_url, peptide_sequence);
    Ok(reqwest::get(&url).await?.json().await?)
}

/// Properties for peptide page
#[derive(PartialEq, Props)]
pub struct PeptideProps {
    /// Peptide sequence to fetch
    pub peptide_sequence: String,
}

/// Page for rendering a single peptide
///
pub fn Peptide(cx: Scope<PeptideProps>) -> Element {
    let app_config = use_shared_state::<AppConfiguration>(cx).unwrap();
    let peptide = use_future(cx, (), |_| {
        get_peptide(
            app_config.clone().read().get_macpepdb_base_url().to_owned(),
            cx.props.peptide_sequence.clone(),
        )
    });

    render! {
        div {
            h2 { "peptide: {cx.props.peptide_sequence}" }
            match peptide.value() {
                Some(Ok(peptide)) => render! {
                    table {
                        tr {
                            th { "Attributes" }
                            th { "Value" }
                        }
                        tr {
                            td { "Sequence" }
                            td { "{peptide.get_sequence().to_owned()}" }
                        }
                        tr {
                            "dataPartition": "{peptide.get_partition()}",
                            td { "Theoretical mass (Da)" }
                            td { "{peptide.get_mass()}" }
                        }
                        tr {
                            td { "length" }
                            td { "{peptide.get_sequence().len()}" }
                        }
                        tr {
                            td { "# missed cleavages" }
                            td { "{peptide.get_missed_cleavages()}" }
                        }
                        tr {
                            td { "Proteome IDs" }
                            td {
                                ul {
                                    for id in peptide.get_proteome_ids().iter() {
                                        li { "{id}" }
                                    }
                                }
                            }
                        }
                        tr {
                            td { "Taxonomy IDs" }
                            td {
                                ul {
                                    for id in peptide.get_taxonomy_ids().iter() {
                                        li { "{id}" }
                                    }
                                }
                            }
                        }
                        td { "Unique taxonomy IDs (Taxonomies where this peptide is only present in one protein)" }
                        td {
                            ul {
                                for id in peptide.get_unique_taxonomy_ids().iter() {
                                    li { "{id}" }
                                }
                            }
                        }
                        tr {
                            td { "SwissProt/TrEMBL " }
                            td { "{peptide.get_is_swiss_prot()} / {peptide.get_is_trembl()}" }
                        }
                    }
                    h3 { "Reviewed proteins" }
                    ProteinList {
                        proteins: peptide.get_reviewed_proteins(),
                    }

                    h3 { "Unreviewed proteins" }
                    ProteinList {
                        proteins: peptide.get_unreviewed_proteins(),
                    }
                },
                Some(Err(e)) => render! {
                    div { "Error loading the peptide {e}" }
                },
                None => render! {
                    div { "Loading ..." }
                }
            }
        }
    }
}
