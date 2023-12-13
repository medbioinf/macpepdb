// 3rd party imports
use anyhow::Result;
use dioxus::prelude::*;
use dioxus_router::components::Link;
use reqwest;

// internal imports
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::peptide::Peptide as MaCPepDBPeptide;
use crate::entities::protein::Protein as MaCPepDBProtein;
use crate::routes::Routes;

/// As peptides contain their protein of origin and proteins contain their peptides, MaCPepDB
/// stops the recursion on third level by only adding the protein accession to the peptides
/// instead of the whole protein.
type PeptideEntity = MaCPepDBProtein<MaCPepDBPeptide<String>>;

/// Fetch protein from MaCPepDB
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
/// * `protein_id` - Protein accession or gene name
///
pub async fn get_protein(macpepdb_base_url: String, protein_id: String) -> Result<PeptideEntity> {
    let url = format!("{}/api/proteins/{}", macpepdb_base_url, protein_id);
    Ok(reqwest::get(&url).await?.json().await?)
}

/// Properties for protein page
#[derive(PartialEq, Props)]
pub struct ProteinProps {
    /// Protein ID to fetch
    pub protein_id: String,
}

/// Protein page
pub fn Protein(cx: Scope<ProteinProps>) -> Element {
    let app_config = use_shared_state::<AppConfiguration>(cx).unwrap();
    let protein = use_future(cx, (), |_| {
        get_protein(
            app_config.clone().read().get_macpepdb_base_url().to_owned(),
            cx.props.protein_id.clone(),
        )
    });

    render! {
        div {
            h2 { "Protein: {cx.props.protein_id}" }
            match protein.value() {
                Some(Ok(protein)) => render! {
                    table {
                        tr {
                            td { "Accession" }
                            td { "{protein.get_accession()}" }
                        }
                        tr {
                            td { "Secondary accession" }
                            td {
                                if !protein.get_secondary_accessions().is_empty() {
                                    render! {
                                        ul {
                                            for sec_accession in protein.get_secondary_accessions() {
                                                li { "{sec_accession}" }
                                            }
                                        }
                                    }
                                } else {
                                    render! {
                                        "None"
                                    }
                                }
                            }
                        }
                        tr {
                            td { "Entry name" }
                            td { "{protein.get_entry_name()}" }
                        }
                        tr {
                            td { "Name" }
                            td { "{protein.get_name()}" }
                        }
                        tr {
                            td { "Genes" }
                            td {
                                ul {
                                    for gene in protein.get_genes() {
                                        li { "{gene}" }
                                    }
                                }
                            }
                        }
                        tr {
                            td { "Taxonomy ID" }
                            td { "{protein.get_taxonomy_id()}" }
                        }
                        tr {
                            td { "Proteome ID" }
                            td { "{protein.get_proteome_id()}" }
                        }
                        tr {
                            td { "Is reviewed" }
                            td { "{protein.get_is_reviewed()}" }
                        }
                    }
                    table {
                        for peptide in protein.get_peptides() {
                            tr {
                                td { "{peptide.get_mass()}" }
                                td {
                                    Link {
                                        to: Routes::Peptide{
                                            peptide_sequence: peptide.get_sequence().to_owned()
                                        },
                                        "{peptide.get_sequence()}"
                                    }
                                }
                            }
                        }
                    }
                },
                Some(Err(e)) => render! {
                    div { "Error loading the protein {e}" }
                },
                None => render! {
                    div { "Loading ..." }
                }
            }
        }
    }
}
