use anyhow::Result;
use dioxus::prelude::*;
use dioxus_router::components::Link;

use crate::components::rounded_mass::RoundedMass;
use crate::components::sequence_block::SequenceBlock;
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::peptide::Peptide as MaCPepDBPeptide;
use crate::entities::protein::Protein as MaCPepDBProtein;
use crate::routes::Routes;
use crate::tracking::track_page_visit;

/// As peptides contain their protein of origin and proteins contain their peptides, MaCPepDB
/// stops the recursion on third level by only adding the protein accession to the peptides
/// instead of the whole protein.
type ProteinEntity = MaCPepDBProtein<MaCPepDBPeptide<String>>;

/// Fetch protein from MaCPepDB
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
/// * `protein_id` - Protein accession or gene name
///
pub async fn get_protein(macpepdb_base_url: &str, protein_id: &str) -> Result<ProteinEntity> {
    let url = format!("{}/api/proteins/{}", macpepdb_base_url, protein_id);
    Ok(reqwest::get(&url).await?.json().await?)
}

/// Properties for protein page
#[derive(Clone, PartialEq, Props)]
pub struct ProteinProps {
    /// Protein ID to fetch
    pub protein_id: String,
}

/// Protein page
pub fn Protein(props: ProteinProps) -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();

    let protein_id = use_signal(|| props.protein_id.to_owned());

    let protein: Resource<Result<Option<ProteinEntity>>> = use_resource(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Ok(None),
        };
        Ok(Some(
            get_protein(macpepdb_base_url, protein_id.read().as_str()).await?,
        ))
    });

    let uniprot_link = use_signal(|| format!("https://www.uniprot.org/uniprot/{}", protein_id));

    use_future(move || async move {
        track_page_visit(vec![(
            protein_id.to_string(),
            ":protein_accession".to_string(),
        )])
        .await
    });

    rsx! {
        div {
            h2 { "Protein: {props.protein_id}" }
            match &*protein.read_unchecked() {
                Some(Ok(Some(protein))) => rsx! {
                    table { class: "table table-striped",
                        thead {
                            tr {
                                th { "Attribute" }
                                th { "Value" }
                            }
                        }
                        tbody {
                            tr {
                                td { "Accession" }
                                td { "{protein.get_accession()}" }
                            }
                            tr {
                                td { "Secondary accession" }
                                td {
                                    if !protein.get_secondary_accessions().is_empty() {
                                        ul {
                                            for sec_accession in protein.get_secondary_accessions() {
                                                li { "{sec_accession}" }
                                            }
                                        }
                                    } else {
                                        "None"
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
                                td {
                                    i { class: if protein.get_is_reviewed() { "fas fa-check" } else { "fas fa-times" } }
                                }
                            }
                            tr {
                                td { "Last updated at" }
                                td { "{protein.get_human_readable_updated_at()}" }
                            }
                            tr {
                                td { "Sequence" }
                                td {
                                    SequenceBlock { sequence: protein.get_sequence().clone() }
                                }
                            }
                            tr {
                                td { "UniProt" }
                                td {
                                    a { href: uniprot_link, target: "_blank",
                                        "{uniprot_link}"
                                        i { class: "fas fa-external-link-alt ms-2" }
                                    }
                                }
                            }
                        }
                    }
                    h3 { "Peptides" }
                    table { class: "table table-striped table-hover table-sm table-responsive",
                        thead {
                            tr {
                                th { "Mass (Da)" }
                                th { "Sequence" }
                            }
                        }
                        tbody {
                            for peptide in protein.get_peptides() {
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
                },
                Some(Err(err)) => rsx! {
                    div { "Error loading the protein {err}" }
                },
                None | Some(Ok(None)) => rsx! {
                    div { "Loading ..." }
                },
            }
        }
    }
}
