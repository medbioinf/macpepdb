use std::{collections::HashMap, sync::Arc};

// 3rd party imports
use dioxus::prelude::*;
use dioxus_router::components::Link;
use macpepdb_web_common::responses::protein::ProteinResponse;

// internal imports
use crate::{errors::general_error::GeneralError, routes::Routes};

/// Properties for protein list
///
#[derive(Clone, PartialEq, Props)]
pub struct ProteinListProps {
    /// List of proteins to render
    pub proteins: Arc<Vec<ProteinResponse<String>>>,
    /// Taxonomy ID/name map
    pub taxonomy_names: Resource<Result<HashMap<i32, String>, GeneralError>>,
}

/// Renders a list of proteins with most common attributes: accession, genes, is reviewed.
///
// TODO: `ProteinResponse<String>` (from `macpepdb_web_common`) does not carry `entry_name` or
// `name` (both present on the old hand-rolled `entities::protein::Protein<T>`), so those columns
// have been dropped from this table.
pub fn ProteinList(props: ProteinListProps) -> Element {
    if props.proteins.is_empty() {
        return rsx! {
            div { "No proteins" }
        };
    }

    let reviewed_proteins = props
        .proteins
        .iter()
        .filter(|protein| protein.is_reviewed)
        .collect::<Vec<&ProteinResponse<String>>>();

    let unreviewed_proteins = props
        .proteins
        .iter()
        .filter(|protein| !protein.is_reviewed)
        .collect::<Vec<&ProteinResponse<String>>>();

    let protein_lists = vec![
        ("Reviewed Proteins", reviewed_proteins),
        ("Unreviewed Proteins", unreviewed_proteins),
    ];

    rsx! {
        for (title , proteins) in protein_lists {
            h3 { "{title}" }
            table { class: "table table-striped table-hover",
                thead {
                    tr {
                        th { "Accession" }
                        th { "Genes" }
                        th { "Taxonomy" }
                        th { "Is reviewed" }
                    }
                }
                tbody {
                    for protein in proteins {
                        tr {
                            td {
                                Link {
                                    to: Routes::Protein {
                                        protein_id: protein.accession.clone(),
                                    },
                                    "{protein.accession}"
                                }
                            }
                            td { "{protein.genes.join(\", \")}" }
                            td {
                                match &*props.taxonomy_names.read_unchecked() {
                                    Some(Ok(names)) => match names.get(&protein.taxonomy_id) {
                                        Some(name) => rsx! { "{name} (ID: {protein.taxonomy_id})" },
                                        None => rsx! { "{protein.taxonomy_id}" },
                                    },
                                    _ => rsx! { "{protein.taxonomy_id}" },
                                }
                            }
                            td {
                                i { class: if protein.is_reviewed { "fas fa-check" } else { "fas fa-times" } }
                            }
                        }
                    }
                }
            }
        }
    }
}
