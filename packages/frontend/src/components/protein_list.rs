use std::{collections::HashMap, sync::Arc};

// 3rd party imports
use dioxus::prelude::*;
use dioxus_router::components::Link;
use macpepdb_web_common::responses::protein::ProteinResponse;

// internal imports
use crate::{errors::general_error::GeneralError, routes::Routes};

/// Columns the protein list can be sorted by
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProteinSortColumn {
    Accession,
    Genes,
    Taxonomy,
}

/// Sort direction
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

/// Active sorting of a protein list
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProteinSort {
    pub column: ProteinSortColumn,
    pub direction: SortDirection,
}

impl ProteinSort {
    /// Returns the sorting after a click on `column`: toggles the direction if the column is
    /// already active, otherwise sorts ascending by the new column.
    ///
    pub fn toggled(current: Option<ProteinSort>, column: ProteinSortColumn) -> ProteinSort {
        match current {
            Some(sort) if sort.column == column => ProteinSort {
                column,
                direction: match sort.direction {
                    SortDirection::Ascending => SortDirection::Descending,
                    SortDirection::Descending => SortDirection::Ascending,
                },
            },
            _ => ProteinSort {
                column,
                direction: SortDirection::Ascending,
            },
        }
    }

    /// Sorts the proteins in place. Taxonomies are compared by name (case-insensitive), falling
    /// back to the ID for taxonomies without a resolved name.
    ///
    pub fn sort(
        &self,
        proteins: &mut [ProteinResponse<String>],
        taxonomy_names: &HashMap<i32, String>,
    ) {
        proteins.sort_by(|a, b| {
            let ordering = match self.column {
                ProteinSortColumn::Accession => a.accession.cmp(&b.accession),
                ProteinSortColumn::Genes => a
                    .genes
                    .join(", ")
                    .to_lowercase()
                    .cmp(&b.genes.join(", ").to_lowercase()),
                ProteinSortColumn::Taxonomy => {
                    let name = |id: &i32| taxonomy_names.get(id).map(|name| name.to_lowercase());
                    name(&a.taxonomy_id)
                        .cmp(&name(&b.taxonomy_id))
                        .then(a.taxonomy_id.cmp(&b.taxonomy_id))
                }
            };
            match self.direction {
                SortDirection::Ascending => ordering,
                SortDirection::Descending => ordering.reverse(),
            }
        });
    }
}

/// Properties for protein list
///
#[derive(Clone, PartialEq, Props)]
pub struct ProteinListProps {
    /// List of proteins to render
    pub proteins: Arc<Vec<ProteinResponse<String>>>,
    /// Taxonomy ID/name map
    pub taxonomy_names: Resource<Result<HashMap<i32, String>, GeneralError>>,
    /// Currently active sorting, only used to display the sort indicator
    #[props(default)]
    pub sort: Option<ProteinSort>,
    /// Called with the clicked column. If not set, the headers are not sortable.
    /// The list does not sort itself; the caller has to provide the proteins in the desired order.
    #[props(default)]
    pub on_sort: Option<EventHandler<ProteinSortColumn>>,
}

/// Renders a table header, clickable if sorting is enabled
///
fn sortable_header(
    label: &'static str,
    column: ProteinSortColumn,
    sort: Option<ProteinSort>,
    on_sort: Option<EventHandler<ProteinSortColumn>>,
) -> Element {
    let Some(on_sort) = on_sort else {
        return rsx! {
            th { "{label}" }
        };
    };
    let icon = match sort {
        Some(sort) if sort.column == column => match sort.direction {
            SortDirection::Ascending => "fa-sort-up",
            SortDirection::Descending => "fa-sort-down",
        },
        _ => "fa-sort",
    };
    rsx! {
        th {
            style: "cursor: pointer; user-select: none",
            onclick: move |_| on_sort.call(column),
            "{label} "
            i { class: "fas {icon}" }
        }
    }
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
                        {sortable_header("Accession", ProteinSortColumn::Accession, props.sort, props.on_sort)}
                        {sortable_header("Genes", ProteinSortColumn::Genes, props.sort, props.on_sort)}
                        {sortable_header("Taxonomy", ProteinSortColumn::Taxonomy, props.sort, props.on_sort)}
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
                                if protein.is_reviewed { " (SwissProt)" } else { " (TrEMBL)" }
                            }
                        }
                    }
                }
            }
        }
    }
}
