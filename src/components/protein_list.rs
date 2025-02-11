use std::rc::Rc;

// 3rd party imports
use dioxus::prelude::*;
use dioxus_router::components::Link;

// internal imports
use crate::entities::protein::Protein as MaCPepDBProtein;
use crate::routes::Routes;

/// Properties for protein list
///
#[derive(Clone, PartialEq, Props)]
pub struct ProteinListProps<T>
where
    T: 'static + PartialEq,
{
    /// List of proteins to render
    pub proteins: Rc<Vec<MaCPepDBProtein<T>>>,
}

/// Renders a list of proteins with most common attributes: accession, entry name, name, genes.
///
pub fn ProteinList<T>(props: ProteinListProps<T>) -> Element
where
    T: 'static + PartialEq,
{
    if props.proteins.is_empty() {
        return rsx! {
            div { "No proteins" }
        };
    }
    rsx! {
        table { class: "table table-striped table-hover",
            thead {
                tr {
                    th { "Accession" }
                    th { "Entry name" }
                    th { "Name" }
                    th { "Genes" }
                    th { "Is reviewed" }
                }
            }
            tbody {
                for protein in props.proteins.iter() {
                    tr {
                        td {
                            Link {
                                to: Routes::Protein {
                                    protein_id: protein.get_accession().to_owned(),
                                },
                                "{protein.get_accession()}"
                            }
                        }
                        td { "{protein.get_entry_name()}" }
                        td { "{protein.get_name()}" }
                        td { "{protein.get_genes().join(\", \")}" }
                        td { "{protein.get_is_reviewed()}" }
                    }
                }
            }
        }
    }
}
