// 3rd party imports
use dioxus::prelude::*;
use dioxus_router::components::Link;

// internal imports
use crate::entities::protein::Protein as MaCPepDBProtein;
use crate::routes::Routes;

/// Proteins with peptide sequences
type ProteinEntity = MaCPepDBProtein<String>;

/// Properties for protein list
///
#[derive(Props)]
pub struct ProteinListProps<'a> {
    /// List of proteins to render
    pub proteins: Vec<&'a ProteinEntity>,
}

/// Renders a list of proteins with most common attributes: accession, entry name, name, genes.
///
pub fn ProteinList<'a>(cx: Scope<'a, ProteinListProps<'a>>) -> Element {
    if cx.props.proteins.is_empty() {
        return render! {
            div { "No proteins" }
        };
    }
    render! {
        table {
            tr {
                th { "Accession" }
                th { "Entry name" }
                th { "Name" }
                th { "Genes" }
                th { "Is reviewed" }
            }
            for protein in cx.props.proteins.iter() {
                tr {
                    td {
                        Link {
                            to: Routes::Protein{protein_id: protein.get_accession().to_owned()},
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
