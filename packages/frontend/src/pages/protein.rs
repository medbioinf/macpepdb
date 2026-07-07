use dioxus::prelude::*;
use dioxus_router::components::Link;
use macpepdb_web_common::responses::peptide::PeptideResponse;

use crate::api_client::Client;
use crate::components::rounded_mass::RoundedMass;
use crate::components::sequence_block::SequenceBlock;
use crate::components::spinner::Spinner;
use crate::configuration::Configuration as AppConfiguration;
use crate::errors::general_error::GeneralError;
use crate::routes::Routes;
use crate::tracking::track_page_visit;
use macpepdb_web_common::responses::protein::ProteinResponse;

/// Properties for protein page
#[derive(Clone, PartialEq, Props)]
pub struct ProteinProps {
    /// Protein ID to fetch
    pub protein_id: String,
}

/// Protein page
// TODO: `ProteinResponse` (from `macpepdb_web_common`) does not carry `secondary_accessions`,
// `entry_name`, `name`, `proteome_id`, or `updated_at` (all present on the old hand-rolled
// `entities::protein::Protein<T>`), so the rows displaying them have been dropped from this page.
pub fn Protein(props: ProteinProps) -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();

    let protein_id = use_signal(|| props.protein_id.to_owned());

    let protein: Resource<Result<ProteinResponse<PeptideResponse>, GeneralError>> =
        use_resource(move || async move {
            let app_config = app_config.read();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Err(GeneralError::ConfigurationNotLoaded),
            };

            let client = Client::new(macpepdb_base_url)?;

            Ok(client.get_protein(protein_id.read().as_str()).await?)
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
                Some(Ok(protein)) => rsx! {
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
                                td { "{protein.accession}" }
                            }
                            tr {
                                td { "Genes" }
                                td {
                                    ul {
                                        for gene in protein.genes.iter() {
                                            li { "{gene}" }
                                        }
                                    }
                                }
                            }
                            tr {
                                td { "Taxonomy ID" }
                                td { "{protein.taxonomy_id}" }
                            }
                            tr {
                                td { "Is reviewed" }
                                td {
                                    i { class: if protein.is_reviewed { "fas fa-check" } else { "fas fa-times" } }
                                }
                            }
                            tr {
                                td { "Sequence" }
                                td {
                                    SequenceBlock { sequence: protein.sequence.clone() }
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
                            for peptide in protein.peptides.iter() {
                                tr {
                                    td {
                                        RoundedMass { mass: peptide.mass }
                                    }
                                    td { class: "text-break",
                                        Link {
                                            to: Routes::Peptide {
                                                peptide_sequence: peptide.sequence.clone(),
                                            },
                                            "{peptide.sequence}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                Some(Err(err)) => rsx! {
                    div { class: "alert alert-danger", "Error getting protein: {err}" }
                },
                None => rsx! {
                    if protein.pending() {
                        Spinner {}
                    }
                },
            }
        }
    }
}
