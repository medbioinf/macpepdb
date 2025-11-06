use std::collections::HashMap;
use std::rc::Rc;

use anyhow::Result;
use dioxus::prelude::*;

use crate::components::peptide::amino_acid_composition_header_cell::AminoAcidCompositionHeaderCell;
use crate::components::protein_list::ProteinList;
use crate::components::rounded_mass::RoundedMass;
use crate::components::sequence_block::SequenceBlock;
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::amino_acid::AminoAcid;
use crate::entities::peptide::Peptide as MaCPepDBPeptide;
use crate::entities::protein::Protein as MaCPepDBProteins;
use crate::tracking::track_page_visit;

/// As proteins contain their peptides and peptides contain their protein of origin, MaCPepDB
/// stops the recursion on third level by only adding the Protein sequences to the protein
/// instead of the whole peptide.
type ProteinEntity = MaCPepDBProteins<String>;
type PeptideEntity = MaCPepDBPeptide<ProteinEntity>;

/// Fetches peptide from MaCPepDB
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
/// * `peptide_id` - peptide accession or gene name
///
pub async fn get_peptide(macpepdb_base_url: &str, peptide_sequence: &str) -> Result<PeptideEntity> {
    let url = format!("{macpepdb_base_url}/api/peptides/{peptide_sequence}");
    Ok(reqwest::get(&url).await?.json().await?)
}

/// Fetches amino acids and returns them as map of code => amino acid
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
/// * `peptide_id` - peptide accession or gene name
///
pub async fn get_amino_acid_map(macpepdb_base_url: &str) -> Result<HashMap<char, AminoAcid>> {
    let url = format!("{macpepdb_base_url}/api/chemistry/amino_acids");
    let amino_acids: Vec<AminoAcid> = reqwest::get(&url).await?.json().await?;
    Ok(amino_acids
        .into_iter()
        .map(|aa| (aa.get_code().to_owned(), aa))
        .collect())
}

/// Properties for peptide page
#[derive(Clone, PartialEq, Props)]
pub struct PeptideProps {
    /// Peptide sequence to fetch
    pub peptide_sequence: String,
}

/// Page for rendering a single peptide
///
pub fn Peptide(props: PeptideProps) -> Element {
    let app_config = use_context::<Resource<AppConfiguration>>();
    let peptide_sequence = use_signal(|| props.peptide_sequence.clone());

    let peptide: Resource<Result<Option<PeptideEntity>>> = use_resource(move || async move {
        let app_config = app_config.read_unchecked();
        let macpepdb_base_url = match app_config.as_ref() {
            Some(config) => config.get_macpepdb_base_url(),
            None => return Ok(None),
        };

        Ok(Some(
            get_peptide(macpepdb_base_url, peptide_sequence.read().as_str()).await?,
        ))
    });

    let amino_acid_map: Resource<Result<Option<Rc<HashMap<char, AminoAcid>>>>> =
        use_resource(move || async move {
            let app_config = app_config.read_unchecked();
            let macpepdb_base_url = match app_config.as_ref() {
                Some(config) => config.get_macpepdb_base_url(),
                None => return Ok(None),
            };

            let map = get_amino_acid_map(macpepdb_base_url).await?;

            Ok(Some(Rc::new(map)))
        });

    let _ = use_resource(move || async move {
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
                Some(Ok(Some(peptide))) => rsx! {
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
                                    SequenceBlock { sequence: peptide.get_sequence().to_owned() }
                                }
                            }
                            tr { "data-partition": "{peptide.get_partition()}",
                                td { "Theoretical mass (Da)" }
                                td {
                                    RoundedMass { mass: peptide.get_mass() }
                                }
                            }
                            tr {
                                td { "Length" }
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
                            tr {
                                td {
                                    span { class: "d-block", "Unique taxonomy IDs" }
                                    small { class: "d-block",
                                        "(Taxonomies where this peptide is only present in one protein)"
                                    }
                                }
                                td {
                                    ul {
                                        for id in peptide.get_unique_taxonomy_ids().iter() {
                                            li { "{id}" }
                                        }
                                    }
                                }
                            }
                            tr {
                                td { "SwissProt / TrEMBL " }
                                td {
                                    i { class: if peptide.get_is_swiss_prot() { "fas fa-check" } else { "fas fa-times" } }
                                    " / "
                                    i { class: if peptide.get_is_trembl() { "fas fa-check" } else { "fas fa-times" } }
                                }
                            }
                        }
                    }
                    h3 { "Amino acid composition" }
                    match &*amino_acid_map.read_unchecked() {
                        Some(Ok(Some(amino_acid_map))) => rsx! {
                            table { class: "table table-sm",
                                thead {
                                    tr {
                                        for (idx , _) in peptide.get_aa_counts().iter().enumerate() {
                                            AminoAcidCompositionHeaderCell { index: idx, amino_acid_map: amino_acid_map.clone() }
                                        }
                                    }
                                }
                                tbody {
                                    tr {
                                        for count in peptide.get_aa_counts() {
                                            td { "{count}" }
                                        }
                                    }
                                }
                            }
                        },
                        Some(Err(err)) => rsx! {
                            div { "Error loading the amino acid map {err}" }
                        },
                        Some(Ok(None)) | None => rsx! {
                            div { "Loading ..." }
                        },
                    }
                    ProteinList { proteins: peptide.get_proteins() }
                },
                Some(Err(err)) => rsx! {
                    div { "Error loading the peptide {err}" }
                },
                Some(Ok(None)) | None => rsx! {
                    div { "Loading ..." }
                },
            }
        }
    }
}
