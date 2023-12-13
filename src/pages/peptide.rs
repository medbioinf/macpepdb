// std imports
use std::collections::HashMap;

// 3rd party imports
use anyhow::Result;
use dioxus::prelude::*;
use reqwest;

use crate::components::peptide::amino_acid_composition_header_cell::AminoAcidCompositionHeaderCell;
// internal imports
use crate::components::protein_list::ProteinList;
use crate::components::rounded_mass::RoundedMass;
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::amino_acid::AminoAcid;
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

/// Fetches amino acids and returns them as map of code => amino acid
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
/// * `peptide_id` - peptide accession or gene name
///
pub async fn get_amino_acid_map(macpepdb_base_url: String) -> Result<HashMap<char, AminoAcid>> {
    let url = format!("{}/api/chemistry/amino_acids", macpepdb_base_url);
    let amino_acids: Vec<AminoAcid> = reqwest::get(&url).await?.json().await?;
    Ok(amino_acids
        .into_iter()
        .map(|aa| (aa.get_code().to_owned(), aa))
        .collect())
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
    let amino_acid_map = use_future(cx, (), |_| {
        get_amino_acid_map(app_config.clone().read().get_macpepdb_base_url().to_owned())
    });

    render! {
        div {
            h2 { "Peptide: {cx.props.peptide_sequence}" }
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
                            "data-partition": "{peptide.get_partition()}",
                            td { "Theoretical mass (Da)" }
                            td {
                                RoundedMass {
                                    mass: peptide.get_mass(),
                                }
                            }
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
                    h3 { "Amino acid composition" }
                    match amino_acid_map.clone().value() {
                        Some(Ok(amino_acid_map)) => render! {
                            table {
                                tr {
                                    for (idx, _) in peptide.get_aa_counts().iter().enumerate() {
                                        AminoAcidCompositionHeaderCell{
                                            index: idx,
                                            amino_acid_map: amino_acid_map.clone(),
                                        }
                                    }
                                }
                                tr {
                                    for count in peptide.get_aa_counts() {
                                        td { "{count}" }
                                    }
                                }
                            }
                        },
                        Some(Err(e)) => render! {
                            div { "Error loading the amino acid map {e}" }
                        },
                        None => render! {
                            div { "Loading ..." }
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
