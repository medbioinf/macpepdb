// std imports
use std::collections::HashMap;
use std::rc::Rc;

// 3rd party imports
use anyhow::Result;
use dioxus::prelude::*;

// internal imports
use crate::components::peptide::amino_acid_composition_header_cell::AminoAcidCompositionHeaderCell;
use crate::components::protein_list::ProteinList;
use crate::components::rounded_mass::RoundedMass;
use crate::configuration::Configuration as AppConfiguration;
use crate::entities::amino_acid::AminoAcid;
use crate::entities::peptide::Peptide as MaCPepDBPeptide;
use crate::entities::protein::Protein as MaCPepDBProteins;

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
pub async fn get_peptide(
    macpepdb_base_url: Signal<String>,
    peptide_sequence: Signal<String>,
) -> Result<PeptideEntity> {
    let url = format!("{macpepdb_base_url}/api/peptides/{peptide_sequence}");
    Ok(reqwest::get(&url).await?.json().await?)
}

/// Fetches amino acids and returns them as map of code => amino acid
///
/// # Arguments
/// * `macpepdb_base_url` - Base URL of MaCPepDB
/// * `peptide_id` - peptide accession or gene name
///
pub async fn get_amino_acid_map(
    macpepdb_base_url: Signal<String>,
) -> Result<Rc<HashMap<char, AminoAcid>>> {
    let url = format!("{macpepdb_base_url}/api/chemistry/amino_acids");
    let amino_acids: Vec<AminoAcid> = reqwest::get(&url).await?.json().await?;
    Ok(Rc::new(
        amino_acids
            .into_iter()
            .map(|aa| (aa.get_code().to_owned(), aa))
            .collect(),
    ))
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
    let app_config = use_context::<AppConfiguration>();
    let macpepdb_base_url = use_signal(|| app_config.get_macpepdb_base_url().to_owned());
    let peptide_sequence = use_signal(|| props.peptide_sequence.clone());
    let mut review_proteins: Signal<Option<Rc<Vec<ProteinEntity>>>> = use_signal(|| None);
    let mut unreview_proteins: Signal<Option<Rc<Vec<ProteinEntity>>>> = use_signal(|| None);

    let peptide: Resource<Result<MaCPepDBPeptide<MaCPepDBProteins<String>>>> =
        use_resource(move || async move {
            let mut peptide = get_peptide(macpepdb_base_url, peptide_sequence).await?;
            let proteins = peptide.take_proteins();
            let (reviewed, unreviewed): (Vec<ProteinEntity>, Vec<ProteinEntity>) =
                proteins.into_iter().partition(|p| p.get_is_reviewed());
            review_proteins.set(Some(Rc::new(reviewed)));
            unreview_proteins.set(Some(Rc::new(unreviewed)));
            Ok(peptide)
        });
    let amino_acid_map = use_resource(move || get_amino_acid_map(macpepdb_base_url));

    rsx! {
        div {
            h2 { "Peptide: {peptide_sequence}" }
            match &*peptide.read_unchecked() {
                Some(Ok(peptide)) => rsx! {
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
                                td { "{peptide.get_sequence().to_owned()}" }
                            }
                            tr { "data-partition": "{peptide.get_partition()}",
                                td { "Theoretical mass (Da)" }
                                td {
                                    RoundedMass { mass: peptide.get_mass() }
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
                    }
                    h3 { "Amino acid composition" }
                    match &*amino_acid_map.read_unchecked() {
                        Some(Ok(amino_acid_map)) => rsx! {
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
                        Some(Err(e)) => rsx! {
                            div { "Error loading the amino acid map {e}" }
                        },
                        None => rsx! {
                            div { "Loading ..." }
                        },
                    }
                    match &*review_proteins.read_unchecked() {
                        Some(proteins) => rsx! {
                            h3 { "Reviewed Proteins" }
                            ProteinList { proteins: proteins.clone() }
                        },
                        None => rsx! {
                            div { "Loading ..." }
                        },
                    }
                    match &*unreview_proteins.read_unchecked() {
                        Some(proteins) => rsx! {
                            h3 { "Unreviewed Proteins" }
                            ProteinList { proteins: proteins.clone() }
                        },
                        None => rsx! {
                            div { "Loading ..." }
                        },
                    }
                },
                Some(Err(e)) => rsx! {
                    div { "Error loading the peptide {e}" }
                },
                None => rsx! {
                    div { "Loading ..." }
                },
            }
        }
    }
}
