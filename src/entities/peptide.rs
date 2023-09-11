// std imports
use std::{
    cmp,
    collections::HashMap,
    hash::{Hash, Hasher},
    ops::Index,
};

// 3rd party imports
use anyhow::Result;
use scylla::frame::response::result::Row as ScyllaRow;
use scylla::{_macro_internal::ValueList, frame::response::result::CqlValue};
use tracing::error;

// internal imports
use crate::{
    biology::digestion_enzyme::enzyme::Enzyme, entities::protein::Protein,
    tools::cql::get_cql_value,
};

use super::domain::Domain;

#[derive(Clone)]
pub struct Peptide {
    partition: i64,
    mass: i64,
    sequence: String,
    missed_cleavages: i16,
    aa_counts: Vec<i16>,
    proteins: Vec<String>,
    is_swiss_prot: bool,
    is_trembl: bool,
    taxonomy_ids: Vec<i64>,
    unique_taxonomy_ids: Vec<i64>,
    proteome_ids: Vec<String>,
    domains: Vec<Domain>,
}

impl Peptide {
    /// Creates a new peptide
    ///
    /// # Arguments
    /// * `partition` - The mass partition
    /// * `mass` - The mass
    /// * `sequence` - The sequence
    /// * `missed_cleavages` - The number of missed cleavages
    /// * `proteins` - The containing proteins
    /// * `is_swiss_prot` - True if the peptide is contained in a Swiss-Prot protein
    /// * `is_trembl` - True if the peptide is contained in a TrEMBL protein
    /// * `taxonomy_ids` - The taxonomy IDs
    /// * `unique_taxonomy_ids` - Taxonomy IDs where the peptide is only contained in one protein
    /// * `proteome_ids` - The proteome IDs
    ///
    pub fn new(
        partition: i64,
        mass: i64,
        sequence: String,
        missed_cleavages: i16,
        proteins: Vec<String>,
        is_swiss_prot: bool,
        is_trembl: bool,
        taxonomy_ids: Vec<i64>,
        unique_taxonomy_ids: Vec<i64>,
        proteome_ids: Vec<String>,
        domains: Vec<Domain>,
    ) -> Result<Peptide> {
        let mut aa_counts = vec![0; 26];
        for one_letter_code in sequence.chars() {
            aa_counts[one_letter_code as usize % 65] += 1;
        }

        return Ok(Peptide {
            partition,
            mass,
            sequence,
            missed_cleavages,
            aa_counts,
            proteins,
            is_swiss_prot,
            is_trembl,
            taxonomy_ids,
            unique_taxonomy_ids,
            proteome_ids,
            domains,
        });
    }

    /// Returns the mass partition
    ///
    pub fn get_partition(&self) -> i64 {
        return self.partition;
    }

    /// Returns the mass partition as ref
    ///
    pub fn get_partition_as_ref(&self) -> &i64 {
        return &self.partition;
    }

    /// Returns the mass
    pub fn get_mass(&self) -> i64 {
        return self.mass;
    }

    /// Returns the mass as ref
    pub fn get_mass_as_ref(&self) -> &i64 {
        return &self.mass;
    }

    /// Returns the sequence
    pub fn get_sequence(&self) -> &String {
        return &self.sequence;
    }

    /// Returns the number of missed cleavages
    pub fn get_missed_cleavages(&self) -> i16 {
        return self.missed_cleavages;
    }

    /// Returns the number of missed cleavages as ref
    pub fn get_missed_cleavages_as_ref(&self) -> &i16 {
        return &self.missed_cleavages;
    }

    /// Returns the amino acid counts
    ///
    pub fn get_aa_counts(&self) -> &Vec<i16> {
        return &self.aa_counts;
    }

    /// Returns the containing proteins
    pub fn get_proteins(&self) -> &Vec<String> {
        return &self.proteins;
    }

    /// Returns true if the peptide is contained in a Swiss-Prot protein
    ///
    pub fn get_is_swiss_prot(&self) -> bool {
        return self.is_swiss_prot;
    }

    /// Returns true if the peptide is contained in a Swiss-Prot protein as ref
    ///
    pub fn get_is_swiss_prot_as_ref(&self) -> &bool {
        return &self.is_swiss_prot;
    }

    /// Returns true if the peptide is contained in a TrEMBL protein
    ///
    pub fn get_is_trembl(&self) -> bool {
        return self.is_trembl;
    }

    /// Returns true if the peptide is contained in a TrEMBL protein as ref
    ///
    pub fn get_is_trembl_as_ref(&self) -> &bool {
        return &self.is_trembl;
    }

    /// Returns the taxonomy IDs
    ///
    pub fn get_taxonomy_ids(&self) -> &Vec<i64> {
        return &self.taxonomy_ids;
    }

    /// Returns the unique taxonomy IDs
    ///
    pub fn get_unique_taxonomy_ids(&self) -> &Vec<i64> {
        return &self.unique_taxonomy_ids;
    }

    /// Returns the proteome IDs
    ///
    pub fn get_proteome_ids(&self) -> &Vec<String> {
        return &self.proteome_ids;
    }

    /// Returns the proteome IDs
    ///
    pub fn get_domains(&self) -> &Vec<Domain> {
        return &self.domains;
    }

    /// Returns the peptide metadata from the given proteins, format:
    /// (is_swiss_prot, is_trembl, taxonomy_ids, unique_taxonomy_ids, proteome_ids)
    ///
    /// # Arguments
    /// * `proteins` - The proteins
    pub fn get_metadata_from_proteins(
        &self,
        proteins: &Vec<Protein>,
        enzyme: &dyn Enzyme,
    ) -> (bool, bool, Vec<i64>, Vec<i64>, Vec<String>, Vec<Domain>) {
        let is_swiss_prot = proteins.iter().any(|protein| protein.get_is_reviewed());
        let is_trembl = proteins.iter().any(|protein| !protein.get_is_reviewed());

        let mut taxonomy_ids: Vec<i64> = proteins
            .iter()
            .map(|protein| *protein.get_taxonomy_id())
            .collect();

        let mut taxonomy_counters: HashMap<i64, u64> = HashMap::new();
        for taxonomy_id in taxonomy_ids.iter() {
            taxonomy_counters
                .entry(*taxonomy_id)
                .and_modify(|counter| *counter += 1)
                .or_insert(1);
        }
        let unique_taxonomy_ids: Vec<i64> = taxonomy_counters
            .iter()
            .filter(|(_, counter)| **counter == 1)
            .map(|(taxonomy_id, _)| *taxonomy_id)
            .collect();

        let proteome_ids: Vec<String> = proteins
            .iter()
            .map(|protein| protein.get_proteome_id().to_owned())
            .collect();

        let domains: Vec<Domain> = proteins
            .iter()
            .flat_map(|p| {
                let dom = p.get_domains();
                let occurence_indices: Vec<(i64, i64)> = p
                    .get_sequence()
                    .match_indices(&self.get_sequence().as_str())
                    .map(|x| i64::try_from(x.0).unwrap())
                    .map(|x| (x, x + i64::try_from(self.get_sequence().len()).unwrap() - 1))
                    .filter(|x| {
                        x.0 == 0
                            || (!enzyme
                                .get_cleavage_blocker_chars()
                                .contains(&self.get_sequence().chars().nth(0).unwrap())
                                && enzyme.get_cleavage_chars().contains(
                                    &p.get_sequence().chars().nth((x.0 - 1) as usize).unwrap(),
                                ))
                    })
                    .collect();

                let mut domains: Vec<Domain> = vec![];

                for (start_idx, end_idx) in occurence_indices {
                    for d in dom {
                        let start_idx_in_domain_range =
                            d.get_start_index() <= &start_idx && &start_idx <= d.get_end_index();
                        let end_idx_in_domain_range =
                            d.get_start_index() <= &end_idx && &end_idx <= d.get_end_index();

                        if start_idx_in_domain_range || end_idx_in_domain_range {
                            let relative_start_idx = cmp::max(d.get_start_index() - start_idx, 0);
                            let relative_end_idx = i64::try_from(self.get_sequence().len())
                                .unwrap()
                                - 1
                                - cmp::max(end_idx - d.get_end_index(), 0);
                            domains.push(Domain::new(
                                relative_start_idx,
                                relative_end_idx,
                                d.get_name().clone(),
                                d.get_evidence().clone(),
                                Some(p.get_accession().to_string()),
                                Some(d.get_start_index().clone()),
                                Some(d.get_end_index().clone()),
                            ));
                        }
                    }
                }

                return domains;
            })
            .collect();

        taxonomy_ids.sort();
        taxonomy_ids.dedup();

        return (
            is_swiss_prot,
            is_trembl,
            taxonomy_ids,
            unique_taxonomy_ids,
            proteome_ids,
            domains,
        );
    }
}

impl PartialEq for Peptide {
    fn eq(&self, other: &Self) -> bool {
        return self.sequence == other.sequence;
    }
}

impl Eq for Peptide {}

impl Hash for Peptide {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.sequence.hash(state);
    }
}
impl From<ScyllaRow> for Peptide {
    fn from(row: ScyllaRow) -> Self {
        let (
            partition,
            mass,
            sequence,
            missed_cleavages,
            aa_counts,
            proteins,
            is_swiss_prot,
            is_trembl,
            taxonomy_ids,
            unique_taxonomy_ids,
            proteome_ids,
            domains,
        ) = row
            .into_typed::<(
                i64,
                i64,
                String,
                i16,
                Vec<i16>,
                Vec<String>,
                bool,
                bool,
                Option<Vec<i64>>,
                Option<Vec<i64>>,
                Option<Vec<String>>,
                Option<Vec<Domain>>,
            )>()
            .unwrap();
        Self {
            partition,
            mass,
            sequence,
            missed_cleavages,
            aa_counts,
            proteins,
            is_swiss_prot,
            is_trembl,
            taxonomy_ids: taxonomy_ids.unwrap_or(vec![]),
            unique_taxonomy_ids: unique_taxonomy_ids.unwrap_or(vec![]),
            proteome_ids: proteome_ids.unwrap_or(vec![]),
            domains: domains.unwrap_or(vec![]),
        }
    }
}
