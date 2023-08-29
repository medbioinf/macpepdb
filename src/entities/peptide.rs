// std imports
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

// 3rd party imports
use anyhow::Result;
use scylla::frame::response::result::Row as ScyllaRow;
use scylla::{_macro_internal::ValueList, frame::response::result::CqlValue};
use tokio_postgres::Row;
use tracing::error;

// internal imports
use crate::{entities::protein::Protein, tools::cql::get_cql_value};

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

    /// Returns the peptide metadata from the given proteins, format:
    /// (is_swiss_prot, is_trembl, taxonomy_ids, unique_taxonomy_ids, proteome_ids)
    ///
    /// # Arguments
    /// * `proteins` - The proteins
    pub fn get_metadata_from_proteins(
        proteins: &Vec<Protein>,
    ) -> (bool, bool, Vec<i64>, Vec<i64>, Vec<String>) {
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

        taxonomy_ids.sort();
        taxonomy_ids.dedup();

        return (
            is_swiss_prot,
            is_trembl,
            taxonomy_ids,
            unique_taxonomy_ids,
            proteome_ids,
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

impl From<Row> for Peptide {
    fn from(row: Row) -> Self {
        Self {
            partition: row.get("partition"),
            mass: row.get("mass"),
            sequence: row.get("sequence"),
            missed_cleavages: row.get("missed_cleavages"),
            aa_counts: row.get("aa_counts"),
            proteins: row.get("proteins"),
            is_swiss_prot: row.get("is_swiss_prot"),
            is_trembl: row.get("is_trembl"),
            taxonomy_ids: row.get("taxonomy_ids"),
            unique_taxonomy_ids: row.get("unique_taxonomy_ids"),
            proteome_ids: row.get("proteome_ids"),
        }
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
                Vec<i64>,
                Vec<i64>,
                Vec<String>,
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
            taxonomy_ids,
            unique_taxonomy_ids,
            proteome_ids,
        }
    }
}
