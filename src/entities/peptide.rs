// std imports
use std::hash::{Hash, Hasher};

// 3rd party imports
use anyhow::Result;

#[derive(Clone)]
pub struct Peptide {
    partition: i64,
    mass: i64,
    sequence: String,
    missed_cleavages: i8,
    aa_counts: Vec<i8>,
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
        missed_cleavages: i8,
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
            proteome_ids
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
    pub fn get_missed_cleavages(&self) -> i8 {
        return self.missed_cleavages;
    }

    /// Returns the number of missed cleavages as ref
    pub fn get_missed_cleavages_as_ref(&self) -> &i8 {
        return &self.missed_cleavages;
    }

    /// Returns the amino acid counts
    /// 
    pub fn get_aa_counts(&self) -> &Vec<i8> {
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
