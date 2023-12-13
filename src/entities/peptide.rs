// 3rd party imports
use serde::Deserialize;

// internal imports
use crate::entities::protein::Protein;

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Peptide<T> {
    partition: i64,
    mass: f64,
    sequence: String,
    missed_cleavages: i16,
    aa_counts: Vec<i16>,
    proteins: Vec<T>,
    is_swiss_prot: bool,
    is_trembl: bool,
    taxonomy_ids: Vec<i64>,
    unique_taxonomy_ids: Vec<i64>,
    proteome_ids: Vec<String>,
}

impl<T> Peptide<T> {
    /// Returns the mass partition
    ///
    pub fn get_partition(&self) -> i64 {
        return self.partition;
    }

    /// Returns the mass
    pub fn get_mass(&self) -> f64 {
        return self.mass;
    }

    /// Returns the sequence
    pub fn get_sequence(&self) -> &String {
        return &self.sequence;
    }

    /// Returns the number of missed cleavages
    pub fn get_missed_cleavages(&self) -> i16 {
        return self.missed_cleavages;
    }

    /// Returns the amino acid counts
    ///
    pub fn get_aa_counts(&self) -> &Vec<i16> {
        return &self.aa_counts;
    }

    /// Returns the containing proteins
    pub fn get_proteins(&self) -> &Vec<T> {
        return &self.proteins;
    }

    /// Returns true if the peptide is contained in a Swiss-Prot protein
    ///
    pub fn get_is_swiss_prot(&self) -> bool {
        return self.is_swiss_prot;
    }

    /// Returns true if the peptide is contained in a TrEMBL protein
    ///
    pub fn get_is_trembl(&self) -> bool {
        return self.is_trembl;
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

/// Peptides with full protein records
///
impl<T> Peptide<Protein<T>> {
    /// Returns a vector of reviewed proteins
    ///
    pub fn get_reviewed_proteins(&self) -> Vec<&Protein<T>> {
        return self
            .proteins
            .iter()
            .filter(|p| p.get_is_reviewed())
            .collect();
    }

    /// Returns a vector of unreviewed proteins
    ///
    pub fn get_unreviewed_proteins(&self) -> Vec<&Protein<T>> {
        return self
            .proteins
            .iter()
            .filter(|p| !p.get_is_reviewed())
            .collect();
    }
}
