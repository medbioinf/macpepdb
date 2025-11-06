use std::rc::Rc;

// 3rd party imports
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Peptide<T>
where
    T: 'static + PartialEq,
{
    partition: i64,
    mass: f64,
    sequence: String,
    missed_cleavages: i16,
    aa_counts: Vec<i16>,
    proteins: Rc<Vec<T>>,
    is_swiss_prot: bool,
    is_trembl: bool,
    taxonomy_ids: Vec<i64>,
    unique_taxonomy_ids: Vec<i64>,
    proteome_ids: Rc<Vec<String>>,
}

impl<T> Peptide<T>
where
    T: 'static + PartialEq,
{
    /// Returns the mass partition
    ///
    pub fn get_partition(&self) -> i64 {
        self.partition
    }

    /// Returns the mass
    pub fn get_mass(&self) -> f64 {
        self.mass
    }

    /// Returns the sequence
    pub fn get_sequence(&self) -> &String {
        &self.sequence
    }

    /// Returns the number of missed cleavages
    pub fn get_missed_cleavages(&self) -> i16 {
        self.missed_cleavages
    }

    /// Returns the amino acid counts
    ///
    pub fn get_aa_counts(&self) -> &Vec<i16> {
        &self.aa_counts
    }

    /// Returns true if the peptide is contained in a Swiss-Prot protein
    ///
    pub fn get_is_swiss_prot(&self) -> bool {
        self.is_swiss_prot
    }

    /// Returns true if the peptide is contained in a TrEMBL protein
    ///
    pub fn get_is_trembl(&self) -> bool {
        self.is_trembl
    }

    /// Returns the taxonomy IDs
    ///
    pub fn get_taxonomy_ids(&self) -> &Vec<i64> {
        &self.taxonomy_ids
    }

    /// Returns the unique taxonomy IDs
    ///
    pub fn get_unique_taxonomy_ids(&self) -> &Vec<i64> {
        &self.unique_taxonomy_ids
    }

    /// Returns the proteome IDs
    ///
    pub fn get_proteome_ids(&self) -> &Vec<String> {
        &self.proteome_ids
    }

    // Returns the proteins containing
    pub fn get_proteins(&self) -> Rc<Vec<T>> {
        self.proteins.clone()
    }
}
