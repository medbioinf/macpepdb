// 3rd party imports
use serde::Serialize;

/// Keeps the configuration parameters for MaCPepDB, e.g. digestion parameters, distribution ...
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Configuration {
    enzyme_name: String,
    max_number_of_missed_cleavages: usize,
    min_peptide_length: usize,
    max_peptide_length: usize,
    remove_peptides_containing_unknown: bool,
    partition_limits: Vec<i64>,
}

impl Configuration {
    pub fn new(
        enzyme_name: String,
        max_number_of_missed_cleavages: i16,
        min_peptide_length: i16,
        max_peptide_length: i16,
        remove_peptides_containing_unknown: bool,
        partition_limits: Vec<i64>,
    ) -> Self {
        Self {
            enzyme_name,
            max_number_of_missed_cleavages: max_number_of_missed_cleavages as usize,
            min_peptide_length: min_peptide_length as usize,
            max_peptide_length: max_peptide_length as usize,
            remove_peptides_containing_unknown,
            partition_limits,
        }
    }

    /// Returns enzyme name
    ///
    pub fn get_enzyme_name(&self) -> &str {
        self.enzyme_name.as_str()
    }

    /// Returns maximum number of missed cleavages
    ///
    pub fn get_max_number_of_missed_cleavages(&self) -> usize {
        self.max_number_of_missed_cleavages
    }

    /// Returns minimum peptide length
    ///
    pub fn get_min_peptide_length(&self) -> usize {
        self.min_peptide_length
    }

    /// Returns maximum peptide length
    ///
    pub fn get_max_peptide_length(&self) -> usize {
        self.max_peptide_length
    }

    /// Returns whether peptides containing unknown amino acids should be removed
    ///
    pub fn get_remove_peptides_containing_unknown(&self) -> bool {
        self.remove_peptides_containing_unknown
    }

    /// Returns peptide distribution
    ///
    pub fn get_partition_limits(&self) -> &Vec<i64> {
        &self.partition_limits
    }
}
