// 3rd party imports
use serde::Deserialize;

/// Copy of MacPepDBs configuration for easy deserialization
/// partition limits are stored as f64 for human readability
/// and reconverted by the API
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Configuration {
    protease_name: String,
    max_number_of_missed_cleavages: Option<usize>,
    min_peptide_length: Option<usize>,
    max_peptide_length: Option<usize>,
    remove_peptides_containing_unknown: bool,
    partition_limits: Vec<f64>,
}

impl Configuration {
    /// Returns enzyme name
    ///
    pub fn get_protease_name(&self) -> &str {
        self.protease_name.as_str()
    }

    /// Returns maximum number of missed cleavages
    ///
    pub fn get_max_number_of_missed_cleavages(&self) -> Option<usize> {
        self.max_number_of_missed_cleavages
    }

    /// Returns minimum peptide length
    ///
    pub fn get_min_peptide_length(&self) -> Option<usize> {
        self.min_peptide_length
    }

    /// Returns maximum peptide length
    ///
    pub fn get_max_peptide_length(&self) -> Option<usize> {
        self.max_peptide_length
    }

    /// Returns whether peptides containing unknown amino acids should be removed
    ///
    pub fn get_remove_peptides_containing_unknown(&self) -> bool {
        self.remove_peptides_containing_unknown
    }

    /// Returns peptide distribution
    ///
    pub fn get_partition_limits(&self) -> &Vec<f64> {
        &self.partition_limits
    }
}
