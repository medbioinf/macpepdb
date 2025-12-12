// 3rd party imports
use serde::{Deserialize, Serialize};

// internal imports
use crate::{
    database::system_table::IsSystemTableCompatible,
    tools::serde::{deserialize_mass_vec_from_int_vec, serialize_mass_vec_to_float_vec},
};

/// Keeps the configuration parameters for MaCPepDB, e.g. digestion parameters, distribution ...
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Configuration {
    protease_name: String,
    max_number_of_missed_cleavages: Option<usize>,
    min_peptide_length: Option<usize>,
    max_peptide_length: Option<usize>,
    remove_peptides_containing_unknown: bool,
    #[serde(
        serialize_with = "serialize_mass_vec_to_float_vec",
        deserialize_with = "deserialize_mass_vec_from_int_vec"
    )]
    partition_limits: Vec<i64>,
}

impl Configuration {
    pub fn new(
        protease_name: String,
        max_number_of_missed_cleavages: Option<usize>,
        min_peptide_length: Option<usize>,
        max_peptide_length: Option<usize>,
        remove_peptides_containing_unknown: bool,
        partition_limits: Vec<i64>,
    ) -> Self {
        Self {
            protease_name,
            max_number_of_missed_cleavages,
            min_peptide_length,
            max_peptide_length,
            remove_peptides_containing_unknown,
            partition_limits,
        }
    }

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
    pub fn get_partition_limits(&self) -> &Vec<i64> {
        &self.partition_limits
    }
}

impl IsSystemTableCompatible for Configuration {
    fn system_table_id() -> &'static str {
        "configuration"
    }
}
