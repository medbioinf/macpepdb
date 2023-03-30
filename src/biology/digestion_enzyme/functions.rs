/// Defines additional functions for the digestion_enzyme module
/// 

// 3rd party imports
use anyhow::{bail, Result};

// internal imports
use crate::biology::digestion_enzyme::enzyme::Enzyme;
use crate::biology::digestion_enzyme::trypsin::Trypsin;

/// Returns an enzyme by name
/// 
/// # Arguments
/// * `name` - Name of the enzyme e.g. "trypsin" or "Trypsin"
/// * `max_number_of_missed_cleavages` - Maximum number of missed cleavages
/// * `min_peptide_length` - Minimum length of a peptide
/// * `max_peptide_length` - Maximum length of a peptide
/// 
pub fn get_enzyme_by_name(
    name: &str, max_number_of_missed_cleavages: usize, 
    min_peptide_length: usize, max_peptide_length: usize
) -> Result<Box<dyn Enzyme>> {
    match name.to_lowercase().as_str() {
        "trypsin" => Ok(Box::new(Trypsin::new(
            max_number_of_missed_cleavages,
            min_peptide_length,
            max_peptide_length
        ))),
        _ => bail!("Enzyme {} not supported", name),
    }
}