/// Defines additional functions for the digestion_enzyme module
/// 

// std imports
use std::collections::HashMap;

// 3rd party imports
use anyhow::{bail, Result};

// internal imports
use crate::biology::digestion_enzyme::enzyme::Enzyme;
use crate::biology::digestion_enzyme::trypsin::{
    Trypsin, 
    NAME as TRYPSIN_NAME
};
use crate::chemistry::amino_acid::{
    UNKNOWN,
    calc_sequence_mass
};
use crate::entities::peptide::Peptide;
use crate::entities::protein::Protein;
use crate::tools::{
    peptide_partitioner::get_mass_partition
};

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
        TRYPSIN_NAME => Ok(Box::new(Trypsin::new(
            max_number_of_missed_cleavages,
            min_peptide_length,
            max_peptide_length
        ))),
        _ => bail!("Enzyme {} not supported", name),
    }
}

/// Removes amino acid sequences containing Unknown (`X`) from digest,
/// acquired by Enzyme.digest()
/// 
/// # Arguments
/// * `digest` - Digest of a protein sequence (sequence, number of missed cleavages)
/// 
pub fn remove_unknown_from_digest(digest: &mut HashMap<String, i16>) {
    digest.retain(|peptide, _| !peptide.contains(*UNKNOWN.get_one_letter_code()));
}

/// Create peptide entities collection from digest
/// 
/// # Arguments
/// * `digest` - Digest of a protein sequence (sequence, number of missed cleavages)
/// * `partition_limits` - Mass partition limits from peptide partitioner
/// * `protein_opt` - Optional protein entity, if not given the protein related fields will be empty
/// 
pub fn create_peptides_entities_from_digest<T>(
    digest: &HashMap<String, i16>, partition_limits: &Vec<i64>, protein_opt: Option<&Protein>
) -> Result<T> where T: IntoIterator + std::iter::FromIterator<Peptide>, T::Item: Into<Peptide> {
    digest.iter()
    .map(|(sequence, missed_cleavages)| {
        let mass = calc_sequence_mass(sequence)?;
        let partition = get_mass_partition(partition_limits, mass)?;
        Peptide::new(
            partition as i64,
            mass,
            sequence.to_string(),
            *missed_cleavages as i8,
            if let Some(protein) = protein_opt { vec![protein.get_accession().clone()] } else { Vec::new() },
            if let Some(protein) = protein_opt { protein.get_is_reviewed() } else { false },
            if let Some(protein) = protein_opt { !protein.get_is_reviewed() } else { false },
            Vec::new(),
            Vec::new(),
            Vec::new()
        )
    })
    .collect::<Result<T, _>>()
}


