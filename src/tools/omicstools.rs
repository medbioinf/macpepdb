// 3rd party imports
use dihardts_omicstools::{
    chemistry::amino_acid::{AminoAcid, UNKNOWN},
    proteomics::peptide::Peptide as CleavedPeptide,
};
use fallible_iterator::FallibleIterator;
use serde_json::{json, Value};

// internal imports
use crate::chemistry::amino_acid::calc_sequence_mass_int;
use crate::entities::{peptide::Peptide, protein::Protein};
use crate::tools::peptide_partitioner::get_mass_partition;

/// Removes amino acid sequences containing Unknown (`X`) from digest,
/// acquired by Enzyme.digest()
///
/// # Arguments
/// * `peptides` - Peptides from protease digestion
///
pub fn remove_unknown_from_digest(
    peptides: impl FallibleIterator<Item = CleavedPeptide, Error = anyhow::Error>,
) -> impl FallibleIterator<Item = CleavedPeptide, Error = anyhow::Error> {
    peptides.filter(|pep| Ok(!pep.get_sequence().contains(*UNKNOWN.get_code())))
}

/// Converts the peptides from the cleavage
/// to the internal peptide entities
///
/// # Arguments
/// * `peptides` - Peptides from protease digestion
/// * `partition_limits` - Mass partition limits from peptide partitioner
/// * `protein` - Protein entity
///
pub fn convert_to_internal_peptide<'a>(
    peptides: Box<dyn FallibleIterator<Item = CleavedPeptide, Error = anyhow::Error>>,
    partition_limits: &'a [i64],
    protein: &'a Protein,
) -> impl FallibleIterator<Item = Peptide, Error = anyhow::Error> + 'a {
    peptides.map(|pep| {
        let mass = calc_sequence_mass_int(pep.get_sequence())?;
        let partition = get_mass_partition(partition_limits, mass)?;
        Peptide::new(
            partition as i64,
            mass,
            pep.get_sequence().to_string(),
            pep.get_missed_cleavages() as i16,
            vec![protein.get_accession().clone()],
            protein.get_is_reviewed(),
            !protein.get_is_reviewed(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            #[cfg(feature = "domains")]
            Vec::new(),
        )
    })
}

/// Converts the peptides from the cleavage
/// to the internal peptide entities without any protein information
///
/// # Arguments
/// * `peptides` - Peptides from protease digestion
/// * `partition_limits` - Mass partition limits from peptide partition
///
pub fn convert_to_internal_dummy_peptide(
    peptides: Box<dyn FallibleIterator<Item = CleavedPeptide, Error = anyhow::Error>>,
    partition_limits: &[i64],
) -> impl FallibleIterator<Item = Peptide, Error = anyhow::Error> + '_ {
    peptides.map(|pep| {
        let mass = calc_sequence_mass_int(pep.get_sequence())?;
        let partition = get_mass_partition(partition_limits, mass)?;
        Peptide::new(
            partition as i64,
            mass,
            pep.get_sequence().to_string(),
            pep.get_missed_cleavages() as i16,
            Vec::new(),
            false,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            #[cfg(feature = "domains")]
            Vec::new(),
        )
    })
}

/// Returns something which implements amino acid from `omicstools` crate to json
///
/// # Arguments
/// * `amino_acid` - Amino acid
///
pub fn amino_acid_to_json(amino_acid: &dyn AminoAcid) -> Value {
    json!({
        "name": amino_acid.get_name(),
        "code": amino_acid.get_code(),
        "abbreviation": amino_acid.get_abbreviation(),
        "mono_mass": amino_acid.get_mono_mass(),
        "average_mass": amino_acid.get_average_mass(),
    })
}
