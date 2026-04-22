use std::fmt::{Debug, Display};

use fallible_iterator::FallibleIterator;
use thiserror::Error;

use dihardts_omicstools::proteomics::{
    peptide::Peptide as CleavedPeptide,
    proteases::{
        functions::get_by_name as get_protease_by_name, protease::Protease as InnerProtease,
    },
};

use crate::{amino_acid::UNKNOWN, peptide::Peptide, sequence::PeptideSequence as Sequence};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Unable to cleave sequence: {0}")]
    UnableToCleave(String),
    #[error("Unknown amino acid encountered: {0}")]
    UnknownAminoAcid(String),
    #[error("Unable to get partition for mass: {0}")]
    UnableToGetPartition(String),
    #[error("Protease creation failed: {0}")]
    FailedCreation(String),
    #[error("Sequence error in protease: {0}")]
    Sequence(#[from] crate::sequence::Error),
    #[error("Peptide error in protease: {0}")]
    Peptide(#[from] crate::peptide::Error),
}

/// Wrapper around dihardts_omicstools protease to produce MacPepDB compatible peptides
///
///
pub struct Protease {
    inner_protease: Box<dyn InnerProtease>,
}

impl Protease {
    pub fn cleave<'a>(
        &self,
        sequence: &str,
        remove_unknown: bool,
    ) -> Result<impl FallibleIterator<Item = Peptide, Error = Error> + 'a, Error> {
        let iter = self
            .inner_protease
            .cleave(sequence)
            .map_err(|err| Error::UnableToCleave(err.to_string()))?
            .map_err(|err| Error::UnableToCleave(err.to_string())) // convert the elements' errors
            .filter(move |pep| {
                if remove_unknown {
                    Ok(!pep.get_sequence().contains(UNKNOWN.code()))
                } else {
                    Ok(true)
                }
            })
            .map(move |pep| {
                let peptide = Self::to_internal_peptide(pep)?;
                Ok::<Peptide, Error>(peptide)
            });
        Ok(iter)
    }

    fn to_internal_peptide(pep: CleavedPeptide) -> Result<Peptide, Error> {
        let sequence = Sequence::try_from(pep.get_sequence().as_str())?;
        Ok(Peptide::new(sequence))
    }

    pub fn get_by_name(
        name: &str,
        min_len: Option<usize>,
        max_len: Option<usize>,
        max_missed_cleavages: Option<usize>,
    ) -> Result<Self, Error> {
        Ok(
            get_protease_by_name(name, min_len, max_len, max_missed_cleavages)
                .map_err(|err| Error::FailedCreation(err.to_string()))?
                .into(),
        )
    }

    pub fn name(&self) -> &str {
        self.inner_protease.get_name()
    }

    pub fn min_length(&self) -> Option<usize> {
        self.inner_protease.get_min_length()
    }

    pub fn max_length(&self) -> Option<usize> {
        self.inner_protease.get_max_length()
    }

    pub fn max_missed_cleavages(&self) -> Option<usize> {
        self.inner_protease.get_max_missed_cleavages()
    }
}

impl Clone for Protease {
    fn clone(&self) -> Self {
        // Unwrap should be save as the inner protease is working
        Self::get_by_name(
            self.name(),
            self.min_length(),
            self.max_length(),
            self.max_missed_cleavages(),
        )
        .unwrap()
    }
}

impl Debug for Protease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Protease {{ name: {}, min_length: {:?}, max_length: {:?}, max_missed_cleavages: {:?} }}",
            self.name(),
            self.min_length(),
            self.max_length(),
            self.max_missed_cleavages(),
        )
    }
}

impl Display for Protease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "name: {}, peptide length {} - {}, max. missed_cleavages: {}",
            self.name(),
            self.min_length().unwrap_or(0),
            self.max_length()
                .map(|max_length| max_length.to_string())
                .unwrap_or("∞".to_string()),
            self.max_missed_cleavages()
                .map(|missed_cleavages| missed_cleavages.to_string())
                .unwrap_or("after each amino acid".to_string()),
        )
    }
}

impl From<Box<dyn InnerProtease>> for Protease {
    fn from(inner: Box<dyn InnerProtease>) -> Self {
        Self {
            inner_protease: inner,
        }
    }
}

impl PartialEq for Protease {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
            && self.min_length() == other.min_length()
            && self.max_length() == other.max_length()
            && self.max_missed_cleavages() == other.max_missed_cleavages()
    }
}
