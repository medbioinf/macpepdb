use std::{hash::Hash, sync::OnceLock};

use scylla::{DeserializeRow, SerializeRow};

use serde::Serialize;
use thiserror::Error;
use zerocopy::IntoBytes;

use crate::{
    amino_acid::{AminoAcid, AminoAcidBitCode},
    molecules::WATER_MONO_MASS,
    sequence::{
        CompactSequence, IsBitSequence, IsSimpleSequence, ModifiedSequence, ModifiedSequencePart,
        PeptideSequence as Sequence,
    },
};

pub const MAX_AMINO_ACID_BIT_CODE: usize = (b'Z' - b'A') as usize;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in peptide: {0}")]
    Client(#[from] crate::client::Error),
    #[error("CQL execution error in peptide: {0}")]
    CqlExecution(#[from] Box<scylla::errors::ExecutionError>),
    #[error("CQL paged execution error in peptide: {0}")]
    CqlPagedExecution(#[from] Box<scylla::errors::PagerExecutionError>),
    #[error("CQL type check failed in peptide: {0}")]
    CqlTypeCheck(#[from] scylla::errors::TypeCheckError),
    #[error("Partition not found peptide `{0}` with mass {1}")]
    NoPartition(String, i64),
    #[error("Sequence error in peptide: {0}")]
    Sequence(#[from] crate::sequence::Error),
    #[error("Amino acid error in peptide: {0}")]
    AminoAcid(#[from] crate::amino_acid::Error),
}

pub trait IsPeptide: Send + Sync {
    type Sequence: IsSimpleSequence;

    fn sequence(&self) -> &Self::Sequence;
    fn mass(&self) -> i64;

    fn amino_acid_counts(&self) -> &[u8; MAX_AMINO_ACID_BIT_CODE];

    fn amino_acid_count(&self, amino_acid: &'static AminoAcid) -> u8 {
        let idx = (amino_acid.code() as u8 - b'A') as usize;
        self.amino_acid_counts()[idx]
    }

    fn amino_acid_count_by_code(&self, code: char) -> Result<u8, Error> {
        let amino_acid = AminoAcid::by_code(code)?;
        Ok(self.amino_acid_count(amino_acid))
    }

    fn amino_acid_count_by_bit_code(&self, code: AminoAcidBitCode) -> u8 {
        let amino_acid = AminoAcid::by_bit_code(&code);
        self.amino_acid_count(amino_acid)
    }
}

#[derive(DeserializeRow, Serialize, SerializeRow)]
pub struct Peptide {
    partition: Option<i64>,
    mass: i64,
    sequence: Sequence,
    protein_ids: Vec<i32>,
    unique_taxonomy_ids: Vec<i32>,
    non_unique_taxonomy_ids: Vec<i32>,
    #[scylla(skip)]
    #[serde(skip)]
    amino_acid_counts: OnceLock<[u8; MAX_AMINO_ACID_BIT_CODE]>,
}

impl Peptide {
    pub fn new(
        sequence: Sequence,
        protein_ids: Vec<i32>,
        unique_taxonomy_ids: Vec<i32>,
        non_unique_taxonomy_ids: Vec<i32>,
    ) -> Self {
        let mass = Self::to_peptide_mass(&sequence);
        Self {
            mass,
            sequence,
            protein_ids,
            unique_taxonomy_ids,
            non_unique_taxonomy_ids,
            partition: None,
            amino_acid_counts: OnceLock::new(),
        }
    }

    pub fn partition(&self) -> Option<i64> {
        self.partition
    }

    pub(crate) fn set_partition(&mut self, partition: i64) {
        self.partition = Some(partition);
    }

    pub fn len(&self) -> usize {
        self.sequence.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sequence.is_empty()
    }

    pub fn into_sequence(self) -> Sequence {
        self.sequence
    }

    pub fn to_peptide_mass(sequence: &Sequence) -> i64 {
        sequence
            .amino_acids()
            .fold(WATER_MONO_MASS, |acc, amino_acid| {
                acc + amino_acid.mono_mass()
            })
    }

    pub fn unique_taxonomy_ids(&self) -> &[i32] {
        &self.unique_taxonomy_ids
    }

    pub fn non_unique_taxonomy_ids(&self) -> &[i32] {
        &self.non_unique_taxonomy_ids
    }

    pub fn cql_size(&self) -> usize {
        // partition + mass + protein_ids + taxonomyies + sequence
        std::mem::size_of::<i64>()
            + std::mem::size_of::<i64>()
            + self.protein_ids.len() * std::mem::size_of::<i32>()
            + std::mem::size_of::<i32>()
                * (self.unique_taxonomy_ids.len() + self.non_unique_taxonomy_ids.len())
            + self.sequence.cql_size()
    }
}

impl AsRef<Peptide> for Peptide {
    fn as_ref(&self) -> &Peptide {
        self
    }
}

impl Eq for Peptide {}

impl PartialEq for Peptide {
    fn eq(&self, other: &Self) -> bool {
        self.mass == other.mass && self.sequence == other.sequence
    }
}

impl Hash for Peptide {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.mass.hash(state);
        self.sequence.hash(state);
    }
}

impl IsPeptide for Peptide {
    type Sequence = Sequence;

    fn sequence(&self) -> &Self::Sequence {
        &self.sequence
    }

    fn mass(&self) -> i64 {
        self.mass
    }

    fn amino_acid_counts(&self) -> &[u8; MAX_AMINO_ACID_BIT_CODE] {
        self.amino_acid_counts.get_or_init(|| {
            let mut counts = [0; MAX_AMINO_ACID_BIT_CODE];

            self.sequence
                .amino_acid_bit_codes()
                .for_each(|bit_code| counts[bit_code.as_bytes()[0] as usize] += 1);
            counts
        })
    }
}

impl TryFrom<&str> for Peptide {
    type Error = Error;

    fn try_from(sequence: &str) -> Result<Self, Self::Error> {
        let sequence = Sequence::try_from(sequence)?;
        Ok(Self::new(sequence, Vec::new(), Vec::new(), Vec::new()))
    }
}

impl TryFrom<String> for Peptide {
    type Error = Error;

    fn try_from(sequence: String) -> Result<Self, Self::Error> {
        Self::try_from(sequence.as_str())
    }
}

impl TryFrom<CompactSequence> for Peptide {
    type Error = Error;

    fn try_from(sequence: CompactSequence) -> Result<Self, Self::Error> {
        let sequence = Sequence::try_from(sequence)?;
        Ok(Self::new(sequence, Vec::new(), Vec::new(), Vec::new()))
    }
}

#[derive(Serialize)]
pub struct Peptidoform {
    sequence: ModifiedSequence,
    mass: i64,
    #[serde(skip)]
    amino_acid_counts: OnceLock<[u8; MAX_AMINO_ACID_BIT_CODE]>,
}

impl Peptidoform {
    pub fn new(sequence: ModifiedSequence, mass: i64) -> Self {
        Self {
            sequence,
            mass,
            amino_acid_counts: OnceLock::new(),
        }
    }

    pub fn sequence(&self) -> &ModifiedSequence {
        &self.sequence
    }

    pub fn mass(&self) -> i64 {
        self.mass
    }

    pub fn amino_acid_counts(&self) -> &[u8; MAX_AMINO_ACID_BIT_CODE] {
        self.amino_acid_counts.get_or_init(|| {
            let mut counts = [0; MAX_AMINO_ACID_BIT_CODE];

            self.sequence
                .iter()
                .filter_map(|part| match part {
                    ModifiedSequencePart::AminoAcid(aa) => Some(*aa),
                    _ => None,
                })
                .for_each(|bit_code| counts[bit_code.as_bytes()[0] as usize] += 1);
            counts
        })
    }

    pub fn amino_acid_count(&self, amino_acid: &'static AminoAcid) -> u8 {
        let idx = (amino_acid.code() as u8 - b'A') as usize;
        self.amino_acid_counts()[idx]
    }

    pub fn amino_acid_count_by_bit_code(&self, code: AminoAcidBitCode) -> u8 {
        let amino_acid = AminoAcid::by_bit_code(&code);
        self.amino_acid_count(amino_acid)
    }
}

impl From<Peptide> for Peptidoform {
    fn from(peptide: Peptide) -> Self {
        Self {
            mass: peptide.mass(),
            sequence: peptide.into_sequence().into(),
            amino_acid_counts: OnceLock::new(),
        }
    }
}

impl Eq for Peptidoform {}

impl Hash for Peptidoform {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.mass.hash(state);
        self.sequence.hash(state);
    }
}

impl PartialEq for Peptidoform {
    fn eq(&self, other: &Self) -> bool {
        self.mass == other.mass && self.sequence == other.sequence
    }
}

impl IsPeptide for Peptidoform {
    type Sequence = ModifiedSequence;

    fn sequence(&self) -> &Self::Sequence {
        &self.sequence
    }

    fn mass(&self) -> i64 {
        self.mass
    }

    fn amino_acid_counts(&self) -> &[u8; MAX_AMINO_ACID_BIT_CODE] {
        self.amino_acid_counts.get_or_init(|| {
            let mut counts = [0; MAX_AMINO_ACID_BIT_CODE];

            self.sequence
                .iter()
                .filter_map(|part| match part {
                    ModifiedSequencePart::AminoAcid(aa) => Some(*aa),
                    _ => None,
                })
                .for_each(|bit_code| counts[bit_code.as_bytes()[0] as usize] += 1);
            counts
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_cql_size() {
        use super::*;

        let peptide = Peptide::new(
            Sequence::try_from("VTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSRNVIQISNDLENLR").unwrap(),
            vec![1],
            vec![1, 2],
            vec![1, 2, 3],
        );
        //   8 = partition (i64)
        // + 8 = mass (i64)
        // + 4 = 1x protein_ids
        // + 20 = 5x taxonomy_ids
        // + sequence.cql_size() (32)
        assert_eq!(peptide.cql_size(), 72);
    }
}
