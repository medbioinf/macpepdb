use std::{fmt::Display, hash::Hash, sync::OnceLock};

use itertools::Itertools;
use serde::Serialize;
use thiserror::Error;
use tokio_postgres::Row;
use zerocopy::IntoBytes;

use crate::{
    amino_acid::{AminoAcid, AminoAcidBitCode},
    molecules::WATER_MONO_MASS,
    protein_ids::ProteinIds,
    sequence::{
        CompactSequence, IsBitSequence, IsSimpleSequence, ModifiedSequence, ModifiedSequencePart,
        PeptideSequence as Sequence,
    },
};

pub const MAX_AMINO_ACID_BIT_CODE: usize = (b'Z' - b'A' + 1) as usize;

pub const IS_SWISS_PROT_BIT: usize = 0;
pub const IS_TREMBL_BIT: usize = 1;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in peptide: {0}")]
    Client(#[from] crate::client::Error),
    #[error("Row decoding error in peptide: {0}")]
    Row(#[from] tokio_postgres::Error),
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
    fn is_swiss_prot(&self) -> bool;
    fn is_trembl(&self) -> bool;

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

#[derive(Serialize)]
pub struct Peptide {
    partition: Option<i64>,
    mass: i64,
    sequence: Sequence,
    protein_ids: ProteinIds,
    unique_taxonomy_ids: Vec<i32>,
    non_unique_taxonomy_ids: Vec<i32>,
    /// Bit flags for e.g. review status, see constants to see what is stored in which bit
    flags: i8,
    #[serde(skip)]
    metadata_id: Option<i64>,
    #[serde(skip)]
    amino_acid_counts: OnceLock<[u8; MAX_AMINO_ACID_BIT_CODE]>,
}

impl Peptide {
    pub fn new(
        sequence: Sequence,
        protein_ids: Vec<i32>,
        unique_taxonomy_ids: Vec<i32>,
        non_unique_taxonomy_ids: Vec<i32>,
        is_swiss_prot: bool,
        is_trembl: bool,
    ) -> Self {
        let mass = Self::to_peptide_mass(&sequence);
        let mut flags: i8 = 0b0100_0000;
        if is_swiss_prot {
            flags |= 1 << IS_SWISS_PROT_BIT;
        }
        if is_trembl {
            flags |= 1 << IS_TREMBL_BIT;
        }

        Self {
            mass,
            sequence,
            protein_ids: protein_ids.into(),
            unique_taxonomy_ids,
            non_unique_taxonomy_ids,
            partition: None,
            flags,
            metadata_id: None,
            amino_acid_counts: OnceLock::new(),
        }
    }

    /// Build-time constructor: the protein-id set has been interned to `metadata_id`, so
    /// the peptide carries only the reference. `protein_ids` stays empty until resolved.
    pub fn new_with_metadata(
        sequence: Sequence,
        metadata_id: i64,
        unique_taxonomy_ids: Vec<i32>,
        non_unique_taxonomy_ids: Vec<i32>,
        is_swiss_prot: bool,
        is_trembl: bool,
    ) -> Self {
        let mass = Self::to_peptide_mass(&sequence);
        let mut flags: i8 = 0b0100_0000;
        if is_swiss_prot {
            flags |= 1 << IS_SWISS_PROT_BIT;
        }
        if is_trembl {
            flags |= 1 << IS_TREMBL_BIT;
        }

        Self {
            mass,
            sequence,
            protein_ids: ProteinIds::default(),
            unique_taxonomy_ids,
            non_unique_taxonomy_ids,
            flags,
            partition: None,
            metadata_id: Some(metadata_id),
            amino_acid_counts: OnceLock::new(),
        }
    }

    pub fn partition(&self) -> Option<i64> {
        self.partition
    }

    pub(crate) fn set_partition(&mut self, partition: i64) {
        self.partition = Some(partition);
    }

    pub fn flags(&self) -> i8 {
        self.flags
    }

    pub fn flags_as_ref(&self) -> &i8 {
        &self.flags
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

    pub fn peptide_mass_from_amino_acid_bits<'a>(
        amino_acids: impl Iterator<Item = &'a AminoAcidBitCode>,
    ) -> i64 {
        amino_acids.fold(WATER_MONO_MASS, |acc, bit_code| {
            acc + AminoAcid::by_bit_code(bit_code).mono_mass()
        })
    }

    pub fn unique_taxonomy_ids(&self) -> &[i32] {
        &self.unique_taxonomy_ids
    }

    pub fn non_unique_taxonomy_ids(&self) -> &[i32] {
        &self.non_unique_taxonomy_ids
    }

    pub fn protein_ids(&self) -> &ProteinIds {
        &self.protein_ids
    }

    pub fn metadata_id(&self) -> Option<i64> {
        self.metadata_id
    }

    /// Populate `protein_ids` after resolving `metadata_id` against `peptide_metadata`
    /// (used by the single-peptide GET endpoint to reproduce the full response).
    pub fn set_protein_ids(&mut self, protein_ids: ProteinIds) {
        self.protein_ids = protein_ids;
    }

    pub fn cql_size(&self) -> usize {
        const ROW_OVERHEAD: usize = 32;
        // A non-frozen list<int> stores each element as its own cell.
        const LIST_CELL_SIZE: usize = 16 + 8 + std::mem::size_of::<i32>();
        // per-row overhead + partition + mass + sequence (clustering blob)
        // + metadata_id reference (i64) + taxonomy list cells (i32)
        ROW_OVERHEAD
            + std::mem::size_of::<i64>()
            + std::mem::size_of::<i64>()
            + self.sequence.cql_size()
            + std::mem::size_of::<i64>()
            + (self.unique_taxonomy_ids.len() + self.non_unique_taxonomy_ids.len()) * LIST_CELL_SIZE
            + std::mem::size_of::<i64>()
    }
}

impl AsRef<Peptide> for Peptide {
    fn as_ref(&self) -> &Peptide {
        self
    }
}

impl Display for Peptide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "sequence:    {}", self.sequence())?;
        writeln!(f, "mass:        {} Da", crate::mass::to_float(self.mass))?;
        writeln!(
            f,
            "partititon:  {}",
            self.partition()
                .map(|partition| format!("{partition}"))
                .unwrap_or(String::from("not persisted"))
        )?;
        writeln!(
            f,
            "SwissProt?:  {}",
            if self.is_swiss_prot() { "yes" } else { "no" }
        )?;
        writeln!(
            f,
            "TrEMBL?:     {}",
            if self.is_trembl() { "yes" } else { "no" }
        )?;
        writeln!(
            f,
            "proteins:    {}",
            self.unique_taxonomy_ids().iter().join(", ")
        )?;
        writeln!(
            f,
            "unique tax.: {}",
            self.unique_taxonomy_ids().iter().join(", ")
        )?;
        writeln!(
            f,
            "tax.:        {}",
            self.non_unique_taxonomy_ids().iter().join(", ")
        )
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

    fn is_swiss_prot(&self) -> bool {
        (self.flags & (1 << IS_SWISS_PROT_BIT)) != 0
    }

    fn is_trembl(&self) -> bool {
        (self.flags & (1 << IS_TREMBL_BIT)) != 0
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
        Ok(Self::new(
            sequence,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            false,
            false,
        ))
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
        Ok(Self::new(
            sequence,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            false,
            false,
        ))
    }
}

#[derive(Serialize)]
pub struct Peptidoform {
    sequence: ModifiedSequence,
    mass: i64,
    /// Like peptides
    flags: i8,
    #[serde(skip)]
    amino_acid_counts: OnceLock<[u8; MAX_AMINO_ACID_BIT_CODE]>,
}

impl Peptidoform {
    pub fn new(
        sequence: ModifiedSequence,
        mass: i64,
        is_swiss_prot: bool,
        is_trembl: bool,
    ) -> Self {
        let mut flags: i8 = 0b0000_0000;
        if is_swiss_prot {
            flags |= 1 << IS_SWISS_PROT_BIT;
        }
        if is_trembl {
            flags |= 1 << IS_TREMBL_BIT;
        }

        Self {
            sequence,
            mass,
            flags,
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
            flags: peptide.flags(),
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

    fn is_swiss_prot(&self) -> bool {
        (self.flags & (1 << IS_SWISS_PROT_BIT)) != 0
    }

    fn is_trembl(&self) -> bool {
        (self.flags & (1 << IS_TREMBL_BIT)) != 0
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

impl TryFrom<Row> for Peptide {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Self {
            partition: Some(row.try_get("partition")?),
            mass: row.try_get("mass")?,
            sequence: row.try_get("sequence")?,
            // Resolved on demand (GET endpoint) from peptide_metadata; empty when read directly.
            protein_ids: ProteinIds::default(),
            unique_taxonomy_ids: row.try_get("unique_taxonomy_ids")?,
            non_unique_taxonomy_ids: row.try_get("non_unique_taxonomy_ids")?,
            flags: row.try_get("flags")?,
            metadata_id: Some(row.try_get("metadata_id")?),
            amino_acid_counts: OnceLock::new(),
        })
    }
}

impl Peptide {

    pub fn try_from_search_row(row: &Row) -> Result<Self, Error> {
        Ok(Self {
            partition: None,
            mass: row.try_get("mass")?,
            sequence: row.try_get("sequence")?,
            protein_ids: ProteinIds::default(),
            unique_taxonomy_ids: Vec::new(),
            non_unique_taxonomy_ids: Vec::new(),
            flags: row.try_get("flags")?,
            metadata_id: None,
            amino_acid_counts: OnceLock::new(),
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
            false,
            false,
        );
        //   32 = ROW_OVERHEAD
        // +  8 = partition (i64)
        // +  8 = mass (i64)
        // + 32 = sequence.cql_size()
        // +  8 = metadata_id reference (i64)
        // + 140 = 5 taxonomy list cells (2 unique + 3 non-unique) x LIST_CELL_SIZE (28)
        // + 8 = flags
        assert_eq!(peptide.cql_size(), 236);
    }
}
