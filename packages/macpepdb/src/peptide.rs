use std::{
    fmt::Display,
    hash::Hash,
    sync::{Arc, OnceLock},
};

use itertools::Itertools;
use macpepdb_web_common::responses::peptide::PeptideResponse;
use serde::Serialize;
use thiserror::Error;
use zerocopy::IntoBytes;

use crate::{
    amino_acid::{AminoAcid, AminoAcidBitCode},
    molecules::WATER_MONO_MASS,
    protein_ids::ProteinIds,
    sequence::{
        CompactSequence, IsBitSequence, IsSimpleSequence, ModifiedSequence,
        PeptideSequence as Sequence,
    },
};

/// Number of slots in a peptide's amino-acid count array, one per letter `A..=Z`
/// (amino acid bit codes are indexed by `char - 'A'`).
pub const MAX_AMINO_ACID_BIT_CODE: usize = (b'Z' - b'A' + 1) as usize;

/// Bit index in a `Peptide`/`Peptidoform`'s flags marking it as originating from at least
/// one reviewed (SwissProt) protein.
pub const IS_SWISS_PROT_BIT: usize = 0;
/// Bit index in a `Peptide`/`Peptidoform`'s flags marking it as originating from at least
/// one unreviewed (TrEMBL) protein.
pub const IS_TREMBL_BIT: usize = 1;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in peptide: {0}")]
    Client(#[from] crate::client::Error),
    #[error("Partition not found peptide `{0}` with mass {1}")]
    NoPartition(String, i64),
    #[error("Sequence error in peptide: {0}")]
    Sequence(#[from] crate::sequence::Error),
    #[error("Amino acid error in peptide: {0}")]
    AminoAcid(#[from] crate::amino_acid::Error),
}

/// Trait defining the behavior shared by [`Peptide`] and [`Peptidoform`]
///
pub trait IsPeptide: Send + Sync {
    /// Concrete sequence representation backing this peptide kind.
    type Sequence: IsSimpleSequence;

    /// Returns the sequence.
    fn sequence(&self) -> &Self::Sequence;
    /// Returns the mono-isotopic mass in integer form (see `mass::to_int`).
    fn mass(&self) -> i64;
    /// Returns whether the peptide originates from at least one reviewed (SwissProt) protein.
    fn is_swiss_prot(&self) -> bool;
    /// Returns whether the peptide originates from at least one unreviewed (TrEMBL) protein.
    fn is_trembl(&self) -> bool;

    /// Returns the per-amino-acid occurrence counts, indexed by amino acid bit code.
    fn amino_acid_counts(&self) -> &[u8; MAX_AMINO_ACID_BIT_CODE];

    /// Returns how often `amino_acid` occurs in the sequence.
    fn amino_acid_count(&self, amino_acid: &'static AminoAcid) -> u8 {
        self.amino_acid_counts()[amino_acid.counts_idx()]
    }

    /// Returns how often the amino acid with the given one-letter `code` occurs in the sequence.
    fn amino_acid_count_by_code(&self, code: char) -> Result<u8, Error> {
        let amino_acid = AminoAcid::by_code(code)?;
        Ok(self.amino_acid_count(amino_acid))
    }

    /// Returns how often the amino acid with the given bit `code` occurs in the sequence.
    fn amino_acid_count_by_bit_code(&self, code: AminoAcidBitCode) -> u8 {
        let amino_acid = AminoAcid::by_bit_code(&code);
        self.amino_acid_count(amino_acid)
    }

    /// Returns the IDs of taxa in which this peptide occurs in exactly one protein (i.e. it
    /// uniquely identifies that taxon).
    fn unique_taxonomy_ids(&self) -> &[i32];

    /// Returns the IDs of taxa in which this peptide occurs in more than one protein.
    fn non_unique_taxonomy_ids(&self) -> &[i32];
}

/// A peptide as stored in and read from the database: an unmodified amino acid sequence plus
/// the protein/taxonomy associations and review-status flags accumulated during the build.
#[derive(Serialize)]
pub struct Peptide {
    partition: Option<i64>,
    mass: i64,
    sequence: Sequence,
    protein_ids: Arc<ProteinIds>,
    unique_taxonomy_ids: Arc<Vec<i32>>,
    non_unique_taxonomy_ids: Arc<Vec<i32>>,
    /// Bit flags for e.g. review status, see constants to see what is stored in which bit
    flags: i8,
    #[serde(skip)]
    amino_acid_counts: OnceLock<[u8; MAX_AMINO_ACID_BIT_CODE]>,
}

impl Peptide {
    /// Builds a new peptide from its sequence and the protein/taxonomy associations gathered
    /// during digestion. `mass` and the review-status flags are derived automatically.
    ///
    /// # Arguments
    /// * `sequence` - The peptide's amino acid sequence
    /// * `protein_ids` - IDs of the proteins the peptide was cleaved from
    /// * `unique_taxonomy_ids` - IDs of taxa in which the peptide occurs in exactly one protein
    /// * `non_unique_taxonomy_ids` - IDs of taxa in which the peptide occurs in more than one protein
    /// * `is_swiss_prot` - Whether at least one source protein is reviewed (SwissProt)
    /// * `is_trembl` - Whether at least one source protein is unreviewed (TrEMBL)
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
            protein_ids: Arc::new(protein_ids.into()),
            unique_taxonomy_ids: Arc::new(unique_taxonomy_ids),
            non_unique_taxonomy_ids: Arc::new(non_unique_taxonomy_ids),
            partition: None,
            flags,
            amino_acid_counts: OnceLock::new(),
        }
    }

    /// Build a new peptide with full control over each attribute
    ///
    /// # Arguments
    /// * `partition` - Mass partition
    /// * `mass` - Mass
    /// * `sequence` - The peptide's amino acid sequence
    /// * `protein_ids` - IDs of the proteins the peptide was cleaved from
    /// * `unique_taxonomy_ids` - IDs of taxa in which the peptide occurs in exactly one protein
    /// * `non_unique_taxonomy_ids` - IDs of taxa in which the peptide occurs in more than one protein
    /// * `flags` - Bit flags for e.g. review status, see constants to see what is stored in which bit
    /// * `amino_acid_counts` - Amino acid counts
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn full_new(
        partition: Option<i64>,
        mass: i64,
        sequence: Sequence,
        protein_ids: ProteinIds,
        unique_taxonomy_ids: Vec<i32>,
        non_unique_taxonomy_ids: Vec<i32>,
        flags: i8,
        amino_acid_counts: OnceLock<[u8; MAX_AMINO_ACID_BIT_CODE]>,
    ) -> Self {
        Self {
            partition,
            mass,
            sequence,
            flags,
            amino_acid_counts,
            protein_ids: Arc::new(protein_ids),
            unique_taxonomy_ids: Arc::new(unique_taxonomy_ids),
            non_unique_taxonomy_ids: Arc::new(non_unique_taxonomy_ids),
        }
    }

    /// Returns the mass partition the peptide was assigned to, or `None` if not yet persisted.
    pub fn partition(&self) -> Option<i64> {
        self.partition
    }

    pub(crate) fn set_partition(&mut self, partition: i64) {
        self.partition = Some(partition);
    }

    /// Returns the review-status bit flags.
    pub fn flags(&self) -> i8 {
        self.flags
    }

    /// Returns a reference to the review-status bit flags.
    pub fn flags_as_ref(&self) -> &i8 {
        &self.flags
    }

    /// Returns the sequence length in amino acids.
    pub fn len(&self) -> usize {
        self.sequence.len()
    }

    /// Returns whether the sequence is empty.
    pub fn is_empty(&self) -> bool {
        self.sequence.is_empty()
    }

    /// Consumes the peptide and returns its sequence.
    pub fn into_sequence(self) -> Sequence {
        self.sequence
    }

    /// Computes a peptide's mono-isotopic mass (water plus the mass of every residue) from its
    /// sequence.
    pub fn to_peptide_mass(sequence: &Sequence) -> i64 {
        sequence
            .amino_acids()
            .fold(WATER_MONO_MASS, |acc, amino_acid| {
                acc + amino_acid.mono_mass()
            })
    }

    /// Computes a peptide's mono-isotopic mass directly from an iterator of amino acid bit
    /// codes, without building a `Sequence` first.
    pub fn peptide_mass_from_amino_acid_bits<'a>(
        amino_acids: impl Iterator<Item = &'a AminoAcidBitCode>,
    ) -> i64 {
        amino_acids.fold(WATER_MONO_MASS, |acc, bit_code| {
            acc + AminoAcid::by_bit_code(bit_code).mono_mass()
        })
    }

    /// Returns the IDs of taxa in which this peptide occurs in exactly one protein.
    pub fn unique_taxonomy_ids(&self) -> &[i32] {
        &self.unique_taxonomy_ids
    }

    /// Returns the IDs of taxa in which this peptide occurs in more than one protein.
    pub fn non_unique_taxonomy_ids(&self) -> &[i32] {
        &self.non_unique_taxonomy_ids
    }

    /// Returns the IDs of the proteins this peptide was cleaved from.
    pub fn protein_ids(&self) -> &ProteinIds {
        &self.protein_ids
    }

    /// Returns a cheaply-clonable handle to the protein IDs (refcount bump, no deep copy) —
    /// use this instead of `protein_ids().clone()` when building many owners of the same data.
    pub(crate) fn protein_ids_arc(&self) -> Arc<ProteinIds> {
        Arc::clone(&self.protein_ids)
    }

    /// Returns a cheaply-clonable handle to the unique taxonomy IDs (refcount bump, no deep copy).
    pub(crate) fn unique_taxonomy_ids_arc(&self) -> Arc<Vec<i32>> {
        Arc::clone(&self.unique_taxonomy_ids)
    }

    /// Returns a cheaply-clonable handle to the non-unique taxonomy IDs (refcount bump, no deep copy).
    pub(crate) fn non_unique_taxonomy_ids_arc(&self) -> Arc<Vec<i32>> {
        Arc::clone(&self.non_unique_taxonomy_ids)
    }

    /// Estimates the peptide's serialized row size in bytes; used to bound in-memory buffer
    /// size before flushing a partition (see `PeptideTable::insert_batch`).
    pub fn cql_size(&self) -> usize {
        const ROW_OVERHEAD: usize = 32;
        // A non-frozen list<int> stores each element as its own cell.
        const TAX_LIST_CELL_SIZE: usize = 16 + 8 + std::mem::size_of::<i32>();
        // A non-frozen list<tinyint> stores each element as its own cell.
        const COUNTS_BLOB_OVERHEAD: usize = 12;
        // per-row overhead + partition + mass + sequence (clustering blob)
        // + metadata_id reference (i64) + taxonomy list cells (i32)
        ROW_OVERHEAD
            + std::mem::size_of::<i64>()
            + std::mem::size_of::<i64>()
            + self.sequence.cql_size()
            + std::mem::size_of::<i64>()
            + (self.unique_taxonomy_ids.len() + self.non_unique_taxonomy_ids.len())
                * TAX_LIST_CELL_SIZE
            + std::mem::size_of::<i64>()
            + MAX_AMINO_ACID_BIT_CODE * std::mem::size_of::<i8>()
            + COUNTS_BLOB_OVERHEAD
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
            self.protein_ids()
                .as_slice()
                .iter()
                .map(|id| format!("{id}"))
                .join(", ")
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

    fn unique_taxonomy_ids(&self) -> &[i32] {
        &self.unique_taxonomy_ids
    }

    fn non_unique_taxonomy_ids(&self) -> &[i32] {
        &self.non_unique_taxonomy_ids
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

/// A peptide with (possibly) applied post-translational modifications, e.g. a search result
/// or the ProForma-rendered form of a [`Peptide`].
#[derive(Serialize)]
pub struct Peptidoform {
    sequence: ModifiedSequence,
    mass: i64,
    protein_ids: Arc<ProteinIds>,
    unique_taxonomy_ids: Arc<Vec<i32>>,
    non_unique_taxonomy_ids: Arc<Vec<i32>>,
    /// Like peptides
    flags: i8,
    #[serde(skip)]
    amino_acid_counts: OnceLock<[u8; MAX_AMINO_ACID_BIT_CODE]>,
}

impl Peptidoform {
    /// Builds a new peptidoform from an already-modified sequence, its precomputed mass, and
    /// the protein/taxonomy associations carried over from the originating peptide.
    ///
    /// # Arguments
    /// * `sequence` - The (possibly PTM-modified) sequence
    /// * `mass` - The peptidoform's mono-isotopic mass, including any modifications
    /// * `protein_ids` - IDs of the proteins the peptidoform was cleaved from
    /// * `unique_taxonomy_ids` - IDs of taxa in which the peptide occurs in exactly one protein
    /// * `non_unique_taxonomy_ids` - IDs of taxa in which the peptide occurs in more than one protein
    /// * `is_swiss_prot` - Whether at least one source protein is reviewed (SwissProt)
    /// * `is_trembl` - Whether at least one source protein is unreviewed (TrEMBL)
    pub fn new(
        sequence: ModifiedSequence,
        mass: i64,
        protein_ids: Arc<ProteinIds>,
        unique_taxonomy_ids: Arc<Vec<i32>>,
        non_unique_taxonomy_ids: Arc<Vec<i32>>,
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
            protein_ids,
            unique_taxonomy_ids,
            non_unique_taxonomy_ids,
            flags,
            amino_acid_counts: OnceLock::new(),
        }
    }

    /// Returns the peptidoform's (possibly modified) sequence.
    pub fn sequence(&self) -> &ModifiedSequence {
        &self.sequence
    }

    /// Returns the peptidoform's mono-isotopic mass, including any modifications.
    pub fn mass(&self) -> i64 {
        self.mass
    }

    /// Returns the per-amino-acid occurrence counts, indexed by amino acid bit code.
    pub fn amino_acid_counts(&self) -> &[u8; MAX_AMINO_ACID_BIT_CODE] {
        self.amino_acid_counts.get_or_init(|| {
            let mut counts = [0; MAX_AMINO_ACID_BIT_CODE];

            self.sequence
                .amino_acid_bit_codes()
                .for_each(|bit_code| counts[bit_code.as_bytes()[0] as usize] += 1);
            counts
        })
    }

    /// Returns how often `amino_acid` occurs in the sequence.
    pub fn amino_acid_count(&self, amino_acid: &'static AminoAcid) -> u8 {
        let idx = (amino_acid.code() as u8 - b'A') as usize;
        self.amino_acid_counts()[idx]
    }

    /// Returns how often the amino acid with the given bit `code` occurs in the sequence.
    pub fn amino_acid_count_by_bit_code(&self, code: AminoAcidBitCode) -> u8 {
        let amino_acid = AminoAcid::by_bit_code(&code);
        self.amino_acid_count(amino_acid)
    }

    fn protein_ids(&self) -> &ProteinIds {
        &self.protein_ids
    }
}

impl From<Peptide> for Peptidoform {
    fn from(peptide: Peptide) -> Self {
        Self {
            mass: peptide.mass(),
            flags: peptide.flags(),
            protein_ids: peptide.protein_ids,
            unique_taxonomy_ids: peptide.unique_taxonomy_ids,
            non_unique_taxonomy_ids: peptide.non_unique_taxonomy_ids,
            sequence: peptide.sequence.into(),
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
                .amino_acid_bit_codes()
                .for_each(|bit_code| counts[bit_code.as_bytes()[0] as usize] += 1);
            counts
        })
    }

    fn unique_taxonomy_ids(&self) -> &[i32] {
        &self.unique_taxonomy_ids
    }

    fn non_unique_taxonomy_ids(&self) -> &[i32] {
        &self.non_unique_taxonomy_ids
    }
}

impl From<&Peptide> for PeptideResponse {
    fn from(peptide: &Peptide) -> Self {
        Self {
            partition: peptide.partition,
            mass: crate::mass::to_float(peptide.mass),
            sequence: peptide.sequence.to_string(),
            protein_ids: peptide.protein_ids.as_slice().to_vec(),
            unique_taxonomy_ids: peptide.unique_taxonomy_ids.to_vec(),
            non_unique_taxonomy_ids: peptide.non_unique_taxonomy_ids.to_vec(),
            is_swiss_prot: peptide.is_swiss_prot(),
            is_trembl: peptide.is_trembl(),
            proteins: None,
        }
    }
}

impl From<&Peptidoform> for PeptideResponse {
    fn from(peptidoform: &Peptidoform) -> Self {
        Self {
            partition: None,
            mass: crate::mass::to_float(peptidoform.mass),
            sequence: peptidoform.sequence.to_string(),
            protein_ids: peptidoform.protein_ids().as_vec(),
            unique_taxonomy_ids: peptidoform.unique_taxonomy_ids().to_vec(),
            non_unique_taxonomy_ids: peptidoform.non_unique_taxonomy_ids().to_vec(),
            is_swiss_prot: peptidoform.is_swiss_prot(),
            is_trembl: peptidoform.is_trembl(),
            proteins: None,
        }
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
        // + 26 * 8 + 12 = amino_acid_counts blob (26 amino acids x 8 bits + 12 bytes overhead)
        assert_eq!(peptide.cql_size(), 274);
    }
}
