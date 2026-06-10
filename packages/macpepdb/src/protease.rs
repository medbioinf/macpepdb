use std::{
    fmt::{Debug, Display},
    num::NonZeroUsize,
    ops::RangeInclusive,
};

use fallible_iterator::FallibleIterator;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zerocopy::IntoBytes;

use crate::{
    amino_acid::{ARGININE, AminoAcidBitCode, LYSINE, PROLINE, UNKNOWN},
    peptide::Peptide,
    sequence::{IsBitSequence, PeptideSequence as Sequence},
};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Protease creation failed: {0}")]
    FailedCreation(String),
    #[error("Max peptide length must be equal to or smaller than {expected} but is {0}", expected = Sequence::MAX_LENGTH.get())]
    MaxLengthTooLarge(usize),
    #[error("Min peptide length must be equal to or greater than {expected} but is {0}", expected = Sequence::MIN_LENGTH.get())]
    MinLengthTooSmall(usize),
    #[error("Peptide error in protease: {0}")]
    Peptide(#[from] crate::peptide::Error),
    #[error("Unable to get partition for mass: {0}")]
    UnableToGetPartition(String),
    #[error("Sequence error in protease: {0}")]
    Sequence(#[from] crate::sequence::Error),
    #[error("Unknown amino acid encountered: {0}")]
    UnknownAminoAcid(String),
    #[error("Unknown protease `{0}`")]
    UnknownProtease(String),
}

/// Trait defining the behavior for a protease
///
pub trait IsProtease: Send + Sync {
    /// Returns the name of the enzyme
    fn name(&self) -> &'static str;

    /// Returns the sequence digested with zero missed cleavages
    ///
    /// # Arguments
    /// * `sequence` - Amino acid sequence
    ///
    fn full_digest<'a>(&self, sequence: &'a [AminoAcidBitCode]) -> Vec<&'a [AminoAcidBitCode]>;

    /// Count missed cleavages
    ///
    fn count_missed_cleavages(&self, sequence: &[AminoAcidBitCode]) -> usize;
}

pub struct Trypsin;

impl Trypsin {
    pub const NAME: &'static str = "trypsin";
}

impl IsProtease for Trypsin {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn full_digest<'a>(&self, sequence: &'a [AminoAcidBitCode]) -> Vec<&'a [AminoAcidBitCode]> {
        let lysine_byte: u8 = LYSINE.bit_code().as_bytes()[0];
        let arginine_byte: u8 = ARGININE.bit_code().as_bytes()[0];
        let proline_byte: u8 = PROLINE.bit_code().as_bytes()[0];

        let mut last_cleavage_pos: usize = 0;
        memchr::memchr2_iter(lysine_byte, arginine_byte, sequence.as_bytes())
            .map(|pos| {
                (
                    pos + 1,
                    sequence.get(pos + 1).map(|bit_code| bit_code.as_bytes()[0]),
                )
            })
            .filter_map(|(pos, next_aa)| {
                if let Some(next_aa) = next_aa
                    && next_aa == proline_byte
                {
                    None
                } else {
                    Some(pos)
                }
            })
            .chain(std::iter::once(sequence.len()))
            .sorted()
            .map(|pos| {
                let start = last_cleavage_pos;
                last_cleavage_pos = pos;
                &sequence[start..pos]
            })
            .collect()
    }

    fn count_missed_cleavages(&self, sequence: &[AminoAcidBitCode]) -> usize {
        let lysine_byte: u8 = LYSINE.bit_code().as_bytes()[0];
        let arginine_byte: u8 = ARGININE.bit_code().as_bytes()[0];
        let proline_byte: u8 = PROLINE.bit_code().as_bytes()[0];

        memchr::memchr2_iter(
            lysine_byte,
            arginine_byte,
            sequence
                .iter()
                .map(|bit_code| bit_code.as_bytes()[0])
                .collect::<Vec<u8>>()
                .as_slice(),
        )
        .map(|pos| sequence.get(pos + 1).map(|bit_code| bit_code.as_bytes()[0]))
        .filter_map(|next_aa| {
            if let Some(next_aa) = next_aa
                && next_aa == proline_byte
            {
                None
            } else {
                Some(())
            }
        })
        .count()
    }
}

pub struct Unspecific;

impl Unspecific {
    pub const NAME: &'static str = "unspecific";
}

impl IsProtease for Unspecific {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn full_digest<'a>(&self, sequence: &'a [AminoAcidBitCode]) -> Vec<&'a [AminoAcidBitCode]> {
        (0..sequence.len())
            .map(|pos| &sequence[pos..(pos + 1)])
            .collect()
    }

    fn count_missed_cleavages(&self, sequence: &[AminoAcidBitCode]) -> usize {
        sequence.len()
    }
}

pub struct MissedCleavageIterator<
    'a,
    T: Sized,
    F: Fn(&[&[AminoAcidBitCode]]) -> Result<Option<T>, Error>,
> {
    min_length: NonZeroUsize,
    max_length: NonZeroUsize,
    full_digest: Vec<&'a [AminoAcidBitCode]>,
    max_window_size: usize,
    mass_range: Option<RangeInclusive<i64>>,
    prefix_len: Vec<usize>,
    has_unknown: Vec<bool>,
    start: usize,
    window_size: usize,
    conversion_fn: F,
}

impl<'a, T, F> MissedCleavageIterator<'a, T, F>
where
    T: Sized,
    F: Fn(&[&[AminoAcidBitCode]]) -> Result<Option<T>, Error>,
{
    pub fn new(
        min_length: NonZeroUsize,
        max_length: NonZeroUsize,
        max_missed_cleavages: usize,
        keep_unknown: bool,
        mass_range: Option<RangeInclusive<i64>>,
        full_digest: Vec<&'a [AminoAcidBitCode]>,
        conversion_fn: F,
    ) -> Self {
        let max_window_size = max_missed_cleavages + 1;
        let mut prefix_len = Vec::with_capacity(full_digest.len());
        let mut acc = 0usize;

        for seq in &full_digest {
            acc += seq.len();
            prefix_len.push(acc);
        }

        let mut has_unknown = Vec::with_capacity(full_digest.len());

        for seq in &full_digest {
            has_unknown.push(
                !keep_unknown
                    && memchr::memchr(UNKNOWN.bit_code().as_bytes()[0], seq.as_bytes()).is_some(),
            );
        }

        Self {
            min_length,
            max_length,
            mass_range,
            full_digest,
            max_window_size,
            prefix_len,
            has_unknown,
            conversion_fn,
            start: 0,
            window_size: 1,
        }
    }

    fn continue_window_size(&mut self) -> Result<Option<T>, Error> {
        self.window_size += 1;
        self.next()
    }

    fn break_window_size(&mut self) -> Result<Option<T>, Error> {
        self.start += 1;
        self.window_size = 1;
        self.next()
    }
}

impl<'a, T, F> FallibleIterator for MissedCleavageIterator<'a, T, F>
where
    T: Sized,
    F: Fn(&[&[AminoAcidBitCode]]) -> Result<Option<T>, Error>,
{
    type Item = T;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if self.start >= self.full_digest.len() {
            return Ok(None);
        }

        if self.window_size > self.max_window_size {
            return self.break_window_size();
        }

        let end = (self.start + self.window_size).min(self.full_digest.len());

        if self.start >= end {
            return self.continue_window_size();
        }

        let total_len = if self.start == 0 {
            self.prefix_len[end - 1]
        } else {
            self.prefix_len[end - 1] - self.prefix_len[self.start - 1]
        };

        if total_len > self.max_length.get() {
            return self.break_window_size();
        }

        if total_len < self.min_length.get() {
            return self.continue_window_size();
        }

        if self.has_unknown[self.start..end].iter().any(|&x| x) {
            return self.continue_window_size();
        }

        if let Some(mass_range) = self.mass_range.as_ref() {
            let mass = Peptide::peptide_mass_from_amino_acid_bits(
                self.full_digest[self.start..end]
                    .iter()
                    .flat_map(|sequences| sequences.iter()),
            );

            if !mass_range.contains(&mass) {
                return self.continue_window_size();
            }
        }

        self.window_size += 1;

        (self.conversion_fn)(&self.full_digest[self.start..end])
    }
}

#[derive(Deserialize, Serialize)]
pub struct Protease {
    #[serde(with = "is_protease_serde")]
    inner: Box<dyn IsProtease>,
    min_length: NonZeroUsize,
    max_length: NonZeroUsize,
    max_missed_cleavages: usize,
    keep_unknown: bool,
}

impl Protease {
    /// Cleaves a protein into peptides and returns a iterator over the peptides
    ///
    /// # Arguments
    /// * `sequence` - Amino acid sequence
    ///
    pub fn cleave<'a>(
        &'a self,
        sequence: &'a [AminoAcidBitCode],
        mass_range: Option<RangeInclusive<i64>>,
    ) -> impl FallibleIterator<Item = Peptide, Error = Error> + 'a {
        let full_digest = self.inner.full_digest(sequence);
        MissedCleavageIterator::new(
            self.min_length,
            self.max_length,
            self.max_missed_cleavages,
            self.keep_unknown,
            mass_range,
            full_digest,
            |raw_seq| {
                Ok(Some(Peptide::new(
                    Sequence::try_from(raw_seq)?,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    false,
                    false,
                )))
            },
        )
    }

    pub(crate) fn cleave_masses_only<'a>(
        &'a self,
        sequence: &'a [AminoAcidBitCode],
    ) -> impl FallibleIterator<Item = i64, Error = Error> + 'a {
        let full_digest = self.inner.full_digest(sequence);
        MissedCleavageIterator::new(
            self.min_length,
            self.max_length,
            self.max_missed_cleavages,
            self.keep_unknown,
            None,
            full_digest,
            |raw_seq| {
                Ok(Some(Peptide::peptide_mass_from_amino_acid_bits(
                    raw_seq.iter().flat_map(|sequences| sequences.iter()),
                )))
            },
        )
    }

    pub fn by_name(
        name: &str,
        min_length: Option<NonZeroUsize>,
        max_length: Option<NonZeroUsize>,
        max_missed_cleavages: Option<usize>,
        keep_unknown: bool,
    ) -> Result<Self, Error> {
        let min_length = min_length.unwrap_or(Sequence::MIN_LENGTH);
        if min_length.get() < Sequence::MIN_LENGTH.get() {
            return Err(Error::MinLengthTooSmall(min_length.get()));
        }
        let max_length = max_length.unwrap_or(Sequence::MAX_LENGTH);
        if max_length.get() > Sequence::MAX_LENGTH.get() {
            return Err(Error::MaxLengthTooLarge(max_length.get()));
        }
        // worst case each full digested peptide is only one amino acid long (e.g. when unspecifically cleaved)
        // a peptided can only contain as many missed cleavages as there a are amino acids allowed
        let max_missed_cleavages = max_missed_cleavages.unwrap_or(max_length.get());

        let inner = Self::inner_by_name(name)?;

        Ok(Self {
            min_length,
            max_length,
            max_missed_cleavages,
            keep_unknown,
            inner,
        })
    }

    fn inner_by_name(name: &str) -> Result<Box<dyn IsProtease>, Error> {
        match name.to_lowercase().as_str() {
            Trypsin::NAME => Ok(Box::new(Trypsin {})),
            Unspecific::NAME => Ok(Box::new(Unspecific {})),
            _ => Err(Error::UnknownProtease(name.to_string())),
        }
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn min_length(&self) -> NonZeroUsize {
        self.min_length
    }

    pub fn max_length(&self) -> NonZeroUsize {
        self.max_length
    }

    pub fn max_missed_cleavages(&self) -> usize {
        self.max_missed_cleavages
    }
}

impl Clone for Protease {
    fn clone(&self) -> Self {
        Self::by_name(
            self.name(),
            Some(self.min_length),
            Some(self.max_length),
            Some(self.max_missed_cleavages),
            self.keep_unknown,
        )
        .unwrap()
    }
}

impl Debug for Protease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Protease {{ name: {}, min_length: {:?}, max_length: {:?}, max_missed_cleavages: {:?}, keep_unknown: {} }}",
            self.name(),
            self.min_length(),
            self.max_length(),
            self.max_missed_cleavages(),
            self.keep_unknown
        )
    }
}

impl Display for Protease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "name: {}, peptide length {} - {}, max. missed_cleavages: {}, keep unknown: {}",
            self.name(),
            self.min_length,
            self.max_length,
            self.max_missed_cleavages,
            self.keep_unknown
        )
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

mod is_protease_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[allow(clippy::borrowed_box)]
    pub fn serialize<S>(protease: &Box<dyn IsProtease>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        protease.name().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Box<dyn IsProtease>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let name = String::deserialize(deserializer)?;
        Protease::inner_by_name(&name).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::sequence::ProteinSequence;

    use super::*;

    #[test]
    fn test_trypsin() {
        let leptin = ProteinSequence::try_from(
            "MHWGTLCGFLWLWPYLFYVQAVPIQKVQDDTKTLIKTIVTRINDISHTQSVSSKQKVTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSRNVIQISNDLENLRDLLHVLAFSKSCHLPWASGLETLDSLGGVLEASGYSTEVVALSRLQGSLQDMLWQLDLSPGC",
        ).unwrap();

        let expected_pepts_file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("leptin.tryptic.6-50.2-missed-cleavages.txt");

        let expected_peps: HashSet<Sequence> = std::fs::read_to_string(expected_pepts_file_path)
            .unwrap()
            .split("\n")
            .map(|line| line.trim())
            .filter(|line| !line.is_empty())
            .map(|line| Sequence::try_from(line).unwrap())
            .collect();

        let trypsin = Protease::by_name(
            "trypsin",
            Some(NonZeroUsize::new(6).unwrap()),
            Some(NonZeroUsize::new(50).unwrap()),
            Some(2),
            false,
        )
        .unwrap();

        let peps = trypsin
            .cleave(leptin.as_ref(), None)
            .map(|peptide| Ok(peptide.into_sequence()))
            .collect::<HashSet<Sequence>>()
            .unwrap();

        assert_eq!(peps.len(), expected_peps.len());
        assert_eq!(peps, expected_peps);
    }

    #[test]
    fn test_unspecific() {
        let leptin = ProteinSequence::try_from(
            "MHWGTLCGFLWLWPYLFYVQAVPIQKVQDDTKTLIKTIVTRINDISHTQSVSSKQKVTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSRNVIQISNDLENLRDLLHVLAFSKSCHLPWASGLETLDSLGGVLEASGYSTEVVALSRLQGSLQDMLWQLDLSPGC",
        ).unwrap();

        let unspecific = Protease::by_name(
            "unspecific",
            Some(NonZeroUsize::new(6).unwrap()),
            Some(NonZeroUsize::new(50).unwrap()),
            None,
            false,
        )
        .unwrap();

        let expected_pepts_file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("leptin.unspecific.6-50.txt");

        let expected_peps: HashSet<Sequence> = std::fs::read_to_string(expected_pepts_file_path)
            .unwrap()
            .split("\n")
            .map(|line| line.trim())
            .filter(|line| !line.is_empty())
            .map(|line| Sequence::try_from(line).unwrap())
            .collect();

        let peps = unspecific
            .cleave(leptin.as_ref(), None)
            .map(|peptide| Ok(peptide.into_sequence()))
            .collect::<HashSet<Sequence>>()
            .unwrap();

        assert_eq!(peps.len(), expected_peps.len());
        assert_eq!(peps, expected_peps);
    }
}
