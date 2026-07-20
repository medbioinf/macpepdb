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
    amino_acid::{ARGININE, AminoAcid, AminoAcidBitCode, LYSINE, PROLINE, UNKNOWN},
    molecules::WATER_MONO_MASS,
    peptide::Peptide,
    sequence::{IsBitSequence, PeptideSequence as Sequence},
};

/// Errors which might occur while creating or applying a protease
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

/// Trypsin, cleaving after lysine (K) or arginine (R) unless followed by proline (P).
pub struct Trypsin;

impl Trypsin {
    /// The name used to look up this protease by, e.g. via `Protease::by_name`.
    pub const NAME: &'static str = "trypsin";
    /// The name used to look up the semi-specific variant of this protease by.
    pub const SEMI_NAME: &'static str = "semi-trypsin";
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

/// A protease with no cleavage specificity: every position between two residues is a
/// potential cleavage site.
pub struct Unspecific;

impl Unspecific {
    /// The name used to look up this protease by, e.g. via `Protease::by_name`.
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

/// Iterator over fully-specific peptides generated from a protease's full digest, sliding a
/// window of consecutive fragments (0..=`max_missed_cleavages` of them) over the digest and
/// filtering by length, unknown-residue content and mass range using precomputed prefix sums.
pub struct MissedCleavageIterator<
    'a,
    T: Sized,
    F: Fn(&[&[AminoAcidBitCode]], i64) -> Result<Option<T>, Error>,
> {
    min_length: NonZeroUsize,
    max_length: NonZeroUsize,
    full_digest: Vec<&'a [AminoAcidBitCode]>,
    max_window_size: usize,
    mass_range: Option<RangeInclusive<i64>>,
    prefix_len: Vec<usize>,
    prefix_mass: Vec<i64>,
    unknown_prefix: Vec<usize>,
    start: usize,
    window_size: usize,
    conversion_fn: F,
}

impl<'a, T, F> MissedCleavageIterator<'a, T, F>
where
    T: Sized,
    F: Fn(&[&[AminoAcidBitCode]], i64) -> Result<Option<T>, Error>,
{
    /// Creates a new iterator, precomputing prefix sums (length, mass, unknown-residue count)
    /// over `full_digest` so each window's totals can be derived in O(1).
    ///
    /// # Arguments
    /// * `keep_unknown` - Whether fragments containing an unknown residue are kept
    /// * `mass_range` - If given, only windows whose mass falls in this range are yielded
    /// * `full_digest` - The protease's zero-missed-cleavage digest of the sequence
    /// * `conversion_fn` - Turns an accepted window of fragments (and its mass) into `T`,
    ///   or returns `Ok(None)` to skip it without stopping iteration
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
        let mut prefix_mass = Vec::with_capacity(full_digest.len());
        let mut unknown_prefix = Vec::with_capacity(full_digest.len());
        let mut acc_len = 0usize;
        let mut acc_mass = 0i64;
        let mut acc_unknown = 0usize;
        let unknown_byte = UNKNOWN.bit_code().as_bytes()[0];

        for seq in &full_digest {
            acc_len += seq.len();
            prefix_len.push(acc_len);

            acc_mass += seq
                .iter()
                .map(|bit_code| AminoAcid::by_bit_code(bit_code).mono_mass())
                .sum::<i64>();
            prefix_mass.push(acc_mass);

            if !keep_unknown && memchr::memchr(unknown_byte, seq.as_bytes()).is_some() {
                acc_unknown += 1;
            }
            unknown_prefix.push(acc_unknown);
        }

        Self {
            min_length,
            max_length,
            mass_range,
            full_digest,
            max_window_size,
            prefix_len,
            prefix_mass,
            unknown_prefix,
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
    F: Fn(&[&[AminoAcidBitCode]], i64) -> Result<Option<T>, Error>,
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

        let unknown_count = if self.start == 0 {
            self.unknown_prefix[end - 1]
        } else {
            self.unknown_prefix[end - 1] - self.unknown_prefix[self.start - 1]
        };

        if unknown_count > 0 {
            return self.continue_window_size();
        }

        let mass = WATER_MONO_MASS
            + if self.start == 0 {
                self.prefix_mass[end - 1]
            } else {
                self.prefix_mass[end - 1] - self.prefix_mass[self.start - 1]
            };

        if let Some(mass_range) = self.mass_range.as_ref()
            && !mass_range.contains(&mass)
        {
            return self.continue_window_size();
        }

        self.window_size += 1;

        (self.conversion_fn)(&self.full_digest[self.start..end], mass)
    }
}

/// Iterator for semi-specific digestion.
///
/// For each tryptic window (considering missed cleavages), generates:
/// - Left-anchored sub-sequences T[0..j] (N-terminal tryptic terminus)
/// - Right-anchored sub-sequences T[i..L] (C-terminal tryptic terminus)
///
/// This matches the proteomics definition of semi-specific: every yielded peptide has
/// at least one tryptic terminus.
pub struct SemiSpecificIterator<
    'a,
    T: Sized,
    F: Fn(&[AminoAcidBitCode], i64) -> Result<Option<T>, Error>,
> {
    min_length: NonZeroUsize,
    max_length: NonZeroUsize,
    keep_unknown: bool,
    full_digest: Vec<&'a [AminoAcidBitCode]>,
    max_window_size: usize,
    mass_range: Option<RangeInclusive<i64>>,
    conversion_fn: F,
    // Outer: tryptic window state
    tryptic_start: usize,
    tryptic_window: usize, // 0 = not yet initialized
    // Inner: sub-sequence state within the merged tryptic peptide
    current_merged: Vec<AminoAcidBitCode>,
    // Prefix sums over `current_merged` (length merged_len + 1, [0] = 0): cumulative residue
    // mass and count of unknown residues, so any ragged sub-window T[i..j] has O(1) mass
    // (`prefix_mass[j] - prefix_mass[i]`) and O(1) unknown count.
    prefix_mass: Vec<i64>,
    unknown_prefix: Vec<usize>,
    // left_done = false: generating left-anchored (N-term tryptic) sub-seqs, sub_len varies
    // left_done = true:  generating right-anchored (C-term tryptic) sub-seqs, sub_start varies
    left_done: bool,
    sub_start: usize, // used in right mode
    sub_len: usize,   // used in left mode
}

impl<'a, T, F> SemiSpecificIterator<'a, T, F>
where
    T: Sized,
    F: Fn(&[AminoAcidBitCode], i64) -> Result<Option<T>, Error>,
{
    /// Creates a new iterator over the semi-specific sub-sequences of `full_digest`.
    ///
    /// # Arguments
    /// * `keep_unknown` - Whether sub-sequences containing an unknown residue are kept
    /// * `mass_range` - If given, only sub-sequences whose mass falls in this range are yielded
    /// * `full_digest` - The protease's zero-missed-cleavage digest of the sequence
    /// * `conversion_fn` - Turns an accepted sub-sequence (and its mass) into `T`, or returns
    ///   `Ok(None)` to skip it without stopping iteration
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
        Self {
            min_length,
            max_length,
            keep_unknown,
            full_digest,
            max_window_size,
            mass_range,
            conversion_fn,
            tryptic_start: 0,
            tryptic_window: 0,
            current_merged: Vec::new(),
            prefix_mass: Vec::new(),
            unknown_prefix: Vec::new(),
            left_done: false,
            sub_start: 0,
            sub_len: 0,
        }
    }

    /// Returns true if all sub-sequences for current_merged have been emitted.
    fn is_sub_exhausted(&self) -> bool {
        // In right mode, exhausted when sub_start is beyond the last valid position.
        self.left_done && self.sub_start + self.min_length.get() > self.current_merged.len()
    }

    /// Advance the sub-sequence cursor within the current tryptic window.
    ///
    /// Left mode: increment sub_len; when exhausted switch to right mode.
    /// Right mode: increment sub_start.
    fn advance_sub(&mut self) {
        let merged_len = self.current_merged.len();
        if !self.left_done {
            self.sub_len += 1;
            if self.sub_len > self.max_length.get().min(merged_len) {
                // Switch to right mode. Skip sub_start=0 to avoid re-emitting the full fragment
                // that was already yielded in left mode (when merged_len <= max_length).
                self.left_done = true;
                self.sub_start = 1_usize.max(merged_len.saturating_sub(self.max_length.get()));
            }
        } else {
            self.sub_start += 1;
        }
    }

    /// Advance the outer tryptic window and populate current_merged / current_unknown.
    /// Returns false when all windows are exhausted.
    fn advance_tryptic(&mut self) -> bool {
        loop {
            if self.tryptic_window == 0 {
                self.tryptic_start = 0;
                self.tryptic_window = 1;
            } else {
                self.tryptic_window += 1;
                if self.tryptic_window > self.max_window_size
                    || self.tryptic_start + self.tryptic_window > self.full_digest.len()
                {
                    self.tryptic_start += 1;
                    self.tryptic_window = 1;
                }
            }

            if self.tryptic_start >= self.full_digest.len() {
                return false;
            }

            let end = self.tryptic_start + self.tryptic_window;
            self.current_merged = self.full_digest[self.tryptic_start..end]
                .iter()
                .flat_map(|s| s.iter().copied())
                .collect();

            let unknown_byte = UNKNOWN.bit_code().as_bytes()[0];
            self.prefix_mass.clear();
            self.unknown_prefix.clear();
            self.prefix_mass.push(0);
            self.unknown_prefix.push(0);
            let mut acc_mass = 0i64;
            let mut acc_unknown = 0usize;
            for aa in &self.current_merged {
                acc_mass += AminoAcid::by_bit_code(aa).mono_mass();
                self.prefix_mass.push(acc_mass);
                if !self.keep_unknown && aa.as_bytes()[0] == unknown_byte {
                    acc_unknown += 1;
                }
                self.unknown_prefix.push(acc_unknown);
            }

            // Reset to left mode, starting at min_length
            self.left_done = false;
            self.sub_start = 0;
            self.sub_len = self.min_length.get();

            if self.current_merged.len() >= self.min_length.get() {
                return true;
            }
        }
    }
}

impl<'a, T, F> FallibleIterator for SemiSpecificIterator<'a, T, F>
where
    T: Sized,
    F: Fn(&[AminoAcidBitCode], i64) -> Result<Option<T>, Error>,
{
    type Item = T;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            if (self.tryptic_window == 0 || self.is_sub_exhausted()) && !self.advance_tryptic() {
                return Ok(None);
            }

            let merged_len = self.current_merged.len();
            let (sub_start, sub_end) = if !self.left_done {
                (0, self.sub_len) // Left-anchored: T[0..sub_len]
            } else {
                (self.sub_start, merged_len) // Right-anchored: T[sub_start..L]
            };

            if self.unknown_prefix[sub_end] - self.unknown_prefix[sub_start] > 0 {
                self.advance_sub();
                continue;
            }

            let mass = WATER_MONO_MASS + self.prefix_mass[sub_end] - self.prefix_mass[sub_start];

            if let Some(ref mass_range) = self.mass_range
                && !mass_range.contains(&mass)
            {
                self.advance_sub();
                continue;
            }

            let sub_seq = &self.current_merged[sub_start..sub_end];
            let result = (self.conversion_fn)(sub_seq, mass)?;
            self.advance_sub();
            if result.is_some() {
                return Ok(result);
            }
        }
    }
}

/// A named cleavage rule (e.g. trypsin) combined with the length/missed-cleavage constraints
/// used to digest proteins into peptides. Persisted as part of the `Configuration` blob so a
/// build's digestion parameters can be recovered later.
#[derive(Deserialize, Serialize)]
pub struct Protease {
    #[serde(with = "is_protease_serde")]
    inner: Box<dyn IsProtease>,
    #[serde(default)]
    semi_specific: bool,
    min_length: NonZeroUsize,
    max_length: NonZeroUsize,
    max_missed_cleavages: usize,
    keep_unknown: bool,
}

impl Protease {
    /// Cleaves a protein into peptides and returns an iterator over the peptides.
    ///
    /// For semi-specific proteases, each tryptic window is sub-cleaved unspecifically.
    ///
    pub fn cleave<'a>(
        &'a self,
        sequence: &'a [AminoAcidBitCode],
        mass_range: Option<RangeInclusive<i64>>,
    ) -> Box<dyn FallibleIterator<Item = Peptide, Error = Error> + 'a> {
        let full_digest = self.inner.full_digest(sequence);
        if self.semi_specific {
            Box::new(SemiSpecificIterator::new(
                self.min_length,
                self.max_length,
                self.max_missed_cleavages,
                self.keep_unknown,
                mass_range,
                full_digest,
                |sub_seq: &[AminoAcidBitCode], _mass: i64| {
                    Ok(Some(Peptide::new(
                        Sequence::try_from([sub_seq].as_slice())?,
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                        false,
                        false,
                    )))
                },
            ))
        } else {
            Box::new(MissedCleavageIterator::new(
                self.min_length,
                self.max_length,
                self.max_missed_cleavages,
                self.keep_unknown,
                mass_range,
                full_digest,
                |raw_seq, _mass: i64| {
                    Ok(Some(Peptide::new(
                        Sequence::try_from(raw_seq)?,
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                        false,
                        false,
                    )))
                },
            ))
        }
    }

    pub(crate) fn cleave_masses_only<'a>(
        &'a self,
        sequence: &'a [AminoAcidBitCode],
    ) -> Box<dyn FallibleIterator<Item = i64, Error = Error> + 'a> {
        let full_digest = self.inner.full_digest(sequence);
        if self.semi_specific {
            Box::new(SemiSpecificIterator::new(
                self.min_length,
                self.max_length,
                self.max_missed_cleavages,
                self.keep_unknown,
                None,
                full_digest,
                |_sub_seq: &[AminoAcidBitCode], mass: i64| Ok(Some(mass)),
            ))
        } else {
            Box::new(MissedCleavageIterator::new(
                self.min_length,
                self.max_length,
                self.max_missed_cleavages,
                self.keep_unknown,
                None,
                full_digest,
                |_raw_seq, mass: i64| Ok(Some(mass)),
            ))
        }
    }

    /// Looks up a protease by name and applies the given length/missed-cleavage constraints,
    /// falling back to `Sequence::MIN_LENGTH`/`MAX_LENGTH` when not given. A `semi-` prefix
    /// (e.g. `semi-trypsin`) selects the semi-specific variant of the named protease.
    ///
    /// # Arguments
    /// * `name` - The protease name, optionally prefixed with `semi-`
    /// * `min_length` - Minimum peptide length (inclusive)
    /// * `max_length` - Maximum peptide length (inclusive)
    /// * `max_missed_cleavages` - Maximum number of missed cleavages
    /// * `keep_unknown` - Whether peptides containing unknown residues are kept
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

        let lower = name.to_lowercase();
        let (semi_specific, base_name) = if let Some(base) = lower.strip_prefix("semi-") {
            (true, base.to_string())
        } else {
            (false, lower)
        };

        let inner = Self::inner_by_name(&base_name)?;

        Ok(Self {
            min_length,
            max_length,
            max_missed_cleavages,
            keep_unknown,
            inner,
            semi_specific,
        })
    }

    fn inner_by_name(name: &str) -> Result<Box<dyn IsProtease>, Error> {
        match name.to_lowercase().as_str() {
            Trypsin::NAME => Ok(Box::new(Trypsin {})),
            Unspecific::NAME => Ok(Box::new(Unspecific {})),
            _ => Err(Error::UnknownProtease(name.to_string())),
        }
    }

    /// Returns the protease's name, prefixed with `semi-` if it is semi-specific.
    pub fn name(&self) -> String {
        if self.semi_specific {
            format!("semi-{}", self.inner.name())
        } else {
            self.inner.name().to_string()
        }
    }

    /// Returns the minimum peptide length.
    pub fn min_length(&self) -> NonZeroUsize {
        self.min_length
    }

    /// Returns the maximum peptide length.
    pub fn max_length(&self) -> NonZeroUsize {
        self.max_length
    }

    /// Returns the maximum number of missed cleavages.
    pub fn max_missed_cleavages(&self) -> usize {
        self.max_missed_cleavages
    }
}

impl From<&Protease> for macpepdb_web_common::responses::configuration::ProteaseResponse {
    fn from(protease: &Protease) -> Self {
        Self {
            name: protease.inner.name().to_string(),
            semi_specific: protease.semi_specific,
            min_length: protease.min_length.get(),
            max_length: protease.max_length.get(),
            max_missed_cleavages: protease.max_missed_cleavages,
            keep_unknown: protease.keep_unknown,
        }
    }
}

impl Clone for Protease {
    fn clone(&self) -> Self {
        Self::by_name(
            &self.name(),
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

/// Serde serialization/deserialization for `Box<dyn IsProtease>`, using the protease's name
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

    #[test]
    fn test_semi_trypsin() {
        let leptin = ProteinSequence::try_from(
            "MHWGTLCGFLWLWPYLFYVQAVPIQKVQDDTKTLIKTIVTRINDISHTQSVSSKQKVTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSRNVIQISNDLENLRDLLHVLAFSKSCHLPWASGLETLDSLGGVLEASGYSTEVVALSRLQGSLQDMLWQLDLSPGC",
        ).unwrap();

        let expected_pepts_file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("leptin.semi-tryptic.6-50.2-missed-cleavages.txt");

        let expected_peps: HashSet<Sequence> = std::fs::read_to_string(expected_pepts_file_path)
            .unwrap()
            .split("\n")
            .map(|line| line.trim())
            .filter(|line| !line.is_empty())
            .map(|line| Sequence::try_from(line).unwrap())
            .collect();

        let semi_trypsin = Protease::by_name(
            Trypsin::SEMI_NAME,
            Some(NonZeroUsize::new(6).unwrap()),
            Some(NonZeroUsize::new(50).unwrap()),
            Some(2),
            false,
        )
        .unwrap();

        let peps = semi_trypsin
            .cleave(leptin.as_ref(), None)
            .map(|peptide| Ok(peptide.into_sequence()))
            .collect::<HashSet<Sequence>>()
            .unwrap();

        assert_eq!(peps.len(), expected_peps.len());
        assert_eq!(peps, expected_peps);
    }
}
