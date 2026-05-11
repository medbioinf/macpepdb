use std::fmt::{Debug, Display};

use deku::DekuEnumExt;
use fallible_iterator::FallibleIterator;
use itertools::Itertools;
use thiserror::Error;

use crate::{
    amino_acid::{ARGININE, AminoAcidBitCode, LYSINE, PROLINE, UNKNOWN},
    peptide::Peptide,
    sequence::{IsBitSequence, PeptideSequence as Sequence},
};

#[derive(Error, Debug)]
pub enum Error {
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
    #[error("Unkown protease `{0}`")]
    UnkownProtease(String),
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

struct Trypsin;

impl Trypsin {
    const NAME: &'static str = "trypsin";
}

impl IsProtease for Trypsin {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn full_digest<'a>(&self, sequence: &'a [AminoAcidBitCode]) -> Vec<&'a [AminoAcidBitCode]> {
        let lysine_byte: u8 = LYSINE.bit_code().deku_id().unwrap();
        let arginine_byte: u8 = ARGININE.bit_code().deku_id().unwrap();
        let proline_byte: u8 = PROLINE.bit_code().deku_id().unwrap();

        let mut last_cleavage_pos: usize = 0;
        memchr::memchr2_iter(
            lysine_byte,
            arginine_byte,
            sequence
                .iter()
                .map(|bit_code| bit_code.deku_id().unwrap())
                .collect::<Vec<u8>>()
                .as_slice(),
        )
        .map(|pos| {
            (
                pos + 1,
                sequence
                    .get(pos + 1)
                    .map(|bit_code| bit_code.deku_id().unwrap()),
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
        let lysine_byte: u8 = LYSINE.bit_code().deku_id().unwrap();
        let arginine_byte: u8 = ARGININE.bit_code().deku_id().unwrap();
        let proline_byte: u8 = PROLINE.bit_code().deku_id().unwrap();

        memchr::memchr2_iter(
            lysine_byte,
            arginine_byte,
            sequence
                .iter()
                .map(|bit_code| bit_code.deku_id().unwrap())
                .collect::<Vec<u8>>()
                .as_slice(),
        )
        .map(|pos| {
            sequence
                .get(pos + 1)
                .map(|bit_code| bit_code.deku_id().unwrap())
        })
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

struct Unspecific;

impl Unspecific {
    const NAME: &'static str = "unspecific";
}

impl IsProtease for Unspecific {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn full_digest<'a>(&self, sequence: &'a [AminoAcidBitCode]) -> Vec<&'a [AminoAcidBitCode]> {
        (1..sequence.len())
            .map(|pos| &sequence[(pos - 1)..pos])
            .collect()
    }

    fn count_missed_cleavages(&self, sequence: &[AminoAcidBitCode]) -> usize {
        sequence.len()
    }
}

pub struct Protease {
    inner: Box<dyn IsProtease>,
    min_length: usize,
    max_length: usize,
    max_missed_cleavages: Option<usize>,
    keep_unknown: bool,
}

impl Protease {
    /// Cleaves a protein into peptides and returns a iterator over the peptides
    ///
    /// # Arguments
    /// * `sequence` - Amino acid sequence
    ///
    pub fn cleave(
        &self,
        sequence: &[AminoAcidBitCode],
    ) -> impl FallibleIterator<Item = Peptide, Error = Error> {
        let max_window_size = self.max_missed_cleavages().unwrap_or(sequence.len()) + 2;
        let full_digest = self.inner.full_digest(sequence);
        let n = full_digest.len();
        let mut window_size = 1_usize;
        let mut pos = 0_usize;

        fallible_iterator::from_fn(move || {
            loop {
                if window_size >= max_window_size {
                    return Ok(None);
                }
                if pos + window_size > n {
                    window_size += 1;
                    pos = 0;
                    continue;
                }
                let window = &full_digest[pos..pos + window_size];
                pos += 1;

                let length = window.iter().map(|seq| seq.len()).sum::<usize>();
                if !self.keep_unknown
                    && window
                        .iter()
                        .flat_map(|seq| seq.iter())
                        .any(|aa| aa == UNKNOWN.bit_code())
                {
                    continue;
                }
                if length < self.min_length {
                    continue;
                }

                if length > self.max_length {
                    continue;
                }

                return Ok(Some(Sequence::try_from(window).map(Peptide::new)?));
            }
        })
    }

    pub fn by_name(
        name: &str,
        min_length: Option<usize>,
        max_length: Option<usize>,
        max_missed_cleavages: Option<usize>,
        keep_unknown: bool,
    ) -> Result<Self, Error> {
        let min_length = min_length.unwrap_or(Sequence::MIN_LENGTH.get());
        let max_length = max_length.unwrap_or(Sequence::MAX_LENGTH.get());

        let inner: Box<dyn IsProtease> = match name.to_lowercase().as_str() {
            Trypsin::NAME => Box::new(Trypsin {}),
            Unspecific::NAME => Box::new(Unspecific {}),
            _ => return Err(Error::UnkownProtease(name.to_string())),
        };

        Ok(Self {
            min_length,
            max_length,
            max_missed_cleavages,
            keep_unknown,
            inner,
        })
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn min_length(&self) -> usize {
        self.min_length
    }

    pub fn max_length(&self) -> usize {
        self.max_length
    }

    pub fn max_missed_cleavages(&self) -> Option<usize> {
        self.max_missed_cleavages
    }
}

impl Clone for Protease {
    fn clone(&self) -> Self {
        Self::by_name(
            self.name(),
            Some(self.min_length),
            Some(self.max_length),
            self.max_missed_cleavages,
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
            self.max_missed_cleavages()
                .map(|missed_cleavages| missed_cleavages.to_string())
                .unwrap_or("after each amino acid".to_string()),
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

        let expected_peps_zero_missed_cleavages: HashSet<Sequence> = HashSet::from_iter([
            Sequence::try_from("SCHLPWASGLETLDSLGGVLEASGYSTEVVALSR").unwrap(),
            Sequence::try_from("MHWGTLCGFLWLWPYLFYVQAVPIQK").unwrap(),
            Sequence::try_from("VTGLDFIPGLHPILTLSK").unwrap(),
            Sequence::try_from("MDQTLAVYQQILTSMPSR").unwrap(),
            Sequence::try_from("LQGSLQDMLWQLDLSPGC").unwrap(),
            Sequence::try_from("NVIQISNDLENLR").unwrap(),
            Sequence::try_from("INDISHTQSVSSK").unwrap(),
            Sequence::try_from("DLLHVLAFSK").unwrap(),
            Sequence::try_from("VQDDTK").unwrap(),
        ]);

        let trypsin = Protease::by_name("trypsin", Some(6), Some(50), Some(0), false).unwrap();

        let peps = trypsin
            .cleave(leptin.as_ref())
            .map(|peptide| Ok(peptide.into_sequence()))
            .collect::<HashSet<Sequence>>()
            .unwrap();

        assert_eq!(peps, expected_peps_zero_missed_cleavages);

        let expected_peps_two_missed_cleavages: HashSet<Sequence> = HashSet::from_iter([
            Sequence::try_from("VTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSRNVIQISNDLENLR").unwrap(),
            Sequence::try_from("DLLHVLAFSKSCHLPWASGLETLDSLGGVLEASGYSTEVVALSR").unwrap(),
            Sequence::try_from("MDQTLAVYQQILTSMPSRNVIQISNDLENLRDLLHVLAFSK").unwrap(),
            Sequence::try_from("QKVTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSR").unwrap(),
            Sequence::try_from("VTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSR").unwrap(),
            Sequence::try_from("MHWGTLCGFLWLWPYLFYVQAVPIQKVQDDTKTLIK").unwrap(),
            Sequence::try_from("SCHLPWASGLETLDSLGGVLEASGYSTEVVALSR").unwrap(),
            Sequence::try_from("INDISHTQSVSSKQKVTGLDFIPGLHPILTLSK").unwrap(),
            Sequence::try_from("MHWGTLCGFLWLWPYLFYVQAVPIQKVQDDTK").unwrap(),
            Sequence::try_from("MDQTLAVYQQILTSMPSRNVIQISNDLENLR").unwrap(),
            Sequence::try_from("MHWGTLCGFLWLWPYLFYVQAVPIQK").unwrap(),
            Sequence::try_from("NVIQISNDLENLRDLLHVLAFSK").unwrap(),
            Sequence::try_from("TLIKTIVTRINDISHTQSVSSK").unwrap(),
            Sequence::try_from("QKVTGLDFIPGLHPILTLSK").unwrap(),
            Sequence::try_from("TIVTRINDISHTQSVSSKQK").unwrap(),
            Sequence::try_from("VTGLDFIPGLHPILTLSK").unwrap(),
            Sequence::try_from("TIVTRINDISHTQSVSSK").unwrap(),
            Sequence::try_from("LQGSLQDMLWQLDLSPGC").unwrap(),
            Sequence::try_from("MDQTLAVYQQILTSMPSR").unwrap(),
            Sequence::try_from("INDISHTQSVSSKQK").unwrap(),
            Sequence::try_from("VQDDTKTLIKTIVTR").unwrap(),
            Sequence::try_from("INDISHTQSVSSK").unwrap(),
            Sequence::try_from("NVIQISNDLENLR").unwrap(),
            Sequence::try_from("DLLHVLAFSK").unwrap(),
            Sequence::try_from("VQDDTKTLIK").unwrap(),
            Sequence::try_from("TLIKTIVTR").unwrap(),
            Sequence::try_from("VQDDTK").unwrap(),
        ]);

        let trypsin = Protease::by_name("trypsin", Some(6), Some(50), Some(2), false).unwrap();

        let peps = trypsin
            .cleave(leptin.as_ref())
            .map(|peptide| Ok(peptide.into_sequence()))
            .collect::<HashSet<Sequence>>()
            .unwrap();

        assert_eq!(peps, expected_peps_two_missed_cleavages);
    }
}
