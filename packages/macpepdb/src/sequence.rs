use std::{
    fmt::{Debug, Display},
    hash::Hash,
    num::{NonZeroU8, NonZeroUsize},
    ops::{Index, Range},
};

use pastey::paste;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use serde::Serialize;
use thiserror::Error;
use zerocopy::IntoBytes;

use crate::{
    amino_acid::{AminoAcid, AminoAcidBitCode},
    mass::to_float as mass_to_float,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Amino acid in sequence: {0}")]
    AminoAcid(#[from] crate::amino_acid::Error),
    #[error("Malformed byte: {0:?}")]
    MalformedBytes(CompactSequence),
    #[error("Sequence too large {length} exceeds max {max_length})")]
    TooLong { length: usize, max_length: usize },
    #[error("Sequence too short {length} exceeds min {min_length})")]
    TooShort { length: usize, min_length: usize },
}

/// A more compact version of sequence, which stores the amino acids as
/// as 5 bits + x bits for the length, rounded to the next byte.
/// This can save up to 30% memory depending on the length of the sequence.
/// This version is meant to be compact, not feature rich (because non byte logic is slow), so more of it can be stored in
/// maps, sets, databases etc.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct CompactSequence(Vec<u8>);

impl From<CompactSequence> for Vec<u8> {
    fn from(compact_sequence: CompactSequence) -> Self {
        compact_sequence.0
    }
}

impl AsRef<[u8]> for CompactSequence {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

pub trait IsSimpleSequence: Clone + Display + Eq + Hash + PartialEq + Send + Sync {
    fn amino_acids(&self) -> impl Iterator<Item = &'static AminoAcid>;
    fn amino_acid_bit_codes(&self) -> impl Iterator<Item = &AminoAcidBitCode>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn first(&self) -> Option<&AminoAcidBitCode>;
    fn last(&self) -> Option<&AminoAcidBitCode>;
    fn contains(&self, aa: &AminoAcid) -> bool;
}

pub trait IsBitSequence<T: num_traits::Unsigned>:
    Debug
    + TryFrom<CompactSequence>
    + for<'a> TryFrom<&'a str>
    + for<'a> FromSql<'a>
    + ToSql
    + IsSimpleSequence
{
    const MIN_LENGTH: NonZeroUsize;
    const MAX_LENGTH: NonZeroUsize;
    const COUNT_BIT_WIDTH: NonZeroU8;

    fn data(&self) -> &[AminoAcidBitCode];

    /// This should check the length
    fn new(data: Vec<AminoAcidBitCode>) -> Result<Self, Error>;

    fn validate_length(data: &[AminoAcidBitCode]) -> Result<(), Error> {
        if data.len() < Self::MIN_LENGTH.get() {
            return Err(Error::TooShort {
                length: data.len(),
                min_length: Self::MIN_LENGTH.get(),
            });
        }

        if data.len() > Self::MAX_LENGTH.get() {
            return Err(Error::TooLong {
                length: data.len(),
                max_length: Self::MAX_LENGTH.get(),
            });
        }

        Ok(())
    }

    fn get(&self, index: usize) -> Option<&AminoAcidBitCode> {
        self.data().get(index)
    }

    fn cql_size(&self) -> usize {
        // The size of the compact representation in bytes, which is ceil((COUNT_BIT_WIDTH + 5 * len) / 8)
        let total_bits =
            Self::COUNT_BIT_WIDTH.get() as usize + AminoAcid::BIT_CODE_LEN * self.len();
        total_bits.div_ceil(8)
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Vec<AminoAcidBitCode>>() + self.data().len()
    }
}

macro_rules! make_sequence {
    ($name:ident, $count_type:ty, $count_bits:literal, $min_len:expr, $max_len:expr) => {
        paste! {
            #[derive(Clone, Eq, Hash, PartialEq, Serialize)]
            #[serde(into = "String")]
            pub struct [< $name:camel >] {
                data: Vec<AminoAcidBitCode>,
            }

            impl [< $name:camel >] {
                const LENGTH_BITS: u8 = $count_bits;
            }

            impl IsSimpleSequence for [< $name:camel >] {

                fn amino_acids(&self) -> impl Iterator<Item = &'static AminoAcid> {
                    self.data.iter().map(<&'static AminoAcid>::from)
                }

                fn amino_acid_bit_codes(&self) -> impl Iterator<Item = &AminoAcidBitCode> {
                    self.data.iter()
                }

                fn len(&self) -> usize {
                    self.data.len()
                }

                fn is_empty(&self) -> bool {
                    self.data.is_empty()
                }

                fn first(&self) -> Option<&AminoAcidBitCode> {
                    self.data.first()
                }

                fn last(&self) -> Option<&AminoAcidBitCode> {
                    self.data.last()
                }

                fn contains(&self, aa: &AminoAcid) -> bool {
                    self.data().contains(aa.bit_code())
                }
            }

            impl AsRef<[AminoAcidBitCode]> for [< $name:camel >] {
                fn as_ref(&self) -> &[AminoAcidBitCode] {
                    self.data.as_ref()
                }
            }


            impl IsBitSequence<$count_type> for [< $name:camel >] {
                const MIN_LENGTH: NonZeroUsize = NonZeroUsize::new($min_len).unwrap();
                const MAX_LENGTH: NonZeroUsize = NonZeroUsize::new($max_len).unwrap();
                const COUNT_BIT_WIDTH: NonZeroU8 = NonZeroU8::new($count_bits).unwrap();


                fn data(&self) -> &[AminoAcidBitCode] {
                    &self.data
                }

                fn new(data: Vec<AminoAcidBitCode>) -> Result<Self, Error> {
                    Self::validate_length(&data)?;
                    Ok(Self {
                        data,
                    })
                }
            }

            impl TryFrom<&[&[AminoAcidBitCode]]> for [< $name:camel >] {
                type Error = Error;

                fn try_from(values: &[&[AminoAcidBitCode]]) -> Result<Self, Self::Error> {
                    let mut data: Vec<AminoAcidBitCode> = Vec::with_capacity(values.iter().map(|sequence| sequence.len()).sum());

                    for slice in values {
                        data.extend_from_slice(slice);
                    }

                    Self::new(
                        data
                    )
                }
            }


            impl TryFrom<&str> for [< $name:camel >] {
                type Error = Error;

                fn try_from(value: &str) -> Result<Self, Self::Error> {
                    let vec = value
                        .chars()
                        .map(|c| {
                            AminoAcid::by_code(c)
                                .map(|aa| aa.bit_code().clone())
                                .map_err(Error::AminoAcid)
                        })
                        .collect::<Result<Vec<AminoAcidBitCode>, Error>>()?;

                    Self::new(vec)
                }
            }

            impl TryFrom<CompactSequence> for [< $name:camel >] {
                type Error = Error;

                fn try_from(value: CompactSequence) -> Result<Self, Self::Error> {
                    let mut buf: u64 = 0;
                    let mut bits: u8 = 0;
                    let mut byte_iter = value.0.iter();

                    // Accumulate bytes until we have at least COUNT_BITS bits available.
                    while bits < [< $name:camel >]::LENGTH_BITS {
                        match byte_iter.next() {
                            Some(&b) => {
                                buf = (buf << 8) | (b as u64);
                                bits += 8;
                            }
                            None => return Err(Error::MalformedBytes(value)),
                        }
                    }
                    // Extract the count by consuming the COUNT_BITS most-significant bits.
                    // After the subtraction `bits` is in 0..=7, so (1u64 << bits) never
                    // overflows, and the cast to $count_type acts as the natural mask.
                    bits -= [< $name:camel >]::LENGTH_BITS;
                    let count = ((buf >> bits) as $count_type) as usize;
                    buf &= (1u64 << bits) - 1;

                    // Read count × 5 bits, one amino acid at a time.
                    let mut data = Vec::with_capacity(count);
                    for _ in 0..count {
                        while bits < 5 {
                            match byte_iter.next() {
                                Some(&b) => {
                                    buf = (buf << 8) | (b as u64);
                                    bits += 8;
                                }
                                None => return Err(Error::MalformedBytes(value)),
                            }
                        }
                        bits -= 5;
                        let v = ((buf >> bits) & 0x1F) as u8;
                        buf &= (1u64 << bits) - 1;
                        data.push(AminoAcidBitCode::try_from(v)?);
                    }

                    Self::validate_length(&data)?;
                    Ok(Self { data })
                }
            }

            impl TryFrom<&[< $name:camel >]> for CompactSequence {
                type Error = Error;

                fn try_from(value: &[< $name:camel >]) -> Result<Self, Self::Error> {
                    let n = value.data.len();
                    let total_bits = [< $name:camel >]::LENGTH_BITS as usize + n * 5;
                    #[allow(clippy::manual_div_ceil)]
                    let num_bytes = (total_bits + 7) / 8;
                    let mut out = Vec::with_capacity(num_bytes);

                    // Seed the window with the length value occupying the LENGTH_BITS
                    // most-significant bits.
                    let mut buf: u64 = n as u64;
                    let mut bits: u8 = [< $name:camel >]::LENGTH_BITS;

                    for aa in value.data.iter() {
                        // Flush complete bytes *before* shifting in the next 5-bit
                        // value so the buffer never overflows (important when
                        // LENGTH_BITS = 64).
                        while bits >= 8 {
                            bits -= 8;
                            out.push((buf >> bits) as u8);
                            buf &= (1u64 << bits) - 1;
                        }
                        buf = (buf << 5) | (*aa as u8 as u64);
                        bits += 5;
                    }

                    // Flush remaining complete bytes …
                    while bits >= 8 {
                        bits -= 8;
                        out.push((buf >> bits) as u8);
                        buf &= (1u64 << bits) - 1;
                    }
                    // … and the partial trailing byte (zero-padded at the LSB).
                    if bits > 0 {
                        out.push((buf << (8 - bits)) as u8);
                    }
                    Ok(CompactSequence(out))
                }
            }

             impl TryFrom<[< $name:camel >]> for CompactSequence {
                type Error = Error;

                fn try_from(value: [< $name:camel >]) -> Result<Self, Self::Error> {
                    Self::try_from(&value)
                }
             }

            impl Display for [< $name:camel >] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    self.amino_acids()
                        .try_for_each(|amino_acid| write!(f, "{}", amino_acid.code()))
                }
            }

            impl Debug for [< $name:camel >] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    let byte_string = self
                        .data
                        .iter()
                        .map(|aa| format!("{:#X}", aa.as_bytes()[0]))
                        .collect::<Vec<String>>()
                        .join(" ");

                    write!(f, "Sequence({byte_string} ({self}))")
                }
            }

            impl Index<Range<usize>> for [< $name:camel >] {
                type Output = [AminoAcidBitCode];

                fn index(&self, range: Range<usize>) -> &Self::Output {
                    &self.data[range]
                }
            }


            impl Index<usize> for [< $name:camel >] {
                type Output = AminoAcidBitCode;

                fn index(&self, index: usize) -> &Self::Output {
                    &self.data[index]
                }
            }

            impl IntoIterator for [< $name:camel >] {
                type Item = AminoAcidBitCode;
                type IntoIter = std::vec::IntoIter<AminoAcidBitCode>;

                fn into_iter(self) -> Self::IntoIter {
                    self.data.into_iter()
                }
            }

            impl From<[< $name:camel >]> for String {
                fn from(value: [< $name:camel >]) -> Self {
                    value.to_string()
                }
            }

            // Stored as a BYTEA: the bit-packed `CompactSequence` bytes (5 bits/AA).
            impl ToSql for [< $name:camel >] {
                fn to_sql(
                    &self,
                    ty: &Type,
                    out: &mut bytes::BytesMut,
                ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
                    let byte_sequence = CompactSequence::try_from(self)?;
                    byte_sequence.as_ref().to_sql(ty, out)
                }

                fn accepts(ty: &Type) -> bool {
                    <&[u8] as ToSql>::accepts(ty)
                }

                to_sql_checked!();
            }

            impl<'a> FromSql<'a> for [< $name:camel >] {
                fn from_sql(
                    _ty: &Type,
                    raw: &'a [u8],
                ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                    Ok(Self::try_from(CompactSequence(raw.to_vec()))?)
                }

                fn accepts(ty: &Type) -> bool {
                    <&[u8] as FromSql>::accepts(ty)
                }
            }
        }
    };
}

// Peptide sequence limited to 6 to 50 amino acids, length can be stored in 6 bits
make_sequence!(PeptideSequence, u8, 6, 1, 50);

// Proteinsequence limited to 1 to 65.536 amino acids length can be stored in 16 bits
make_sequence!(ProteinSequence, u16, 16, 1, u16::MAX as usize);

/// Part of the a modified sequence which can keep amino acids as well as modifications (as strings)
///
#[derive(Clone, Eq, Hash, PartialEq)]
pub enum ModifiedSequencePart {
    AminoAcid(AminoAcidBitCode),
    CTerminalModification(i64),
    GlobalModifications(Vec<(i64, AminoAcidBitCode)>),
    NTerminalModification(i64),
    PositionModification(i64),
}

impl Display for ModifiedSequencePart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModifiedSequencePart::AminoAcid(aa) => {
                write!(f, "{}", AminoAcid::by_bit_code(aa).code())
            }
            ModifiedSequencePart::CTerminalModification(mass) => {
                write!(f, "-[{:+}]", mass_to_float(*mass))
            }
            ModifiedSequencePart::GlobalModifications(modifications) => {
                let mods = modifications
                    .iter()
                    .map(|(mass, aa)| {
                        format!(
                            "[{:+}]@{}",
                            mass_to_float(*mass),
                            AminoAcid::by_bit_code(aa).code()
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(",");
                write!(f, "<{}>", mods)
            }
            ModifiedSequencePart::NTerminalModification(mass) => {
                write!(f, "[{:+}]-", mass_to_float(*mass))
            }
            ModifiedSequencePart::PositionModification(mass) => {
                write!(f, "[{:+}]", mass_to_float(*mass))
            }
        }
    }
}

impl From<ModifiedSequencePart> for String {
    fn from(part: ModifiedSequencePart) -> Self {
        part.to_string()
    }
}

/// sequences which can contain both amino acids and modifications (ProForma compatible),
#[derive(Clone, Eq, Hash, PartialEq, Serialize)]
#[serde(into = "String")]
pub struct ModifiedSequence(Vec<ModifiedSequencePart>);

impl ModifiedSequence {
    pub fn first(&self) -> Option<&AminoAcidBitCode> {
        self.0.iter().find_map(|part| {
            if let ModifiedSequencePart::AminoAcid(aa) = part {
                Some(aa)
            } else {
                None
            }
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = &ModifiedSequencePart> {
        self.0.iter()
    }

    pub fn last(&self) -> Option<&AminoAcidBitCode> {
        self.0.iter().rev().find_map(|part| {
            if let ModifiedSequencePart::AminoAcid(aa) = part {
                Some(aa)
            } else {
                None
            }
        })
    }

    pub(crate) fn push(&mut self, part: ModifiedSequencePart) {
        self.0.push(part);
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }
}

impl Display for ModifiedSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for part in &self.0 {
            write!(f, "{}", part)?;
        }
        Ok(())
    }
}

impl From<PeptideSequence> for ModifiedSequence {
    fn from(peptide_sequence: PeptideSequence) -> Self {
        Self(
            peptide_sequence
                .into_iter()
                .map(ModifiedSequencePart::AminoAcid)
                .collect(),
        )
    }
}

impl From<ModifiedSequence> for String {
    fn from(value: ModifiedSequence) -> Self {
        value.0.into_iter().map(String::from).collect()
    }
}

impl IsSimpleSequence for ModifiedSequence {
    fn amino_acids(&self) -> impl Iterator<Item = &'static AminoAcid> {
        self.0.iter().filter_map(|part| {
            if let ModifiedSequencePart::AminoAcid(aa) = part {
                Some(AminoAcid::by_bit_code(aa))
            } else {
                None
            }
        })
    }

    fn amino_acid_bit_codes(&self) -> impl Iterator<Item = &AminoAcidBitCode> {
        self.0.iter().filter_map(|part| {
            if let ModifiedSequencePart::AminoAcid(aa) = part {
                Some(aa)
            } else {
                None
            }
        })
    }

    fn len(&self) -> usize {
        self.0
            .iter()
            .filter(|part| matches!(part, ModifiedSequencePart::AminoAcid(_)))
            .count()
    }

    fn is_empty(&self) -> bool {
        self.0
            .iter()
            .all(|part| !matches!(part, ModifiedSequencePart::AminoAcid(_)))
    }

    fn first(&self) -> Option<&AminoAcidBitCode> {
        self.0.iter().find_map(|part| {
            if let ModifiedSequencePart::AminoAcid(aa) = part {
                Some(aa)
            } else {
                None
            }
        })
    }

    fn last(&self) -> Option<&AminoAcidBitCode> {
        self.0.iter().rev().find_map(|part| {
            if let ModifiedSequencePart::AminoAcid(aa) = part {
                Some(aa)
            } else {
                None
            }
        })
    }

    fn contains(&self, aa: &AminoAcid) -> bool {
        self.0
            .contains(&ModifiedSequencePart::AminoAcid(*aa.bit_code()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creation() {
        let known_aa_seq = AminoAcid::all()
            .iter()
            .map(|aa| aa.code())
            .collect::<String>();

        let sequence = PeptideSequence::try_from(known_aa_seq.as_str());
        assert!(sequence.is_ok());

        let sequence = sequence.unwrap();
        assert_eq!(sequence.len(), known_aa_seq.len());
        assert_eq!(sequence.to_string(), known_aa_seq);

        let invalid_seq = format!("{known_aa_seq}!123");
        let sequence = PeptideSequence::try_from(invalid_seq.as_str());
        assert!(sequence.is_err());
    }

    #[test]
    fn test_into_bytes() {
        let known_aa_seq = AminoAcid::all()
            .iter()
            .map(|aa| aa.code())
            .collect::<String>();

        let sequence = PeptideSequence::try_from(known_aa_seq.as_str());
        assert!(sequence.is_ok());

        let sequence = sequence.unwrap();
        let compact = CompactSequence::try_from(&sequence).unwrap();

        let deserialized_sequence = PeptideSequence::try_from(compact).unwrap();
        assert_eq!(sequence, deserialized_sequence);
        assert_eq!(sequence.to_string(), known_aa_seq);
    }

    #[test]
    fn test_cql_size() {
        let peptide =
            PeptideSequence::try_from("VTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSRNVIQISNDLENLR").unwrap();

        // (5 (amino acid bit code) * length + count bits).div_ceil(8)
        assert_eq!(peptide.cql_size(), 32);
    }
}
