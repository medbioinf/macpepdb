use std::fmt::{Debug, Display};

use deku::prelude::*;
use scylla::{
    cluster::metadata::{ColumnType, NativeType},
    deserialize::value::DeserializeValue,
    errors::SerializationError,
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
    },
};
use thiserror::Error;

use crate::{
    amino_acid::{AminoAcid, AminoAcidBitCode},
    cql::ensure_not_null_slice,
    molecules::WATER_MONO_MASS,
};

pub const MIN_SEQUENCE_LEN: usize = 1;
pub const MAX_SEQUENCE_LENGTH: usize = 50;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Amino acid in sequence: {0}")]
    AminoAcid(#[from] crate::amino_acid::Error),
    #[error("Deku error in seqeunce: {0}")]
    Bytes(#[from] deku::error::DekuError),
    #[error("CQL value too large for Sequence blob (max {MAX_SEQUENCE_LENGTH} amino acids)")]
    CqlValueTooLarge,
    #[error("Internal CQL error in seqeunce: {0}")]
    InternalCql(#[from] crate::cql::Error),
    #[error("Sequence too large for CQL blob (max {MAX_SEQUENCE_LENGTH} amino acids)")]
    TooLong { length: usize, max_length: usize },
    #[error("Sequence too short for CQL blob (min {MIN_SEQUENCE_LEN} amino acids)")]
    TooShort { length: usize, min_length: usize },
    #[error("Expected {0:?} got {1:?}")]
    UnexpectedCqlValueType(
        scylla::cluster::metadata::ColumnType<'static>,
        scylla::cluster::metadata::ColumnType<'static>,
    ),
}

#[derive(Clone, Eq, Hash, PartialEq, DekuRead, DekuWrite)]
pub struct Sequence {
    #[deku(update = "self.data.len() as u8", bits = 6)]
    count: u8,
    #[deku(count = "count")]
    data: Vec<AminoAcidBitCode>,
}

impl Sequence {
    pub fn new(data: Vec<AminoAcidBitCode>) -> Result<Self, Error> {
        if data.len() < MIN_SEQUENCE_LEN {
            return Err(Error::TooShort {
                length: data.len(),
                min_length: MIN_SEQUENCE_LEN,
            });
        }

        if data.len() > MAX_SEQUENCE_LENGTH {
            return Err(Error::TooLong {
                length: data.len(),
                max_length: MAX_SEQUENCE_LENGTH,
            });
        }

        Ok(Self {
            count: data.len() as u8,
            data,
        })
    }

    pub fn amino_acids(&self) -> impl Iterator<Item = &'static AminoAcid> {
        self.data.iter().map(<&'static AminoAcid>::from)
    }

    pub fn amino_acid_bit_codes(&self) -> impl Iterator<Item = &AminoAcidBitCode> {
        self.data.iter()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn to_peptide_mass(&self) -> i64 {
        self.amino_acids().fold(WATER_MONO_MASS, |acc, amino_acid| {
            acc + amino_acid.mono_mass()
        })
    }
}

impl TryFrom<&str> for Sequence {
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

        Sequence::new(vec)
    }
}

impl TryFrom<&Sequence> for Vec<u8> {
    type Error = Error;
    fn try_from(value: &Sequence) -> Result<Self, Self::Error> {
        value.to_bytes().map_err(Error::Bytes)
    }
}

impl From<&Sequence> for String {
    fn from(value: &Sequence) -> Self {
        value.amino_acids().map(|aa| aa.code()).collect()
    }
}

impl Display for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.amino_acids()
            .try_for_each(|amino_acid| write!(f, "{}", amino_acid.code()))
    }
}

impl Debug for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bit_string = match self.to_bytes() {
            Ok(bytes) => bytes
                .into_iter()
                .map(|byte| format!("{byte:0>5b}"))
                .collect::<Vec<String>>()
                .join("_"),
            Err(e) => format!("Error converting to bytes: {e}"),
        };

        write!(f, "Sequence({bit_string} ({self}))")
    }
}

// impl Serialize for Sequence {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         let s = String::try_from(self).map_err(serde::ser::Error::custom)?;
//         serializer.serialize_str(&s)
//     }
// }

// impl<'de> Deserialize<'de> for Sequence {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         let s = String::deserialize(deserializer)?;
//         Sequence::try_from(s.as_str()).map_err(serde::de::Error::custom)
//     }
// }

impl SerializeValue for Sequence {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        if !matches!(typ, ColumnType::Native(NativeType::Blob)) {
            return Err(SerializationError::new(Error::UnexpectedCqlValueType(
                typ.clone().into_owned(),
                ColumnType::Native(NativeType::Blob),
            )));
        }

        let blob = self
            .to_bytes()
            .map_err(Error::Bytes)
            .map_err(SerializationError::new)?;
        writer
            .set_value(&blob)
            .map_err(|_| SerializationError::new(Error::CqlValueTooLarge))
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for Sequence {
    fn type_check(typ: &ColumnType) -> Result<(), scylla::errors::TypeCheckError> {
        if !matches!(typ, ColumnType::Native(NativeType::Blob)) {
            return Err(scylla::errors::TypeCheckError::new(
                Error::UnexpectedCqlValueType(
                    typ.clone().into_owned(),
                    ColumnType::Native(NativeType::Blob),
                ),
            ));
        }
        Ok(())
    }

    fn deserialize(
        typ: &'metadata scylla::cluster::metadata::ColumnType<'metadata>,
        v: Option<scylla::deserialize::FrameSlice<'frame>>,
    ) -> Result<Self, scylla::errors::DeserializationError> {
        let val = ensure_not_null_slice::<&[u8]>(typ, v)?;
        let (_, sequence) = Self::from_bytes((val, 0))
            .map_err(|err| scylla::errors::DeserializationError::new(Error::Bytes(err)))?;
        Ok(sequence)
    }
}

/// A more compact version of sequence, which stores the amino acids as
/// as 5 bits + 6 bit for the length rounded to the next byte.
/// This can safe up to 30% memory depending on the length of the seqeunce.
/// This version is ment to be compact, not feature rich (because non byte logic is slow), so more of it can be stored in
/// maps, sets etc.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct BitSequence(Vec<u8>);

impl TryFrom<Sequence> for BitSequence {
    type Error = Error;

    fn try_from(value: Sequence) -> Result<Self, Self::Error> {
        Ok(BitSequence(value.to_bytes()?))
    }
}

impl TryFrom<BitSequence> for Sequence {
    type Error = Error;

    fn try_from(value: BitSequence) -> Result<Self, Self::Error> {
        let (_, sequence) = Sequence::from_bytes((value.0.as_slice(), 0))?;
        Ok(sequence)
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

        let sequence = Sequence::try_from(known_aa_seq.as_str());
        assert!(sequence.is_ok());

        let sequence = sequence.unwrap();
        assert_eq!(sequence.len(), known_aa_seq.len());
        assert_eq!(sequence.to_string(), known_aa_seq);

        let invalid_seq = format!("{known_aa_seq}!123");
        let sequence = Sequence::try_from(invalid_seq.as_str());
        assert!(sequence.is_err());
    }

    #[test]
    fn test_into_bytes() {
        let known_aa_seq = AminoAcid::all()
            .iter()
            .map(|aa| aa.code())
            .collect::<String>();

        let sequence = Sequence::try_from(known_aa_seq.as_str());
        assert!(sequence.is_ok());

        let sequence = sequence.unwrap();
        let bytea = sequence.to_bytes().unwrap();

        let (_, deserialized_sequence) = Sequence::from_bytes((bytea.as_slice(), 0)).unwrap();
        assert_eq!(sequence, deserialized_sequence);
        assert_eq!(sequence.to_string(), known_aa_seq);
    }
}
