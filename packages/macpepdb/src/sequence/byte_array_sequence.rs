use std::fmt::{Debug, Display};

use bitvec::{field::BitField, order::Lsb0, vec::BitVec, view::BitView};

use crate::{
    amino_acid::AminoAcid,
    sequence::{Error, IsSequence},
};

#[derive(Eq, Hash, PartialEq)]
pub struct ByteArraySequence(BitVec<u8, Lsb0>);

impl IsSequence for ByteArraySequence {
    const PEPTIDE_DATABASE: &str = "bytea_peptides";

    fn amino_acids(&self) -> impl Iterator<Item = Result<&'static AminoAcid, Error>> {
        self.0
            .chunks(AminoAcid::BIT_CODE_LEN)
            .map(|chunk| AminoAcid::by_bit_code(chunk).map_err(Error::AminoAcid))
    }

    fn len(&self) -> usize {
        self.0.len() / AminoAcid::BIT_CODE_LEN
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl TryFrom<&str> for ByteArraySequence {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut vec = BitVec::<u8, Lsb0>::with_capacity(value.len() * AminoAcid::BIT_CODE_LEN);

        for amino_acid in value.chars().map(AminoAcid::by_code) {
            let amino_acid = amino_acid?;
            vec.extend_from_bitslice(amino_acid.bit_code());
        }

        Ok(ByteArraySequence(vec))
    }
}

impl TryFrom<ByteArraySequence> for String {
    type Error = Error;

    fn try_from(value: ByteArraySequence) -> Result<Self, Self::Error> {
        let mut string = String::with_capacity(value.len());
        for amino_acid in value.amino_acids() {
            string.push(amino_acid?.code());
        }
        Ok(string)
    }
}

impl Display for ByteArraySequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for amino_acid in self.amino_acids() {
            match amino_acid {
                Ok(amino_acid) => write!(f, "{}", amino_acid.code())?,
                Err(err) => write!(f, "?[{}]", err)?,
            }
        }
        Ok(())
    }
}

impl Debug for ByteArraySequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ByteArraySequence({})", self)
    }
}

impl TryFrom<&[u8]> for ByteArraySequence {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut bitvec = BitVec::<u8, Lsb0>::from_slice(value);
        if bitvec.len() < 6 {
            return Err(Error::InvalidByteArrayByteVectorRepresentation(
                bitvec.len(),
            ));
        }

        // Take the first 6 bit and restore length
        let length = bitvec.drain(..6).as_bitslice()[..6].load_le::<u8>() as usize;

        // Check if reminaing array is not too short
        if bitvec.len() < length * AminoAcid::BIT_CODE_LEN {
            return Err(Error::InvalidByteArraySequenceLength(
                bitvec.len(),
                length * AminoAcid::BIT_CODE_LEN,
            ));
        }

        bitvec.truncate(length * AminoAcid::BIT_CODE_LEN); // truncate the remaining bits from u8 conversion

        Ok(ByteArraySequence(bitvec))
    }
}

impl From<&ByteArraySequence> for Vec<u8> {
    fn from(value: &ByteArraySequence) -> Self {
        let mut bytes =
            BitVec::<u8, Lsb0>::with_capacity(6 + value.len() * AminoAcid::BIT_CODE_LEN);

        bytes.extend_from_bitslice(&(value.len() as u8).view_bits::<Lsb0>()[..6]);
        bytes.extend_from_bitslice(&value.0);

        bytes.as_raw_slice().to_vec()
    }
}

impl<'a> tokio_postgres::types::FromSql<'a> for ByteArraySequence {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &[u8],
    ) -> Result<ByteArraySequence, Box<dyn std::error::Error + Sync + Send>> {
        Ok(ByteArraySequence::try_from(raw)?)
    }

    tokio_postgres::types::accepts!(BYTEA);
}

impl tokio_postgres::types::ToSql for ByteArraySequence {
    fn to_sql(
        &self,
        _: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        postgres_protocol::types::bytea_to_sql(&Vec::<u8>::from(self), out);

        Ok(tokio_postgres::types::IsNull::No)
    }

    tokio_postgres::types::accepts!(BYTEA);
    tokio_postgres::types::to_sql_checked!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence() {
        let sequence = ByteArraySequence::try_from("PEPTIDER");
        assert!(sequence.is_ok());
        let sequence = sequence.unwrap();
        assert_eq!(sequence.len(), 8);
        assert_eq!(sequence.to_string(), "PEPTIDER");
    }

    #[test]
    fn test_into_bytes() {
        let sequence = ByteArraySequence::try_from("PEPTIDER").unwrap();
        let bytea = Vec::<u8>::from(&sequence);
        let deserialized_sequence = ByteArraySequence::try_from(bytea.as_slice()).unwrap();
        assert_eq!(sequence, deserialized_sequence);
        assert_eq!(sequence.to_string(), "PEPTIDER");
    }
}
