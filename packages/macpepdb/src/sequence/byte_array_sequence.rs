use std::fmt::{Debug, Display};

use deku::prelude::*;

use crate::{
    amino_acid::{AminoAcid, AminoAcidBitCode},
    sequence::{Error, IsSequence, cql::ensure_not_null_slice},
};

#[derive(Eq, Hash, PartialEq, DekuRead, DekuWrite)]
pub struct ByteArraySequence {
    #[deku(update = "self.data.len() as u8", bits = 6)]
    count: u8,
    #[deku(count = "count")]
    data: Vec<AminoAcidBitCode>,
}

impl ByteArraySequence {
    pub fn new(data: Vec<AminoAcidBitCode>) -> Self {
        Self {
            count: data.len() as u8,
            data,
        }
    }
}

impl IsSequence for ByteArraySequence {
    const PEPTIDE_DATABASE: &str = "bytea_peptides";

    fn amino_acids(&self) -> impl Iterator<Item = Result<&'static AminoAcid, Error>> {
        self.data
            .iter()
            .map(|code| AminoAcid::by_aa_bit_code(code).map_err(Error::AminoAcid))
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl TryFrom<&str> for ByteArraySequence {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let vec = value
            .chars()
            .map(|c| {
                AminoAcid::by_code(c)
                    .map(|aa| aa.aa_bit_code().clone())
                    .map_err(Error::AminoAcid)
            })
            .collect::<Result<Vec<AminoAcidBitCode>, Error>>()?;

        Ok(ByteArraySequence::new(vec))
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

impl TryFrom<&ByteArraySequence> for Vec<u8> {
    type Error = Error;
    fn try_from(value: &ByteArraySequence) -> Result<Self, Self::Error> {
        value.to_bytes().map_err(Error::Bytes)
    }
}

impl<'a> tokio_postgres::types::FromSql<'a> for ByteArraySequence {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &[u8],
    ) -> Result<ByteArraySequence, Box<dyn std::error::Error + Sync + Send>> {
        let (_, sequence) = ByteArraySequence::from_bytes((raw, 0)).map_err(Error::Bytes)?;
        Ok(sequence)
    }

    tokio_postgres::types::accepts!(BYTEA);
}

impl tokio_postgres::types::ToSql for ByteArraySequence {
    fn to_sql(
        &self,
        _: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        postgres_protocol::types::bytea_to_sql(&self.to_bytes().map_err(Error::Bytes)?, out);

        Ok(tokio_postgres::types::IsNull::No)
    }

    tokio_postgres::types::accepts!(BYTEA);
    tokio_postgres::types::to_sql_checked!();
}

impl scylla::serialize::value::SerializeValue for ByteArraySequence {
    fn serialize<'b>(
        &self,
        typ: &scylla::cluster::metadata::ColumnType,
        writer: scylla::serialize::writers::CellWriter<'b>,
    ) -> Result<
        scylla::serialize::writers::WrittenCellProof<'b>,
        scylla::serialize::SerializationError,
    > {
        if !matches!(
            typ,
            scylla::cluster::metadata::ColumnType::Native(
                scylla::cluster::metadata::NativeType::Blob
            )
        ) {
            return Err(scylla::serialize::SerializationError::new(
                Error::UnexpectedCqlValueType(
                    typ.clone().into_owned(),
                    scylla::cluster::metadata::ColumnType::Native(
                        scylla::cluster::metadata::NativeType::Blob,
                    ),
                ),
            ));
        }

        let blob = self
            .to_bytes()
            .map_err(Error::Bytes)
            .map_err(scylla::serialize::SerializationError::new)?;
        writer.set_value(&blob).map_err(|_| {
            scylla::serialize::SerializationError::new(Error::ByteSequenceTooLargeForCqlBlob)
        })
    }
}

impl<'frame, 'metadata> scylla::deserialize::value::DeserializeValue<'frame, 'metadata>
    for ByteArraySequence
{
    fn type_check(
        typ: &scylla::cluster::metadata::ColumnType,
    ) -> Result<(), scylla::errors::TypeCheckError> {
        if !matches!(
            typ,
            scylla::cluster::metadata::ColumnType::Native(
                scylla::cluster::metadata::NativeType::Blob
            )
        ) {
            return Err(scylla::errors::TypeCheckError::new(
                Error::UnexpectedCqlValueType(
                    typ.clone().into_owned(),
                    scylla::cluster::metadata::ColumnType::Native(
                        scylla::cluster::metadata::NativeType::Blob,
                    ),
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
        let (_, sequence) =
            Self::from_bytes((val, 0)).map_err(scylla::errors::DeserializationError::new)?;
        Ok(sequence)
    }
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
        let bytea = sequence.to_bytes().unwrap();
        let (_, deserialized_sequence) =
            ByteArraySequence::from_bytes((bytea.as_slice(), 0)).unwrap();
        assert_eq!(sequence, deserialized_sequence);
        assert_eq!(sequence.to_string(), "PEPTIDER");
    }
}
