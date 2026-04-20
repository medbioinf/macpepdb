use postgres_types::{FromSql, ToSql};
use std::fmt::{Debug, Display};

use crate::{
    amino_acid::AminoAcid,
    sequence::{Error, IsSequence, cql::ensure_not_null_slice},
};

#[derive(Eq, Hash, PartialEq, ToSql, FromSql)]
#[postgres(transparent)]
pub struct StringSequence(String);

impl IsSequence for StringSequence {
    const PEPTIDE_DATABASE: &str = "str_peptides";

    fn amino_acids(&self) -> impl Iterator<Item = Result<&'static AminoAcid, Error>> {
        self.0
            .chars()
            .map(|code| AminoAcid::by_code(code).map_err(Error::AminoAcid))
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl TryFrom<&str> for StringSequence {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut string = String::with_capacity(value.len());

        for amino_acid in value.chars().map(AminoAcid::by_code) {
            let amino_acid = amino_acid?;
            string.push(amino_acid.code());
        }

        Ok(StringSequence(string))
    }
}

impl TryFrom<StringSequence> for String {
    type Error = Error;

    fn try_from(value: StringSequence) -> Result<Self, Self::Error> {
        Ok(value.0)
    }
}

impl Display for StringSequence {
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

impl Debug for StringSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StringSequence({})", self)
    }
}

impl scylla::serialize::value::SerializeValue for StringSequence {
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
                scylla::cluster::metadata::NativeType::Text
            )
        ) {
            return Err(scylla::serialize::SerializationError::new(
                Error::UnexpectedCqlValueType(
                    typ.clone().into_owned(),
                    scylla::cluster::metadata::ColumnType::Native(
                        scylla::cluster::metadata::NativeType::Text,
                    ),
                ),
            ));
        }

        writer.set_value(self.0.as_bytes()).map_err(|_| {
            scylla::serialize::SerializationError::new(Error::ByteSequenceTooLargeForCqlBlob)
        })
    }
}

impl<'frame, 'metadata> scylla::deserialize::value::DeserializeValue<'frame, 'metadata>
    for StringSequence
{
    fn type_check(
        typ: &scylla::cluster::metadata::ColumnType,
    ) -> Result<(), scylla::errors::TypeCheckError> {
        if matches!(
            typ,
            scylla::cluster::metadata::ColumnType::Native(
                scylla::cluster::metadata::NativeType::Text
            )
        ) {
            return Err(scylla::errors::TypeCheckError::new(
                Error::UnexpectedCqlValueType(
                    typ.clone().into_owned(),
                    scylla::cluster::metadata::ColumnType::Native(
                        scylla::cluster::metadata::NativeType::Text,
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
        Ok(StringSequence(String::from_utf8_lossy(val).to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence() {
        let sequence = StringSequence::try_from("PEPTIDER");
        assert!(sequence.is_ok());
        let sequence = sequence.unwrap();
        assert_eq!(sequence.len(), 8);
        assert_eq!(sequence.to_string(), "PEPTIDER");
    }
}
