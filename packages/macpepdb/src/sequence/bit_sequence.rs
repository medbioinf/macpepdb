use std::fmt::{Debug, Display};

use bitvec::{order::Lsb0, vec::BitVec};

use crate::{
    amino_acid::AminoAcid,
    sequence::{Error, IsSequence},
};

#[derive(Eq, Hash, PartialEq)]
pub struct BitSequence(BitVec<u8, Lsb0>);

impl IsSequence for BitSequence {
    const PEPTIDE_DATABASE: &str = "bit_peptides";

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

impl TryFrom<&str> for BitSequence {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut vec = BitVec::<u8, Lsb0>::with_capacity(value.len() * AminoAcid::BIT_CODE_LEN);

        for amino_acid in value.chars().map(AminoAcid::by_code) {
            let amino_acid = amino_acid?;
            vec.extend_from_bitslice(amino_acid.bit_code());
        }

        Ok(BitSequence(vec))
    }
}

impl TryFrom<BitSequence> for String {
    type Error = Error;

    fn try_from(value: BitSequence) -> Result<Self, Self::Error> {
        let mut string = String::with_capacity(value.len());
        for amino_acid in value.amino_acids() {
            string.push(amino_acid?.code());
        }
        Ok(string)
    }
}

impl Display for BitSequence {
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

impl Debug for BitSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitSequence({})", self)
    }
}

impl<'a> tokio_postgres::types::FromSql<'a> for BitSequence {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        raw: &[u8],
    ) -> Result<BitSequence, Box<dyn std::error::Error + Sync + Send>> {
        let varbit = postgres_protocol::types::varbit_from_sql(raw)?;
        let mut bitvec = BitVec::<u8, Lsb0>::from_slice(varbit.bytes());
        while bitvec.len() > varbit.len() {
            bitvec.pop();
        }

        Ok(BitSequence(bitvec))
    }

    tokio_postgres::types::accepts!(BIT, VARBIT);
}

impl tokio_postgres::types::ToSql for BitSequence {
    fn to_sql(
        &self,
        _: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        postgres_protocol::types::varbit_to_sql(
            self.0.len(),
            self.0.as_raw_slice().iter().cloned(),
            out,
        )?;
        Ok(tokio_postgres::types::IsNull::No)
    }

    tokio_postgres::types::accepts!(BIT, VARBIT);
    tokio_postgres::types::to_sql_checked!();
}

impl scylla::serialize::value::SerializeValue for BitSequence {
    fn serialize<'b>(
        &self,
        _typ: &scylla::cluster::metadata::ColumnType,
        _writer: scylla::serialize::writers::CellWriter<'b>,
    ) -> Result<
        scylla::serialize::writers::WrittenCellProof<'b>,
        scylla::serialize::SerializationError,
    > {
        unimplemented!()
    }
}

impl<'frame, 'metadata> scylla::deserialize::value::DeserializeValue<'frame, 'metadata>
    for BitSequence
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
        _typ: &'metadata scylla::cluster::metadata::ColumnType<'metadata>,
        _v: Option<scylla::deserialize::FrameSlice<'frame>>,
    ) -> Result<Self, scylla::errors::DeserializationError> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence() {
        let sequence = BitSequence::try_from("PEPTIDER");
        assert!(sequence.is_ok());
        let sequence = sequence.unwrap();
        assert_eq!(sequence.len(), 8);
        assert_eq!(sequence.to_string(), "PEPTIDER");
    }
}
