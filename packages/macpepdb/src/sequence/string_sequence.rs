use postgres_types::{FromSql, ToSql};
use std::fmt::{Debug, Display};

use crate::{
    amino_acid::AminoAcid,
    sequence::{Error, IsSequence},
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
