use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

use tokio_postgres::types::{FromSql, ToSql};

use crate::{amino_acid::AminoAcid, molecules::WATER_MONO_MASS, sequence::Error};

pub trait IsSequence:
    Display
    + Debug
    + Eq
    + Hash
    + PartialEq
    + Send
    + Sync
    + TryInto<String, Error = Error>
    + for<'a> TryFrom<&'a str, Error = Error>
    + for<'frame, 'metadata> scylla::deserialize::value::DeserializeValue<'frame, 'metadata>
    + for<'a> FromSql<'a>
    + scylla::serialize::value::SerializeValue
    + ToSql
{
    const PEPTIDE_DATABASE: &str;

    fn amino_acids(&self) -> impl Iterator<Item = Result<&'static AminoAcid, Error>>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn to_peptide_mass(&self) -> Result<i64, Error> {
        self.amino_acids()
            .try_fold(WATER_MONO_MASS, |acc, amino_acid| {
                Ok(acc + amino_acid?.mono_mass())
            })
    }
}
