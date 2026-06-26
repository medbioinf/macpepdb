use thiserror::Error;
use tokio_postgres::Row;

use crate::taxonomy_rank_table::{ID_COL, NAME_COL};

pub static SPECIES: &str = "species";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unable to convert taxonomy rank ID: {0}")]
    IdConversion(#[from] std::num::TryFromIntError),
    #[error("Row decoding error in taxonomy rank: {0}")]
    Row(Box<tokio_postgres::Error>),
}

into_thiserror_boxed!(tokio_postgres::Error, Error, Row);

pub struct TaxonomyRank {
    id: i16,
    name: String,
}

impl TaxonomyRank {
    pub fn new(id: i16, name: String) -> Self {
        TaxonomyRank { id, name }
    }

    pub fn id(&self) -> i16 {
        self.id
    }

    pub fn id_as_ref(&self) -> &i16 {
        &self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }
}

impl TryFrom<Row> for TaxonomyRank {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(TaxonomyRank {
            id: row.try_get(ID_COL)?,
            name: row.try_get(NAME_COL)?,
        })
    }
}

impl TryFrom<(&u64, &String)> for TaxonomyRank {
    type Error = Error;

    fn try_from((id, name): (&u64, &String)) -> Result<Self, Self::Error> {
        Ok(TaxonomyRank {
            id: i16::try_from(*id)?,
            name: name.clone(),
        })
    }
}
