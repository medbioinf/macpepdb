use std::fmt::Display;

use dihardts_omicstools::biology::taxonomy::Taxonomy as OmicstoolsTaxonomy;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_postgres::Row;

use crate::taxonomy_table::{
    ID_COL, PARENT_ID_COL, RANK_ID_COL, RANK_NAME_ALIAS_COL, SCIENTIFIC_NAME_COL,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to convert taxonomy ID to i32: {0}")]
    IdConversion(std::num::TryFromIntError),
    #[error("Failed to convert taxonomy parent ID to i32: {0}")]
    ParentIdConversion(std::num::TryFromIntError),
    #[error("Failed to convert taxonomy rank id ID i32: {0}")]
    RankIdConversion(std::num::TryFromIntError),
    #[error("Row decoding error in taxonomy: {0}")]
    Row(Box<tokio_postgres::Error>),
}

into_thiserror_boxed!(tokio_postgres::Error, Error, Row);

/// An NCBI taxonomy node as stored in and read from the database (ID, parent ID, scientific
/// name, and rank).
#[derive(Clone, Deserialize, Serialize)]
pub struct Taxonomy {
    id: i32,
    parent_id: i32,
    scientific_name: String,
    rank_id: i16,
    rank_name: Option<String>,
}

impl Taxonomy {
    /// Creates a new taxonomy record. `rank_name` may be `None` when the caller hasn't joined
    /// the rank name in (see [`Taxonomy::rank_name`]).
    pub fn new(
        id: i32,
        parent_id: i32,
        scientific_name: String,
        rank_id: i16,
        rank_name: Option<String>,
    ) -> Self {
        Taxonomy {
            id,
            parent_id,
            scientific_name,
            rank_id,
            rank_name,
        }
    }

    /// Returns the taxonomy ID.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Returns a reference to the taxonomy ID.
    pub fn id_as_ref(&self) -> &i32 {
        &self.id
    }

    /// Returns the ID of the parent taxonomy node.
    pub fn parent_id(&self) -> i32 {
        self.parent_id
    }

    /// Returns a reference to the ID of the parent taxonomy node.
    pub fn parent_id_as_ref(&self) -> &i32 {
        &self.parent_id
    }

    /// Returns the scientific name.
    pub fn scientific_name(&self) -> &String {
        &self.scientific_name
    }

    /// Returns the ID of the taxonomic rank (see `TaxonomyRank`).
    pub fn rank_id(&self) -> i16 {
        self.rank_id
    }

    /// Returns a reference to the ID of the taxonomic rank.
    pub fn rank_id_as_ref(&self) -> &i16 {
        &self.rank_id
    }

    /// Returns the taxonomic rank's name, if it was fetched/joined in alongside this row.
    pub fn rank_name(&self) -> Option<&str> {
        self.rank_name.as_deref()
    }

    pub(crate) fn rank_name_mut(&mut self) -> &mut Option<String> {
        &mut self.rank_name
    }
}

impl Display for Taxonomy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "ID:              {}", self.id())?;
        writeln!(f, "parent ID:       {} ", self.parent_id())?;
        writeln!(f, "scientific_name: {} ", self.scientific_name())?;
        writeln!(
            f,
            "rank ID:         {} ({}) ",
            self.rank_id(),
            self.rank_name().unwrap_or("name not fetched")
        )
    }
}

impl TryFrom<Row> for Taxonomy {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Taxonomy {
            id: row.try_get(ID_COL)?,
            parent_id: row.try_get(PARENT_ID_COL)?,
            scientific_name: row.try_get(SCIENTIFIC_NAME_COL)?,
            rank_id: row.try_get(RANK_ID_COL)?,
            rank_name: row
                .columns()
                .iter()
                .find(|col| col.name() == RANK_NAME_ALIAS_COL)
                .map(|_| row.try_get(RANK_NAME_ALIAS_COL))
                .transpose()?,
        })
    }
}

impl TryFrom<&&OmicstoolsTaxonomy> for Taxonomy {
    type Error = Error;

    fn try_from(taxonomy: &&OmicstoolsTaxonomy) -> Result<Self, Self::Error> {
        Ok(Taxonomy {
            id: i32::try_from(taxonomy.get_id()).map_err(Error::IdConversion)?,
            parent_id: i32::try_from(taxonomy.get_parent_id())
                .map_err(Error::ParentIdConversion)?,
            scientific_name: taxonomy.get_scientific_name().to_string(),
            rank_id: i16::try_from(taxonomy.get_rank_id()).map_err(Error::RankIdConversion)?,
            rank_name: None,
        })
    }
}

impl From<&Taxonomy> for macpepdb_web_common::responses::taxonomy::TaxonomyResponse {
    fn from(taxonomy: &Taxonomy) -> Self {
        Self {
            id: taxonomy.id,
            parent_id: taxonomy.parent_id,
            scientific_name: taxonomy.scientific_name.clone(),
            rank_id: taxonomy.rank_id,
            rank_name: taxonomy.rank_name.clone(),
        }
    }
}
