use std::{cell::OnceCell, hash::Hash};

use deku::DekuEnumExt;
use scylla::{DeserializeRow, SerializeRow};
use thiserror::Error;

use crate::{amino_acid::AminoAcid, sequence::Sequence};

pub const TABLE_NAME: &str = "peptides";

const MAX_AMINO_ACID_BIT_CODE: usize = (b'Z' - b'A') as usize;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Cql error: {0}")]
    Cql(Box<scylla::errors::ExecutionError>),
    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("{0}")]
    Sequence(#[from] crate::sequence::Error),
    #[error("Invalid amino acid code: {0}")]
    AminoAcid(#[from] crate::amino_acid::Error),
}

#[derive(DeserializeRow, SerializeRow)]
pub struct Peptide {
    mass: i64,
    sequence: Sequence,
    #[scylla(skip)]
    amino_acid_counts: OnceCell<[u8; MAX_AMINO_ACID_BIT_CODE]>,
}

impl Peptide {
    pub fn new(sequence: Sequence) -> Self {
        let mass = sequence.to_peptide_mass();
        Self {
            mass,
            sequence,
            amino_acid_counts: OnceCell::new(),
        }
    }

    pub fn mass(&self) -> i64 {
        self.mass
    }

    pub fn sequence(&self) -> &Sequence {
        &self.sequence
    }

    pub fn len(&self) -> usize {
        self.sequence.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sequence.is_empty()
    }

    pub fn into_sequence(self) -> Sequence {
        self.sequence
    }

    pub fn amino_acid_counts(&self) -> &[u8; MAX_AMINO_ACID_BIT_CODE] {
        self.amino_acid_counts.get_or_init(|| {
            let mut counts = [0; MAX_AMINO_ACID_BIT_CODE];

            self.sequence
                .amino_acid_bit_codes()
                .for_each(|bit_code| counts[bit_code.deku_id().unwrap() as usize] += 1);
            counts
        })
    }

    pub fn amino_acid_count(&self, amino_acid: &'static AminoAcid) -> u8 {
        let idx = (amino_acid.code() as u8 - b'A') as usize;
        self.amino_acid_counts()[idx]
    }

    pub fn amino_acid_count_by_code(&self, code: char) -> Result<u8, Error> {
        let amino_acid = AminoAcid::by_code(code)?;
        Ok(self.amino_acid_count(amino_acid))
    }

    pub fn cssndr_insert_statement() -> String {
        format!("INSERT INTO {} (mass, sequence) VALUES (?, ?)", TABLE_NAME)
    }

    pub async fn cssndr_insert_with_preped_statement(
        &self,
        client: &scylla::client::session::Session,
        prepared_statement: &scylla::statement::prepared::PreparedStatement,
    ) -> Result<(), Error> {
        client
            .execute_unpaged(prepared_statement, (self.mass(), self.sequence()))
            .await
            .map_err(|err| Error::Cql(Box::new(err)))?;

        Ok(())
    }

    pub async fn cssndr_insert_with_preped_statement_owned(
        self,
        client: &scylla::client::session::Session,
        prepared_statement: &scylla::statement::prepared::PreparedStatement,
    ) -> Result<Self, Error> {
        self.cssndr_insert_with_preped_statement(client, prepared_statement)
            .await?;
        Ok(self)
    }
}

impl Eq for Peptide {}

impl PartialEq for Peptide {
    fn eq(&self, other: &Self) -> bool {
        self.mass == other.mass && self.sequence == other.sequence
    }
}

impl Hash for Peptide {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.mass.hash(state);
        self.sequence.hash(state);
    }
}

#[cfg(test)]
mod tests {}
