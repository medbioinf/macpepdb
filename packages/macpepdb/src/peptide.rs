use std::{cell::OnceCell, hash::Hash, sync::LazyLock};

use deku::DekuEnumExt;
use futures::future::join_all;
use scylla::{DeserializeRow, SerializeRow, client::pager::TypedRowStream, errors::ExecutionError};

use thiserror::Error;

use crate::{
    amino_acid::{AminoAcid, AminoAcidBitCode},
    client::Client,
    mass_partitioner::Partitioning,
    molecules::WATER_MONO_MASS,
    sequence::{IsSequence, PeptideSequence as Sequence},
};

pub const TABLE_NAME: &str = "peptides";

static INSERT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!("INSERT INTO {TABLE_NAME} (partition, mass, sequence) VALUES (?, ?, ?)")
});

static SELECT_STATEMENT: LazyLock<String> = LazyLock::new(|| format!("SELECT * FROM {TABLE_NAME}"));

const MAX_AMINO_ACID_BIT_CODE: usize = (b'Z' - b'A') as usize;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in peptide: {0}")]
    Client(#[from] crate::client::Error),
    #[error("CQL execution error in peptide: {0}")]
    CqlExecution(#[from] Box<scylla::errors::ExecutionError>),
    #[error("CQL paged execution error in peptide: {0}")]
    CqlPagedExecution(#[from] Box<scylla::errors::PagerExecutionError>),
    #[error("CQL type check failed in peptide: {0}")]
    CqlTypeCheck(#[from] scylla::errors::TypeCheckError),
    #[error("Partition not found peptide `{0}` with mass {1}")]
    NoPartition(String, i64),
    #[error("Sequence error in peptide: {0}")]
    Sequence(#[from] crate::sequence::Error),
    #[error("Amino acid error in peptide: {0}")]
    AminoAcid(#[from] crate::amino_acid::Error),
}

#[derive(DeserializeRow, SerializeRow)]
pub struct Peptide {
    partition: Option<i32>,
    mass: i64,
    sequence: Sequence,
    #[scylla(skip)]
    amino_acid_counts: OnceCell<[u8; MAX_AMINO_ACID_BIT_CODE]>,
}

impl Peptide {
    pub fn new(sequence: Sequence) -> Self {
        let mass = Self::to_peptide_mass(&sequence);
        Self {
            mass,
            sequence,
            partition: None,
            amino_acid_counts: OnceCell::new(),
        }
    }

    pub fn partition(&self) -> Option<i32> {
        self.partition
    }

    pub fn set_partition(&mut self, partitioning: &Partitioning) -> Result<(), Error> {
        self.partition = partitioning.get(&self.mass).cloned();
        self.partition
            .ok_or(Error::NoPartition(self.sequence().to_string(), self.mass))?;
        Ok(())
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

    pub fn amino_acid_count_by_bit_code(&self, code: AminoAcidBitCode) -> u8 {
        let amino_acid = AminoAcid::by_bit_code(&code);
        self.amino_acid_count(amino_acid)
    }

    pub async fn insert(&self, client: &Client) -> Result<(), Error> {
        let stmt = client
            .get_prepared_statement(INSERT_STATEMENT.as_str())
            .await?;
        client
            .execute_unpaged(&stmt, &self)
            .await
            .map_err(|err| Error::CqlExecution(Box::new(err)))?;
        Ok(())
    }

    pub async fn insert_batch(
        client: &Client,
        values: impl Iterator<Item = Self>,
    ) -> Result<(), Error> {
        let stmt = client
            .get_prepared_statement(INSERT_STATEMENT.as_str())
            .await?;

        let insert_futures = values.map(|value| client.execute_unpaged(&stmt, value));

        join_all(insert_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, ExecutionError>>()
            .map_err(|err| Error::CqlExecution(Box::new(err)))?;

        Ok(())
    }

    pub async fn select(client: &Client) -> Result<TypedRowStream<Self>, Error> {
        let stmt = client
            .get_prepared_statement(SELECT_STATEMENT.as_str())
            .await?;

        Ok(client
            .execute_iter(stmt, ())
            .await
            .map_err(|err| Error::CqlPagedExecution(Box::new(err)))?
            .rows_stream::<Self>()?)
    }

    pub fn to_peptide_mass(sequence: &Sequence) -> i64 {
        sequence
            .amino_acids()
            .fold(WATER_MONO_MASS, |acc, amino_acid| {
                acc + amino_acid.mono_mass()
            })
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
