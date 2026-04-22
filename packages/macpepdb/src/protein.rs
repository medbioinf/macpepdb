use std::sync::LazyLock;

use futures::future::join_all;
use scylla::{DeserializeRow, SerializeRow, client::pager::TypedRowStream, errors::ExecutionError};
use thiserror::Error;

use crate::{client::Client, sequence::ProteinSequence as Sequence};

static TABLE_NAME: &str = "proteins";

static INSERT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("INSERT INTO {TABLE_NAME} (accession, sequence) VALUES (?, ?)"));

static SELECT_STATEMENT: LazyLock<String> = LazyLock::new(|| format!("SELECT * FROM {TABLE_NAME}"));

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in protein: {0}")]
    Client(#[from] crate::client::Error),
    #[error("CQL execution error in protein: {0}")]
    CqlExecution(#[from] scylla::errors::ExecutionError),
    #[error("CQL paged execution error in protein: {0}")]
    CqlPagedExecution(#[from] scylla::errors::PagerExecutionError),
    #[error("CQL type check failed in protein: {0}")]
    CqlTypeCheck(#[from] scylla::errors::TypeCheckError),
    #[error("Sequence error in protein: {0}")]
    Sequence(#[from] crate::sequence::Error),
}

#[derive(Debug, DeserializeRow, SerializeRow)]
pub struct Protein {
    accession: String,
    sequence: Sequence,
}

impl Protein {
    pub fn new(accession: String, sequence: Sequence) -> Self {
        Self {
            accession,
            sequence,
        }
    }

    pub fn accession(&self) -> &str {
        &self.accession
    }

    pub fn sequence(&self) -> &Sequence {
        &self.sequence
    }

    pub async fn insert(&self, client: &Client) -> Result<(), Error> {
        let stmt = client
            .get_prepared_statement(INSERT_STATEMENT.as_str())
            .await?;
        client.execute_unpaged(&stmt, &self).await?;
        Ok(())
    }

    pub async fn insert_batch(
        client: &Client,
        proteins: impl Iterator<Item = Self>,
    ) -> Result<(), Error> {
        let stmt = client
            .get_prepared_statement(INSERT_STATEMENT.as_str())
            .await?;

        let insert_futures = proteins.map(|protein| client.execute_unpaged(&stmt, protein));

        join_all(insert_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, ExecutionError>>()?;

        Ok(())
    }

    pub async fn select(client: &Client) -> Result<TypedRowStream<Self>, Error> {
        let stmt = client
            .get_prepared_statement(SELECT_STATEMENT.as_str())
            .await?;

        Ok(client.execute_iter(stmt, ()).await?.rows_stream::<Self>()?)
    }
}

impl TryFrom<&uniprot_reader::entry::Entry> for Protein {
    type Error = Error;

    fn try_from(entry: &uniprot_reader::entry::Entry) -> Result<Self, Error> {
        let accession = entry
            .accession()
            .find(';')
            .map(|pos| entry.accession()[..pos].trim().to_string())
            .unwrap_or(entry.accession().to_string());

        Ok(Self {
            accession,
            sequence: Sequence::try_from(entry.sequence())?,
        })
    }
}
