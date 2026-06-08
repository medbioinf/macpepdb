use std::sync::{Arc, LazyLock};

use futures::StreamExt;
use thiserror::Error;

use crate::{client::Client, tools};

pub static TABLE_NAME: &str = "stats";

static UPSERT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("UPDATE {TABLE_NAME} SET value = ? WHERE key = ?"));

static SELECT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT value FROM {TABLE_NAME} WHERE key = ? LIMIT 1"));

#[derive(Debug, Error)]
pub enum Error {
    #[error("CQL execution error in stats table: {0}")]
    CqlExecution(#[from] scylla::errors::ExecutionError),
    #[error("CQL next row error in stats table: {0}")]
    CqlNextRow(#[from] scylla::errors::NextRowError),
    #[error("CQL paged execution error in stats table: {0}")]
    CqlPagedExecution(#[from] scylla::errors::PagerExecutionError),
    #[error("Unable to prepare statement: `{0}`")]
    CqlPrepare(#[from] scylla::errors::PrepareError),
    #[error("CQL type check failed in stats table: {0}")]
    CqlTypeCheck(#[from] scylla::errors::TypeCheckError),
}

static PROTEIN_COUNT: &str = "protein_count";
static PEPTIDE_COUNT: &str = "peptide_count";

pub struct StatsTable {
    client: Arc<Client>,
}

impl StatsTable {
    pub fn table_name() -> &'static str {
        TABLE_NAME
    }

    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    async fn upsert(&self, key: &str, value: i64) -> Result<(), Error> {
        self.client
            .as_ref()
            .execute_unpaged(UPSERT_STATEMENT.as_str(), (value, key))
            .await?;

        Ok(())
    }

    async fn select(&self, key: &str) -> Result<Option<i64>, Error> {
        let mut stream = self
            .client
            .as_ref()
            .execute_iter(SELECT_STATEMENT.as_str(), (key,))
            .await?
            .rows_stream::<(i64,)>()?;

        Ok(stream.next().await.transpose()?.map(|row| row.0))
    }

    pub async fn upsert_protein_count(&self, count: usize) -> Result<(), Error> {
        self.upsert(PROTEIN_COUNT, tools::usize_to_i64(count)).await
    }

    pub async fn select_protein_count(&self) -> Result<Option<usize>, Error> {
        Ok(self.select(PROTEIN_COUNT).await?.map(tools::i64_to_usize))
    }

    pub async fn upsert_peptide_count(&self, count: usize) -> Result<(), Error> {
        self.upsert(PEPTIDE_COUNT, tools::usize_to_i64(count)).await
    }

    pub async fn select_peptide_count(&self) -> Result<Option<usize>, Error> {
        Ok(self.select(PEPTIDE_COUNT).await?.map(tools::i64_to_usize))
    }
}
