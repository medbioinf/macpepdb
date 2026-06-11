use std::sync::{Arc, LazyLock};

use thiserror::Error;

use crate::{client::Client, tools};

pub static TABLE_NAME: &str = "stats";

static UPSERT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "INSERT INTO {TABLE_NAME} (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
    )
});

static SELECT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT value FROM {TABLE_NAME} WHERE key = $1 LIMIT 1"));

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in stats table: {0}")]
    Client(#[from] crate::client::Error),
    #[error("Row decoding error in stats table: {0}")]
    Row(#[from] tokio_postgres::Error),
}

static PROTEIN_COUNT: &str = "protein_count";
static PEPTIDE_COUNT: &str = "peptide_count";
static MASS_COUNT: &str = "mass_count";

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
            .execute(UPSERT_STATEMENT.as_str(), &[&key, &value])
            .await?;
        Ok(())
    }

    async fn select(&self, key: &str) -> Result<Option<i64>, Error> {
        let rows = self
            .client
            .query(SELECT_STATEMENT.as_str(), &[&key])
            .await?;
        match rows.first() {
            Some(row) => Ok(Some(row.try_get::<_, i64>(0)?)),
            None => Ok(None),
        }
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

    pub async fn upsert_mass_count(&self, count: usize) -> Result<(), Error> {
        self.upsert(MASS_COUNT, tools::usize_to_i64(count)).await
    }

    pub async fn select_mass_count(&self) -> Result<Option<usize>, Error> {
        Ok(self.select(MASS_COUNT).await?.map(tools::i64_to_usize))
    }
}
