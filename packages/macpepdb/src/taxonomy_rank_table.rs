use std::sync::{Arc, LazyLock};

use postgres_types::Type;
use thiserror::Error;

use crate::{client::Client, taxonomy_rank::TaxonomyRank};

pub static INSERTED_RANKS_METRIC: &str = "taxonomy_rank_table::build::inserted_ranks";

pub static TABLE_NAME: &str = "taxonomy_ranks";

pub static ID_COL: &str = "id";
pub static NAME_COL: &str = "name";

static COPY_TYPES: [Type; 2] = [
    Type::INT2,    // id
    Type::VARCHAR, // name
];

static COPY_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!("COPY {TABLE_NAME} ({ID_COL}, {NAME_COL}) FROM STDIN (FORMAT binary)")
});

// static SELECT_STATEMENT: LazyLock<String> =
//     LazyLock::new(|| format!("SELECT {ID_COL}, {NAME_COL} FROM {TABLE_NAME}"));

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in taxonomy rank table: {0}")]
    Client(Box<crate::client::Error>),
    #[error("Taxonomy rank error in taxonomy rank table: {0}")]
    Taxonomy(Box<crate::taxonomy_rank::Error>),
}

into_thiserror_boxed!(crate::client::Error, Error, Client);
into_thiserror_boxed!(crate::taxonomy_rank::Error, Error, Taxonomy);

pub struct TaxonomyRankTable {
    client: Arc<Client>,
}

impl TaxonomyRankTable {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    /// Bulk-loads a ranks via one binary COPY transaction.
    async fn insert_batch(&self, ranks: &[TaxonomyRank]) -> Result<usize, Error> {
        if ranks.is_empty() {
            return Ok(0);
        }

        self.client
            .run_congested(|| async {
                let mut copy = self
                    .client
                    .copy_in_binary(COPY_STATEMENT.as_str(), &COPY_TYPES)
                    .await?;
                for rank in ranks {
                    copy.write(&[rank.id_as_ref(), rank.name()]).await?;
                }
                copy.finish().await?;
                Ok::<(), crate::client::Error>(())
            })
            .await?;

        Ok(ranks.len())
    }

    pub async fn build(&self, ranks: Vec<TaxonomyRank>) -> Result<(), Error> {
        let counter = metrics::counter!(INSERTED_RANKS_METRIC);
        let count = self.insert_batch(&ranks).await?;
        counter.increment(count as u64);
        Ok(())
    }
}
