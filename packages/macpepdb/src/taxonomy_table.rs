use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use deadpool_postgres::Transaction;
use futures::{Stream, StreamExt};
use postgres_types::{ToSql, Type};
use thiserror::Error;
use tokio_postgres::{CopyInSink, binary_copy::BinaryCopyInWriter};

use crate::{
    client::Client,
    taxonomy::Taxonomy,
    taxonomy_rank::SPECIES,
    taxonomy_rank_table::{
        ID_COL as TAXONOMY_RANK_ID_COL, NAME_COL as TAXONOMY_RANK_NAME_COL,
        TABLE_NAME as TAXONOMY_RANK_TABLE,
    },
};

pub static INSERTED_TAXONOMIES_METRIC: &str = "taxonomy_table::build::inserted_taxonomies";

pub static TABLE_NAME: &str = "taxonomies";

pub static ID_COL: &str = "id";
pub static PARENT_ID_COL: &str = "parent_id";
pub static SCIENTIFIC_NAME_COL: &str = "scientific_name";
pub static RANK_ID_COL: &str = "rank_id";

// Alias for the rank name colum when joining with the taxonomy rank table to avoid ambiguity in the SELECT statement.
pub static RANK_NAME_ALIAS_COL: &str = "rank_name";

static COPY_TYPES: [Type; 4] = [
    Type::INT4,    // id
    Type::INT4,    // parent_id
    Type::VARCHAR, // scientific_name
    Type::INT2,    // rank_id
];

static COPY_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "COPY {TABLE_NAME} ({ID_COL}, {PARENT_ID_COL}, {SCIENTIFIC_NAME_COL}, {RANK_ID_COL}) FROM STDIN (FORMAT binary)"
    )
});

static SELECT_WITH_RANK_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {TABLE_NAME}.{ID_COL}, {PARENT_ID_COL}, {SCIENTIFIC_NAME_COL}, {RANK_ID_COL}, {TAXONOMY_RANK_TABLE}.{TAXONOMY_RANK_NAME_COL} AS {RANK_NAME_ALIAS_COL} FROM {TABLE_NAME} JOIN {TAXONOMY_RANK_TABLE} ON {RANK_ID_COL} = {TAXONOMY_RANK_TABLE}.{TAXONOMY_RANK_ID_COL}"
    )
});

static SELECT_SUB_TAXONOMIES_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "WITH RECURSIVE subtaxonomies AS (
            SELECT {ID_COL}, {PARENT_ID_COL}, {SCIENTIFIC_NAME_COL}, {RANK_ID_COL} FROM {TABLE_NAME} \
            WHERE id = $1 \
                UNION \
                    SELECT t.{ID_COL}, t.{PARENT_ID_COL}, t.{SCIENTIFIC_NAME_COL}, t.{RANK_ID_COL} \
                    FROM {TABLE_NAME} t \
                    INNER JOIN subtaxonomies s ON s.id = t.parent_id \
        ) SELECT {ID_COL}, {PARENT_ID_COL}, {SCIENTIFIC_NAME_COL}, {RANK_ID_COL} FROM subtaxonomies WHERE {RANK_ID_COL} = (SELECT {TAXONOMY_RANK_ID_COL} FROM {TAXONOMY_RANK_TABLE} WHERE {TAXONOMY_RANK_NAME_COL} = '{SPECIES}');"
    )
});

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in taxonomy rank table: {0}")]
    Client(Box<crate::client::Error>),
    #[error("Row decoding error in taxonomy table: {0}")]
    Row(Box<tokio_postgres::Error>),
    #[error("Taxonomy error in taxonomy table: {0}")]
    Taxonomy(Box<crate::taxonomy::Error>),
}

into_thiserror_boxed!(crate::client::Error, Error, Client);
into_thiserror_boxed!(tokio_postgres::Error, Error, Row);
into_thiserror_boxed!(crate::taxonomy::Error, Error, Taxonomy);

pub struct TaxonomyTable {
    client: Arc<Client>,
}

impl TaxonomyTable {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    /// Bulk-loads a ranks via one binary COPY transaction.
    async fn insert_batch<'a>(
        &self,
        transaction: &Transaction<'a>,
        taxonomies: &[Taxonomy],
    ) -> Result<usize, Error> {
        if taxonomies.is_empty() {
            return Ok(0);
        }

        self.client
            .run_congested(|| async {
                let sink: CopyInSink<Bytes> = transaction.copy_in(COPY_STATEMENT.as_str()).await?;
                let mut writer = Box::pin(BinaryCopyInWriter::new(sink, &COPY_TYPES));
                for taxonomy in taxonomies {
                    writer
                        .as_mut()
                        .write(&[
                            taxonomy.id_as_ref() as &(dyn ToSql + Sync),
                            taxonomy.parent_id_as_ref() as &(dyn ToSql + Sync),
                            taxonomy.scientific_name() as &(dyn ToSql + Sync),
                            taxonomy.rank_id_as_ref() as &(dyn ToSql + Sync),
                        ])
                        .await?;
                }
                writer.as_mut().finish().await?;
                Ok::<(), crate::client::Error>(())
            })
            .await?;

        Ok(taxonomies.len())
    }

    pub async fn select_with_rank(
        &self,
        where_clause: &str,
        params: Vec<Box<dyn ToSql + Sync + Send>>,
    ) -> Result<impl Stream<Item = Result<Taxonomy, Error>> + Send + use<>, Error> {
        let statement = format!("{} {where_clause}", SELECT_WITH_RANK_STATEMENT.as_str());
        let stream = self.client.query_stream(&statement, params).await?;
        Ok(stream.map(|row_res| {
            row_res
                .map_err(Error::from)
                .and_then(|row| Taxonomy::try_from(row).map_err(Error::from))
        }))
    }

    pub async fn select_sub_species(
        &self,
        taxonomy_id: i32,
    ) -> Result<impl Stream<Item = Result<Taxonomy, Error>> + Send + use<>, Error> {
        let stream = self
            .client
            .query_stream(
                SELECT_SUB_TAXONOMIES_STATEMENT.as_str(),
                vec![Box::new(taxonomy_id)],
            )
            .await?;
        Ok(stream.map(|row_res| {
            row_res.map_err(Error::from).and_then(|row| {
                Taxonomy::try_from(row)
                    .map(|mut taxonomy| {
                        *taxonomy.rank_name_mut() = Some(String::from(SPECIES)); // not squeezing a join into the recursive query, just add the rank here manually
                        taxonomy
                    })
                    .map_err(Error::from)
            })
        }))
    }

    pub async fn build(&self, taxonomies: Vec<Taxonomy>) -> Result<(), Error> {
        let counter = metrics::counter!(INSERTED_TAXONOMIES_METRIC);

        let mut conn = self.client.get().await?;
        let transaction = conn.transaction().await?;

        transaction
            .execute("SET CONSTRAINTS taxonomies_parent_id_fkey DEFERRED", &[])
            .await?;

        for chunk in taxonomies.chunks(20_000) {
            let count = self.insert_batch(&transaction, chunk).await?;
            counter.increment(count as u64);
        }

        transaction.commit().await?;
        Ok(())
    }
}
