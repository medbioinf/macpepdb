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

/// Metric name for the counter tracking how many taxonomies have been inserted during
/// [`TaxonomyTable::build`].
pub static INSERTED_TAXONOMIES_METRIC: &str = "taxonomy_table::build::inserted_taxonomies";

/// Name of the `taxonomies` table.
pub static TABLE_NAME: &str = "taxonomies";

/// Name of the `taxonomies.id` column.
pub static ID_COL: &str = "id";
/// Name of the `taxonomies.parent_id` column.
pub static PARENT_ID_COL: &str = "parent_id";
/// Name of the `taxonomies.scientific_name` column.
pub static SCIENTIFIC_NAME_COL: &str = "scientific_name";
/// Name of the `taxonomies.rank_id` column.
pub static RANK_ID_COL: &str = "rank_id";

/// Alias for the rank name column when joining with the taxonomy rank table, to avoid
/// ambiguity in the SELECT statement.
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

static SELECT_ID_NAME_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT {ID_COL}, {SCIENTIFIC_NAME_COL} FROM {TABLE_NAME}"));

/// Recursive CTE query to select a taxonomy and all its descendants (any rank), walking the
/// `parent_id` tree downward. Proteins can be attached to nodes of any rank (species, strain,
/// isolate, `no rank`, ...), so no rank filtering is applied.
static SELECT_SUB_TAXONOMIES_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "WITH RECURSIVE subtaxonomies AS (
            SELECT {ID_COL}, {PARENT_ID_COL}, {SCIENTIFIC_NAME_COL}, {RANK_ID_COL} FROM {TABLE_NAME} \
            WHERE id = $1 \
                UNION \
                    SELECT t.{ID_COL}, t.{PARENT_ID_COL}, t.{SCIENTIFIC_NAME_COL}, t.{RANK_ID_COL} \
                    FROM {TABLE_NAME} t \
                    INNER JOIN subtaxonomies s ON s.id = t.parent_id \
        ) SELECT s.{ID_COL}, s.{PARENT_ID_COL}, s.{SCIENTIFIC_NAME_COL}, s.{RANK_ID_COL}, {TAXONOMY_RANK_TABLE}.{TAXONOMY_RANK_NAME_COL} AS {RANK_NAME_ALIAS_COL} \
        FROM subtaxonomies s JOIN {TAXONOMY_RANK_TABLE} ON s.{RANK_ID_COL} = {TAXONOMY_RANK_TABLE}.{TAXONOMY_RANK_ID_COL};"
    )
});

/// Like [`SELECT_SUB_TAXONOMIES_STATEMENT`], but only returns `(id, group_id)`. The group is the
/// nearest species-rank ancestor-or-self within the subtree, falling back to the root of the walk.
static SELECT_SUB_TAXONOMIES_GROUPED_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "WITH RECURSIVE subtaxonomies AS (
            SELECT t.{ID_COL}, CASE WHEN r.{TAXONOMY_RANK_NAME_COL} = '{SPECIES}' THEN t.{ID_COL} ELSE NULL END AS group_id \
            FROM {TABLE_NAME} t JOIN {TAXONOMY_RANK_TABLE} r ON t.{RANK_ID_COL} = r.{TAXONOMY_RANK_ID_COL} \
            WHERE t.{ID_COL} = $1 \
                UNION \
                    SELECT c.{ID_COL}, CASE WHEN cr.{TAXONOMY_RANK_NAME_COL} = '{SPECIES}' THEN c.{ID_COL} ELSE s.group_id END \
                    FROM {TABLE_NAME} c \
                    JOIN {TAXONOMY_RANK_TABLE} cr ON c.{RANK_ID_COL} = cr.{TAXONOMY_RANK_ID_COL} \
                    INNER JOIN subtaxonomies s ON s.{ID_COL} = c.{PARENT_ID_COL} \
        ) SELECT {ID_COL}, COALESCE(group_id, $1) FROM subtaxonomies;"
    )
});

/// Errors occurring while reading, writing, or building the `taxonomies` table.
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

/// Handle for reading, writing, and building the `taxonomies` table.
pub struct TaxonomyTable {
    client: Arc<Client>,
}

impl TaxonomyTable {
    /// Creates a new `TaxonomyTable` bound to `client`.
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

    /// Streams taxonomies matching `where_clause` (e.g. `WHERE id = $1`), binding `params`
    /// positionally. Each row is joined against `taxonomy_ranks` to include the rank name.
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

    /// Resolves each id in `ids` to its `(id, scientific_name)` pair (`WHERE id = ANY($1)`).
    pub async fn resolve_ids(
        &self,
        ids: Vec<i32>,
    ) -> Result<impl Stream<Item = Result<(i32, String), Error>> + Send + use<>, Error> {
        let statement = format!("{} WHERE id = ANY($1)", SELECT_ID_NAME_STATEMENT.as_str());

        let stream = self
            .client
            .query_stream(statement.as_str(), vec![Box::new(ids)])
            .await?;
        Ok(stream.map(|row_res| {
            let row = row_res.map_err(Error::from)?;
            let id = row.try_get::<_, i32>(0).map_err(Error::from)?;
            let scientific_name = row.try_get::<_, String>(1).map_err(Error::from)?;
            Ok((id, scientific_name))
        }))
    }

    /// Streams `taxonomy_id` and all its descendants (any rank) by walking the `parent_id` tree
    /// downward with a recursive CTE. Each row includes its rank name.
    ///
    /// # Arguments
    /// * `taxonomy_id` - Id of the ancestor taxonomy whose descendants to find
    pub async fn select_sub_taxonomies(
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
            row_res
                .map_err(Error::from)
                .and_then(|row| Taxonomy::try_from(row).map_err(Error::from))
        }))
    }

    /// Streams `(id, group_id)` for `taxonomy_id` and all its descendants (any rank). The group is
    /// the nearest species-rank ancestor-or-self within the subtree, or `taxonomy_id` itself if there
    /// is none, so e.g. strains of one species share a group.
    ///
    /// # Arguments
    /// * `taxonomy_id` - Id of the ancestor taxonomy whose descendants to find
    pub async fn select_sub_taxonomies_grouped(
        &self,
        taxonomy_id: i32,
    ) -> Result<impl Stream<Item = Result<(i32, i32), Error>> + Send + use<>, Error> {
        let stream = self
            .client
            .query_stream(
                SELECT_SUB_TAXONOMIES_GROUPED_STATEMENT.as_str(),
                vec![Box::new(taxonomy_id)],
            )
            .await?;
        Ok(stream.map(|row_res| {
            let row = row_res.map_err(Error::from)?;
            Ok((
                row.try_get::<_, i32>(0).map_err(Error::from)?,
                row.try_get::<_, i32>(1).map_err(Error::from)?,
            ))
        }))
    }

    /// Builds the `taxonomies` table by inserting `taxonomies` in a single transaction, in
    /// 20,000-row chunks. The `parent_id` foreign key check is deferred to commit time so rows
    /// can be inserted in any order regardless of parent/child ordering.
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
