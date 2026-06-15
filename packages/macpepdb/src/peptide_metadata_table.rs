//! `peptide_metadata` — deduplicated protein-id sets shared across peptides.
//!
//! Each distinct protein-id set is stored once with a surrogate `metadata_id`; peptides
//! reference it instead of carrying their own copy. The build interns sets into this table
//! (see `peptide_table::build_concurrently`); reads resolve a `metadata_id` back to its
//! `protein_ids` (single-peptide GET endpoint).

use std::{collections::HashMap, sync::Arc};

use postgres_types::Type;
use std::sync::LazyLock;
use thiserror::Error;

use crate::{client::Client, protein_ids::ProteinIds};

pub const TABLE_NAME: &str = "peptide_metadata";

pub const COLUMNS: &str = "metadata_id, protein_ids";

static COPY_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("COPY {TABLE_NAME} ({COLUMNS}) FROM STDIN (FORMAT binary)"));

/// Column types for the binary COPY into `peptide_metadata`, in column order.
static COPY_TYPES: LazyLock<[Type; 2]> = LazyLock::new(|| [Type::INT8, Type::BYTEA]);

static SELECT_BY_IDS_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT {COLUMNS} FROM {TABLE_NAME} WHERE metadata_id = ANY($1)"));

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in peptide metadata table: {0}")]
    Client(#[from] crate::client::Error),
    #[error("Row decoding error in peptide metadata table: {0}")]
    Row(#[from] tokio_postgres::Error),
}

pub struct PeptideMetadataTable {
    client: Arc<Client>,
}

impl PeptideMetadataTable {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    /// Bulk-loads a batch of newly-interned metadata rows via one binary COPY transaction.
    /// `metadata_id` is a content-addressed surrogate, so rows are independent and order
    /// does not matter.
    pub async fn insert_batch(&self, rows: &[(i64, ProteinIds)]) -> Result<(), Error> {
        if rows.is_empty() {
            return Ok(());
        }

        self.client
            .run_congested(|| async {
                let mut copy = self
                    .client
                    .copy_in_binary(COPY_STATEMENT.as_str(), COPY_TYPES.as_slice())
                    .await?;
                for (metadata_id, protein_ids) in rows {
                    copy.write(&[metadata_id, protein_ids]).await?;
                }
                copy.finish().await?;
                Ok::<(), crate::client::Error>(())
            })
            .await?;

        Ok(())
    }

    /// Resolves a batch of `metadata_id`s to their protein-id sets (`WHERE metadata_id =
    /// ANY($1)` — Citus shard pruning + per-shard PK lookup). Returns a map so callers can
    /// attach the set to each peptide that references it.
    pub async fn select_by_ids(&self, ids: &[i64]) -> Result<HashMap<i64, ProteinIds>, Error> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let ids_vec = ids.to_vec();
        let rows = self
            .client
            .query(SELECT_BY_IDS_STATEMENT.as_str(), &[&ids_vec])
            .await?;
        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            let metadata_id: i64 = row.try_get("metadata_id")?;
            let protein_ids: ProteinIds = row.try_get("protein_ids")?;
            out.insert(metadata_id, protein_ids);
        }
        Ok(out)
    }
}
