use std::{num::NonZeroUsize, sync::LazyLock};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::client::Client;

static MAX_BLOB_PART_SIZE: usize = 500_000; // 500 kB
static MAX_BLOB_SIZE: usize = 2_usize.pow(16) * MAX_BLOB_PART_SIZE;
static TABLE_NAME: &str = "blobs";

static UPSERT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "INSERT INTO {TABLE_NAME} (key, part, data) VALUES ($1, $2, $3) ON CONFLICT (key, part) DO UPDATE SET data = EXCLUDED.data"
    )
});

static SELECT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!("SELECT key, part, data FROM {TABLE_NAME} WHERE key = $1 ORDER BY part")
});

static DELETE_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("DELETE FROM {TABLE_NAME} WHERE key = $1 AND part > $2"));

#[derive(Debug, Error)]
pub enum Error {
    #[error("The given data exceeds the maximum blob size {MAX_BLOB_SIZE} with {0}")]
    BlobTooLarge(usize),
    #[error("Client error in blob: {0}")]
    Client(#[from] crate::client::Error),
    #[error("Row decoding error in blob: {0}")]
    Row(#[from] tokio_postgres::Error),
    #[error("Deserialization error in blob")]
    Deserialize,
    #[error("Serialization error in blob")]
    Serialize,
}

pub trait IsBlob: Send + Sync + Serialize + for<'a> Deserialize<'a> {
    fn key(&self) -> &str;
}

#[derive(Debug, Clone)]
struct BlobPart {
    key: String,
    part: i16,
    data: Vec<u8>,
}

impl BlobPart {
    fn from_row(row: &tokio_postgres::Row) -> Result<Self, Error> {
        Ok(Self {
            key: row.try_get("key")?,
            part: row.try_get("part")?,
            data: row.try_get("data")?,
        })
    }

    pub async fn upsert_batch(client: &Client, values: Vec<Self>) -> Result<(), Error> {
        // Blobs are tiny (the config), so insert parts sequentially — the bound
        // param array is a borrowed temporary that must live across each await.
        for value in &values {
            client
                .execute(
                    UPSERT_STATEMENT.as_str(),
                    &[&value.key, &value.part, &value.data],
                )
                .await?;
        }
        Ok(())
    }

    pub async fn select_by_key(client: &Client, key: &str) -> Result<Vec<Self>, Error> {
        let rows = client.query(SELECT_STATEMENT.as_str(), &[&key]).await?;
        rows.iter().map(Self::from_row).collect()
    }

    /// Deletes leftover parts above `part` (used to truncate a previously larger blob).
    pub async fn delete_above(client: &Client, key: &str, part: i16) -> Result<(), Error> {
        client
            .execute(DELETE_STATEMENT.as_str(), &[&key, &part])
            .await?;
        Ok(())
    }
}

pub struct BlobTable;

impl BlobTable {
    pub async fn insert<T: IsBlob>(
        client: &Client,
        blob: &T,
        concurrent_batch_size: NonZeroUsize,
    ) -> Result<(), Error> {
        let data = postcard::to_allocvec(blob).map_err(|_| Error::Serialize)?;

        if data.len() > MAX_BLOB_SIZE {
            return Err(Error::BlobTooLarge(data.len()));
        }

        // Starting with i16::MAX and using wrapping_add to start with i16::MIN in first iterations
        // just return BlobPart directly and to create and store blob part temprorarily in memory so I can increase the
        // counter afterwards. I know. It is kinda stupid.
        let mut part_ctr = i16::MAX;

        let blob_parts = data
            .into_iter()
            .chunks(MAX_BLOB_PART_SIZE)
            .into_iter()
            .map(|data_chunk| {
                part_ctr = part_ctr.wrapping_add(1);
                BlobPart {
                    key: blob.key().to_string(),
                    part: part_ctr, // TODO: implement part numbering
                    data: data_chunk.collect(),
                }
            })
            .collect::<Vec<_>>();

        BlobPart::delete_above(client, blob.key(), part_ctr).await?;

        for blob_part_batch in &blob_parts.into_iter().chunks(concurrent_batch_size.get()) {
            BlobPart::upsert_batch(client, blob_part_batch.collect()).await?;
        }

        Ok(())
    }

    pub async fn select<T: IsBlob>(client: &Client, key: &str) -> Result<Option<T>, Error> {
        let mut blob_parts = BlobPart::select_by_key(client, key).await?;

        if blob_parts.is_empty() {
            return Ok(None);
        }

        blob_parts.sort_by_key(|blob_part| blob_part.part);

        let data = blob_parts
            .into_iter()
            .flat_map(|blob_part| blob_part.data)
            .collect::<Vec<_>>();

        postcard::from_bytes::<T>(&data)
            .map_err(|_| Error::Deserialize)
            .map(Some)
    }
}
