use std::{num::NonZeroUsize, sync::LazyLock};

use futures::{TryStreamExt, future::join_all};

use itertools::Itertools;
use scylla::{
    DeserializeRow, SerializeRow, client::pager::TypedRowStream, errors::ExecutionError,
    serialize::row::SerializeRow,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::client::Client;

static MAX_BLOB_PART_SIZE: usize = 500_000; // 500 kB
static MAX_BLOB_SIZE: usize = 2_usize.pow(16) * MAX_BLOB_PART_SIZE;
static TABLE_NAME: &str = "blobs";

static UPSERT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("UPDATE {TABLE_NAME} SET data = ? WHERE key = ? AND part = ?"));

static SELECT_STATEMENT: LazyLock<String> = LazyLock::new(|| format!("SELECT * FROM {TABLE_NAME}"));

static DELETE_STATEMENT: LazyLock<String> = LazyLock::new(|| format!("DELETE FROM {TABLE_NAME}"));

#[derive(Debug, Error)]
pub enum Error {
    #[error("The given data exceeds the maximum blob size {MAX_BLOB_SIZE} with {0}")]
    BlobTooLarge(usize),
    #[error("Client error in blob: {0}")]
    Client(#[from] crate::client::Error),
    #[error("CQL execution error in blob: {0}")]
    CqlExecution(#[from] scylla::errors::ExecutionError),
    #[error("CQL next row error in blob: {0}")]
    CqlNextRow(#[from] scylla::errors::NextRowError),
    #[error("CQL paged execution error in blob: {0}")]
    CqlPagedExecution(#[from] scylla::errors::PagerExecutionError),
    #[error("Unable to prepare statement `{0}`: {1}")]
    CqlPrepare(String, Box<scylla::errors::PrepareError>),
    #[error("CQL type check failed in blob: {0}")]
    CqlTypeCheck(#[from] scylla::errors::TypeCheckError),
    #[error("Deserialization error in blob")]
    Deserialize,
    #[error("Serialization error in blob")]
    Serialize,
}

pub trait IsBlob: Send + Sync + Serialize + for<'a> Deserialize<'a> {
    fn key(&self) -> &str;
}

#[derive(Debug, Clone, DeserializeRow, SerializeRow)]
struct BlobPart {
    key: String,
    part: i16,
    data: Vec<u8>,
}

impl BlobPart {
    pub async fn upsert_batch(
        client: &Client,
        values: impl Iterator<Item = Self>,
    ) -> Result<(), Error> {
        let stmt = client
            .get_prepared_statement(UPSERT_STATEMENT.as_str())
            .await?;

        let insertion_futures = values.map(|value| client.execute_unpaged(&stmt, value));

        join_all(insertion_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, ExecutionError>>()?;

        Ok(())
    }

    pub async fn select(
        client: &Client,
        select_addition: Option<&str>,
        values: impl SerializeRow,
    ) -> Result<TypedRowStream<Self>, Error> {
        let statement = select_addition
            .map(|addition| format!("{} {}", SELECT_STATEMENT.as_str(), addition))
            .unwrap_or_else(|| SELECT_STATEMENT.as_str().to_string());

        Ok(client
            .query_iter(statement.as_str(), values)
            .await?
            .rows_stream::<Self>()?)
    }

    pub async fn delete(
        client: &Client,
        select_addition: Option<&str>,
        values: impl SerializeRow,
    ) -> Result<(), Error> {
        let statement = select_addition
            .map(|addition| format!("{} {}", DELETE_STATEMENT.as_str(), addition))
            .unwrap_or_else(|| DELETE_STATEMENT.as_str().to_string());

        let stmt = client
            .prepare(statement.as_str())
            .await
            .map_err(|err| Error::CqlPrepare(statement.clone(), Box::new(err)))?;

        client.execute_iter(stmt, values).await?;

        Ok(())
    }
}

pub struct Blob;

impl Blob {
    pub async fn insert<T: IsBlob>(
        client: &Client,
        blob: &T,
        insert_batch_size: NonZeroUsize,
    ) -> Result<(), Error> {
        let data = postcard::to_allocvec(blob).map_err(|_| Error::Serialize)?;

        if data.len() > MAX_BLOB_SIZE {
            return Err(Error::BlobTooLarge(data.len()));
        }

        let mut part_ctr = i16::MAX;

        let blob_parts = data
            .into_iter()
            .chunks(MAX_BLOB_PART_SIZE)
            .into_iter()
            .map(|data_chunk| {
                part_ctr += 1;
                BlobPart {
                    key: blob.key().to_string(),
                    part: part_ctr, // TODO: implement part numbering
                    data: data_chunk.collect(),
                }
            })
            .collect::<Vec<_>>();

        BlobPart::delete(
            client,
            Some("WHERE key = ? AND part > ?"),
            (blob.key(), part_ctr),
        )
        .await?;

        for blob_part_batch in &blob_parts.into_iter().chunks(insert_batch_size.get()) {
            BlobPart::upsert_batch(client, blob_part_batch).await?;
        }

        Ok(())
    }

    pub async fn select<T: IsBlob>(client: &Client, key: &str) -> Result<Option<T>, Error> {
        let mut blob_parts = BlobPart::select(client, Some("WHERE key = ?"), (key,))
            .await?
            .try_collect::<Vec<_>>()
            .await?;

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
