use super::{client::Client, table::Table};
use anyhow::Result;
use futures::Future;

pub trait BlobTable<'a, C, I>: Table
where
    C: Client<'a, I>,
{
    /// New
    ///
    fn new(client: &'a C) -> Self;

    /// Inserts the given data into the blob table.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    /// * `key_prefix` - The key prefix to use for the data
    /// * `data` - The data to insert
    ///
    fn insert_raw(&self, key_prefix: &str, data: &[u8]) -> impl Future<Output = Result<()>> + Send;

    /// Selects the raw data chunks from the blob table and returns it as a single vector.
    /// The data is sorted by the index of the chunk.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    /// * `key_prefix` - The key prefix to select
    ///
    fn select_raw(&self, key_prefix: &str) -> impl Future<Output = Result<Vec<u8>>> + Send;

    /// Deletes the records for the given key prefix.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    /// * `key_prefix` - The key prefix to delete
    ///
    fn delete(&self, key_prefix: &str) -> impl Future<Output = Result<()>> + Send;
}
