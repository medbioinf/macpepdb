// 3rd party imports
use anyhow::{Context, Result};
use futures::{StreamExt, TryStreamExt};

// internal imports
use crate::database::scylla::client::{Client, GenericClient};

/// Max size of a blob in bytes (512 kB)
///
pub const MAX_BLOB_SIZE: usize = 512000;

/// Table name for the blob table
///
pub const TABLE_NAME: &'static str = "blobs";

/// Columns of the blob table
///
pub const COLUMNS: &'static str = "key, data";

/// Max pages per select (avoids timeouts)
///
pub const MAX_PAGES_PER_SELECT: i32 = 1000;

/// Trait for using the blob table.
/// The blob table is used to store binary data in the database like the JSON-serialized taxonomy tree.
/// As Scylla has a limit 'below' 1 MB each chunk has a maximum size of [MAX_BLOB_SIZE][MAX_BLOB_SIZE].
///
pub struct BlobTable;

impl BlobTable {
    /// Inserts the given data into the blob table.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    /// * `key_prefix` - The key prefix to use for the data
    /// * `data` - The data to insert
    ///
    pub async fn insert_raw(client: &Client, key_prefix: &str, data: &[u8]) -> Result<()> {
        let num_chunks = (data.len() as f64 / MAX_BLOB_SIZE as f64).ceil() as usize;

        let statement = format!(
            "INSERT INTO {}.{} ({}) VALUES (?, ?)",
            client.get_database(),
            TABLE_NAME,
            COLUMNS
        );
        let prepared_statement = client.get_session().prepare(statement).await?;

        for (i, chunk) in data.chunks(num_chunks).enumerate() {
            client
                .get_session()
                .execute(
                    &prepared_statement,
                    (format!("{}_{}", key_prefix, i), Vec::from(chunk)),
                )
                .await?;
        }

        Ok(())
    }

    /// Selects the raw data chunks from the blob table and returns it as a single vector.
    /// The data is sorted by the index of the chunk.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    /// * `key_prefix` - The key prefix to select
    ///
    pub async fn select_raw(client: &Client, key_prefix: &str) -> Result<Vec<u8>> {
        let statement = format!(
            "SELECT {} FROM {}.{} WHERE key LIKE '{}_%' ALLOW FILTERING",
            COLUMNS,
            client.get_database(),
            TABLE_NAME,
            key_prefix
        );
        let mut prepared_statement = client.get_session().prepare(statement).await?;
        prepared_statement.set_page_size(MAX_PAGES_PER_SELECT);

        // Get chunks
        let mut chunks: Vec<(usize, Vec<u8>)> = client
            .get_session()
            .execute_iter(prepared_statement, &[])
            .await?
            // Use `then` and try_collect to be able to handle errors using `?`
            .then(|row| async move {
                // Convert row to typed row and remote
                let typed_row: (String, Vec<u8>) = row?
                    .into_typed()
                    .context("could not convert taxonomy tree chunk")?;
                // Remove prefix + underline, and convert index to usize
                let index = typed_row.0[key_prefix.len() + 1..].parse::<usize>()?;
                // Return (index, data_chunk)
                Ok::<(usize, Vec<u8>), anyhow::Error>((index, typed_row.1))
            })
            .try_collect()
            .await?;
        // Sort by index
        chunks.sort_by(|a, b| a.0.cmp(&b.0));

        // Concat data chunks and return
        Ok(chunks.into_iter().map(|chunk| chunk.1).flatten().collect())
    }

    /// Deletes the records for the given key prefix.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    /// * `key_prefix` - The key prefix to delete
    ///
    pub async fn delete(client: &Client, key_prefix: &str) -> Result<()> {
        let statement = format!(
            "SELECT key FROM {}.{} WHERE key LIKE '{}_%' ALLOW FILTERING",
            client.get_database(),
            TABLE_NAME,
            key_prefix
        );
        let keys: Vec<String> = client
            .get_session()
            .query(statement, &[])
            .await?
            .rows
            .unwrap_or(vec![])
            .into_iter()
            .map(|row| row.into_typed::<(String,)>().unwrap().0)
            .collect();

        let statement = format!(
            "DELETE FROM {}.{} WHERE key IN ?",
            client.get_database(),
            TABLE_NAME,
        );
        let prepared_statement = client.get_session().prepare(statement).await?;
        // Delete in chunks of 10 to avoid overcome scylla partitioning limits
        for chunk in keys.chunks(10) {
            client
                .get_session()
                .execute(&prepared_statement, (chunk,))
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // 3rd party imports
    use lipsum::lipsum;
    use serial_test::serial;

    // internal imports
    use super::*;
    use crate::database::scylla::tests::DATABASE_URL;
    use crate::database::scylla::{client::GenericClient, prepare_database_for_tests};

    #[tokio::test]
    #[serial]
    async fn test_insert_select_delete() {
        let client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        // Test with at least 4 blob chunks
        let min_length = MAX_BLOB_SIZE * 4;

        let mut lorem_ipsum = String::with_capacity(min_length);

        loop {
            lorem_ipsum.push_str(lipsum(20).as_str());
            if lorem_ipsum.len() >= min_length {
                break;
            }
        }

        BlobTable::insert_raw(&client, "lorem_ipsum", lorem_ipsum.as_bytes())
            .await
            .unwrap();
        let selected_lorem_ipsum_bytes =
            BlobTable::select_raw(&client, "lorem_ipsum").await.unwrap();

        assert_eq!(
            lorem_ipsum.as_bytes(),
            selected_lorem_ipsum_bytes.as_slice()
        );

        assert_eq!(
            String::from_utf8(selected_lorem_ipsum_bytes)
                .unwrap()
                .to_string(),
            lorem_ipsum
        );

        BlobTable::delete(&client, "lorem_ipsum").await.unwrap();

        let statement = format!(
            "SELECT count(1) FROM {}.{} WHERE key LIKE 'lorem_ipsum_%' ALLOW FILTERING",
            client.get_database(),
            TABLE_NAME
        );
        let res = client.get_session().query(statement, &[]).await.unwrap();
        assert_eq!(res.rows_num().unwrap(), 1);

        assert_eq!(res.first_row_typed::<(i64,)>().unwrap(), (0,));
    }
}
