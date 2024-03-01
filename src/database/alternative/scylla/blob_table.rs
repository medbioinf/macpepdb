// 3rd party imports
use anyhow::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use scylla::Session;

// internal imports
use super::client::Client;
use crate::database::alternative::{
    blob_table::BlobTable as BlobTableTrait, client::Client as ClientTrait, table::Table,
};

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
pub struct BlobTable<'a> {
    client: &'a Client,
}

impl<'a> Table for BlobTable<'a> {
    /// Returns the name of the table.
    ///
    fn table_name() -> &'static str {
        TABLE_NAME
    }
}

impl<'a> BlobTableTrait<'a, Client, Session> for BlobTable<'a> {
    fn new(client: &'a Client) -> Self {
        Self { client }
    }

    async fn insert_raw(&self, key_prefix: &str, data: &[u8]) -> Result<()> {
        let num_chunks = (data.len() as f64 / MAX_BLOB_SIZE as f64).ceil() as usize;

        let statement = format!(
            "INSERT INTO {}.{} ({}) VALUES (?, ?)",
            self.client.get_database(),
            TABLE_NAME,
            COLUMNS
        );
        let prepared_statement = self.client.prepare(statement).await?;

        for (i, chunk) in data.chunks(num_chunks).enumerate() {
            self.client
                .execute(
                    &prepared_statement,
                    (format!("{}_{}", key_prefix, i), Vec::from(chunk)),
                )
                .await?;
        }

        Ok(())
    }

    async fn select_raw(&self, key_prefix: &str) -> Result<Vec<u8>> {
        let statement = format!(
            "SELECT {} FROM {}.{} WHERE key LIKE '{}_%' ALLOW FILTERING",
            COLUMNS,
            self.client.get_database(),
            TABLE_NAME,
            key_prefix
        );
        let mut prepared_statement = self.client.prepare(statement).await?;
        prepared_statement.set_page_size(MAX_PAGES_PER_SELECT);

        // Get chunks
        let mut chunks: Vec<(usize, Vec<u8>)> = self
            .client
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

    async fn delete(&self, key_prefix: &str) -> Result<()> {
        let statement = format!(
            "SELECT key FROM {}.{} WHERE key LIKE '{}_%' ALLOW FILTERING",
            self.client.get_database(),
            TABLE_NAME,
            key_prefix
        );

        // Using execute_iter plays nicer with smaller database clusters and avoids consistency issues
        // although makes it complexer as results are streamed
        let mut prep_statement = self.client.prepare(statement).await?;
        prep_statement.set_page_size(1000);
        let keys: Vec<String> = self
            .client
            .execute_iter(prep_statement, &[])
            .await
            .context("Error when selecting keys for deletion")?
            .then(|row| async move {
                Ok::<std::string::String, anyhow::Error>(row?.into_typed::<(String,)>()?.0)
            })
            .try_collect()
            .await
            .context("Error when collecting keys for deletion")?;

        let statement = format!(
            "DELETE FROM {}.{} WHERE key IN ?",
            self.client.get_database(),
            TABLE_NAME,
        );
        let prepared_statement = self.client.prepare(statement).await?;
        // Delete in chunks of 10 to avoid overcome scylla partitioning limits
        for chunk in keys.chunks(10) {
            self.client.execute(&prepared_statement, (chunk,)).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // 3rd party imports
    use lipsum::lipsum;
    use serial_test::serial;
    use tracing_test::traced_test;

    // internal imports
    use super::*;
    use crate::database::generic_client::GenericClient;
    use crate::database::scylla::client::Client as OldClient;
    use crate::database::scylla::prepare_database_for_tests;
    use crate::database::scylla::tests::DATABASE_URL;

    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    #[serial]
    async fn test_insert_select_delete() {
        let client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&OldClient::new(DATABASE_URL).await.unwrap()).await;

        // Test with at least 4 blob chunks
        let min_length = MAX_BLOB_SIZE * 4;

        let mut lorem_ipsum = String::with_capacity(min_length);

        loop {
            lorem_ipsum.push_str(lipsum(20).as_str());
            if lorem_ipsum.len() >= min_length {
                break;
            }
        }

        client
            .blob_table()
            .insert_raw("lorem_ipsum", lorem_ipsum.as_bytes())
            .await
            .unwrap();
        let selected_lorem_ipsum_bytes =
            client.blob_table().select_raw("lorem_ipsum").await.unwrap();

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

        client.blob_table().delete("lorem_ipsum").await.unwrap();

        let statement = format!(
            "SELECT count(1) FROM {}.{} WHERE key LIKE 'lorem_ipsum_%' ALLOW FILTERING",
            client.get_database(),
            TABLE_NAME
        );
        let res = client.query(statement, &[]).await.unwrap();
        assert_eq!(res.rows_num().unwrap(), 1);

        assert_eq!(res.first_row_typed::<(i64,)>().unwrap(), (0,));
    }
}
