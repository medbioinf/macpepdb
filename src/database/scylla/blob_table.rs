// 3rd party imports
use anyhow::Result;
use futures::TryStreamExt;
use tokio::pin;

// internal imports
use crate::database::scylla::client::Client;

/// Max size of a blob in bytes (512 kB)
///
pub const MAX_BLOB_SIZE: usize = 512000;

/// Table name for the blob table
///
pub const TABLE_NAME: &str = "blobs";

/// Columns of the blob table
///
pub const COLUMNS: &str = "key, position, data";

/// Max pages per select (avoids timeouts)
///
pub const MAX_PAGES_PER_SELECT: i32 = 1000;

lazy_static! {
    /// Insert statement for the blob table
    /// Takes 3 parameters: key, position, data
    ///
    static ref INSERT_STATEMENT: String = format!(
        "INSERT INTO :KEYSPACE:.{} ({}) VALUES (?, ?, ?)",
        TABLE_NAME, COLUMNS
    );

    /// Select statement for the blob table
    /// Takes 1 parameter: key
    ///
    static ref SELECT_STATEMENT: String = format!(
        "SELECT {} FROM :KEYSPACE:.{} WHERE key = ?",
        COLUMNS, TABLE_NAME
    );

    /// Delete statement for removing all records with a given key prefix
    /// Takes 1 parameter: key
    ///
    static ref DELETE_KEY_PREFIX_STATEMENT: String = format!(
        "DELETE FROM :KEYSPACE:.{} WHERE key = ?",
        TABLE_NAME
    );
}

type TypedBlobRow = (String, i64, Vec<u8>);

/// Trait for using the blob table.
/// The blob table is used to store binary data in the database like the JSON-serialized taxonomy tree.
/// As Scylla has a limit 'below' 1 MB each chunk has a maximum size of [MAX_BLOB_SIZE].
///
pub struct BlobTable;

impl BlobTable {
    /// Inserts the given data into the blob table.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    /// * `key` - The key prefix to use for the data
    /// * `data` - The data to insert
    ///
    pub async fn insert(client: &Client, key: &str, data: &[u8]) -> Result<()> {
        let prepared_statement = client.get_prepared_statement(&INSERT_STATEMENT).await?;

        for (i, chunk) in data.chunks(MAX_BLOB_SIZE).enumerate() {
            client
                .execute_unpaged(&prepared_statement, (key, i as i64, Vec::from(chunk)))
                .await?;
        }

        Ok(())
    }

    /// Selects the raw data chunks from the blob table and returns it as a single vector.
    /// The data is sorted by the index of the chunk.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    /// * `key` - The key prefix to select
    ///
    pub async fn select(client: &Client, key: &str) -> Result<Vec<u8>> {
        let prepared_statement = client.get_prepared_statement(&SELECT_STATEMENT).await?;

        // Get chunks
        let stream = client
            .execute_iter(prepared_statement, (key,))
            .await?
            .rows_stream::<TypedBlobRow>()?;

        pin!(stream);

        let mut chunks = stream.try_collect::<Vec<_>>().await?;

        // Sort by index
        chunks.sort_by(|a, b| a.1.cmp(&b.1));

        // Concat data chunks and return
        Ok(chunks.into_iter().flat_map(|chunk| chunk.2).collect())
    }

    /// Deletes the records for the given key prefix.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    /// * `key` - The key prefix to delete
    ///
    pub async fn delete(client: &Client, key: &str) -> Result<()> {
        let prepared_statement = client
            .get_prepared_statement(&DELETE_KEY_PREFIX_STATEMENT)
            .await?;

        client.execute_unpaged(&prepared_statement, (key,)).await?;
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
    use crate::database::generic_client::GenericClient;
    use crate::database::scylla::prepare_database_for_tests;
    use crate::database::scylla::tests::get_test_database_url;

    #[tokio::test]
    #[serial]
    async fn test_insert_select_delete() {
        let client = Client::new(&get_test_database_url()).await.unwrap();
        prepare_database_for_tests(&client).await;

        // Test with at least 4 blob chunks
        let min_length = MAX_BLOB_SIZE * 4;

        let mut lorem_ipsum = String::with_capacity(min_length);

        while lorem_ipsum.len() < min_length {
            lorem_ipsum.push_str(lipsum(20).as_str());
        }

        let expected_num_blobs = (lorem_ipsum.len() as f64 / MAX_BLOB_SIZE as f64).ceil() as usize;

        BlobTable::insert(&client, "lorem_ipsum", lorem_ipsum.as_bytes())
            .await
            .unwrap();

        // Check if the number of blobs is correct
        let prepared_count_statement = client
            .prepare(format!(
                "SELECT count(*) FROM {}.{} WHERE key = ?",
                client.get_database(),
                TABLE_NAME
            ))
            .await
            .unwrap();

        let res = client
            .execute_unpaged(&prepared_count_statement, ("lorem_ipsum",))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();

        assert_eq!(res.rows_num(), 1);

        assert_eq!(
            res.first_row::<(i64,)>().unwrap(),
            (expected_num_blobs as i64,)
        );

        // select and check if the data is correct
        let selected_lorem_ipsum_bytes = BlobTable::select(&client, "lorem_ipsum").await.unwrap();

        assert_eq!(selected_lorem_ipsum_bytes.len(), lorem_ipsum.len(),);

        assert_eq!(
            String::from_utf8(selected_lorem_ipsum_bytes)
                .unwrap()
                .to_string(),
            lorem_ipsum
        );

        // delete and check if the number of blobs is 0
        BlobTable::delete(&client, "lorem_ipsum").await.unwrap();

        let res = client
            .execute_unpaged(&prepared_count_statement, ("lorem_ipsum",))
            .await
            .unwrap()
            .into_rows_result()
            .unwrap();

        assert_eq!(res.rows_num(), 1);

        assert_eq!(res.first_row::<(i64,)>().unwrap(), (0,));
    }

    #[tokio::test]
    #[serial]
    async fn test_tokenawareness() {
        let client = Client::new(&get_test_database_url()).await.unwrap();
        prepare_database_for_tests(&client).await;
        for statement in &[
            INSERT_STATEMENT.as_str(),
            SELECT_STATEMENT.as_str(),
            DELETE_KEY_PREFIX_STATEMENT.as_str(),
        ] {
            let prepared_statement = client.get_prepared_statement(statement).await.unwrap();
            assert!(prepared_statement.is_token_aware());
        }
    }
}
