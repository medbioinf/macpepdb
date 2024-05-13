// std imports
use std::collections::HashMap;

// 3rd party imports
use anyhow::{Context, Result};
use dihardts_omicstools::biology::taxonomy::TaxonomyTree;
use serde_json::to_string as serde_to_string;

// internal imports
use crate::database::scylla::blob_table::BlobTable;
use crate::database::scylla::client::Client;

/// Prefix for the key of the taxonomy tree in the blob table
///
pub const KEY_PREFIX: &'static str = "taxonomy_tree_chunk_";

/// Stores the taxonomy tree in the blob table as binary JSON.
///
pub struct TaxonomyTreeTable;

impl TaxonomyTreeTable {
    /// Inserts the taxonomy tree into the blob table. Deletes the old entries first.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    /// * `taxonomy_tree` - The taxonomy tree to insert
    ///
    pub async fn insert(client: &Client, taxonomy_tree: &TaxonomyTree) -> Result<()> {
        let json_str = serde_to_string(taxonomy_tree)?;
        BlobTable::delete(client, KEY_PREFIX)
            .await
            .context("Error when deleting the old taxonomy tree chunks")?;
        BlobTable::insert_raw(client, KEY_PREFIX, json_str.as_bytes())
            .await
            .context("Error when inserting the new taxonomy tree chunks")?;
        Ok(())
    }

    /// Selects the taxonomy tree from the blob table.
    ///
    /// # Arguments
    /// * `client` - The client to use for the database connection
    ///
    pub async fn select(client: &Client) -> Result<TaxonomyTree> {
        let bytes = BlobTable::select_raw(client, KEY_PREFIX).await?;
        if bytes.is_empty() {
            tracing::debug!("No taxonomy tree found");
            return Ok(TaxonomyTree::new(
                HashMap::new(),
                Vec::new(),
                HashMap::new(),
                Vec::new(),
            ));
        }
        Ok(serde_json::from_slice(bytes.as_slice())?)
    }
}

#[cfg(test)]
mod tests {
    // 3rd party importss
    use dihardts_omicstools::biology::io::taxonomy_reader::TaxonomyReader;
    use serial_test::serial;

    // internal imports
    use super::*;
    use crate::database::generic_client::GenericClient;
    use crate::database::scylla::prepare_database_for_tests;
    use crate::database::scylla::{client::Client, tests::DATABASE_URL};
    use crate::tools::tests::get_taxdmp_zip;

    #[tokio::test]
    #[serial]
    async fn test_serialization() {
        let taxdmp_zip_path = get_taxdmp_zip().await.unwrap();
        let client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        let taxonomy_tree = TaxonomyReader::new(&taxdmp_zip_path)
            .unwrap()
            .read()
            .unwrap();

        TaxonomyTreeTable::insert(&client, &taxonomy_tree)
            .await
            .unwrap();
        // This just needs to work as BlobTable already tests the insertion and selection of byte arrays, and their
        // correct restructure from chunks, while the (de-)serializazion of the TaxonomyTree is already tested in
        // dihardts_omicstools
        assert!(TaxonomyTreeTable::select(&client).await.is_ok());
    }
}
