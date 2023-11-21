// 3rd party imports
use anyhow::{Context, Result};
use async_stream::try_stream;
use dihardts_omicstools::biology::taxonomy::Taxonomy;
use futures::future::join_all;
use futures::Stream;

// internal imports
use crate::database::scylla::client::{Client, GenericClient};

use crate::database::table::Table;

/// Table name of the taxonomy table
///
const TABLE_NAME: &'static str = "taxonomies";

/// Columns of the taxonomy table
///
const COLUMNS: &'static str = "id, scientific_name";

lazy_static! {
    /// Placeholders for the insert
    ///
    pub static ref INSERT_PLACEHOLDERS: String = COLUMNS.split(", ").map(|_| "?".to_string()).collect::<Vec<String>>().join(", ");
}

/// Database API for taxonomies table. I contrast what the name suggested, this table only exists
/// to be able to do a fuzzy search for taxonomy names
///
pub struct TaxonomyTable;

impl TaxonomyTable {
    /// Inserts the id and scientific name into the database
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `taxonomies` - Taxonomies
    ///
    pub async fn bulk_insert(client: &Client, taxonomies: &Vec<&Taxonomy>) -> Result<()> {
        let statement = format!(
            "INSERT INTO {}.{} ({}) VALUES ({});",
            client.get_database(),
            Self::table_name(),
            COLUMNS,
            INSERT_PLACEHOLDERS.as_str()
        );
        let prep_statement = client.get_session().prepare(statement).await?;

        for taxonomies_chunk in taxonomies.chunks(1000) {
            let insertion_futures = taxonomies_chunk.into_iter().map(|tax| {
                client.get_session().execute(
                    &prep_statement,
                    (tax.get_id() as i64, tax.get_scientific_name()),
                )
            });
            join_all(insertion_futures).await;
        }
        Ok(())
    }

    pub async fn delete_all(client: &Client) -> Result<()> {
        let statement = format!("TRUNCATE {}.{};", client.get_database(), Self::table_name());
        client
            .get_session()
            .query(statement, &[])
            .await
            .context("Error when emptying taxonomy table")?;
        Ok(())
    }

    /// Search for taxonomies by their name
    ///
    /// # Arguments
    /// * `client` - The client to use to connect to the database
    /// * `name_query` - The name to search for. The wildcard character is `*` everything else will be escaped
    ///
    pub async fn search_taxonomy_by_name<'a>(
        client: &'a Client,
        name_query: &str,
    ) -> Result<impl Stream<Item = Result<u64>> + 'a> {
        let mut escaped_name_query = name_query.replace("%", "\\%");
        escaped_name_query = escaped_name_query.replace("*", "%");
        let statement = format!(
            "SELECT id FROM {}.{} WHERE scientific_name LIKE ? ALLOW FILTERING",
            client.get_database(),
            TABLE_NAME,
        );

        Ok(try_stream! {
            let mut prep_statement = client.get_session().prepare(statement).await?;
            prep_statement.set_page_size(1000);
            let rows_stream = client
                .get_session()
                .execute_iter(prep_statement, (escaped_name_query,))
                .await
                .context("Failed to execute query")?;
            for await row in rows_stream {
                yield row?.into_typed::<(i64,)>()?.0 as u64;
            }
        })
    }
}

impl Table for TaxonomyTable {
    fn table_name() -> &'static str {
        TABLE_NAME
    }
}

#[cfg(test)]
mod tests {
    // 3rd party imports
    use dihardts_omicstools::biology::io::taxonomy_reader::TaxonomyReader;
    use serial_test::serial;

    // internal imports
    use super::*;
    use crate::{
        database::scylla::{prepare_database_for_tests, tests::DATABASE_URL},
        tools::tests::get_taxdmp_zip,
    };

    #[tokio::test]
    #[serial]
    async fn test_insert_search() {
        let taxdmp_zip_path = get_taxdmp_zip().await.unwrap();
        let client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        let taxonomy_tree = TaxonomyReader::new(&taxdmp_zip_path)
            .unwrap()
            .read()
            .unwrap();

        let res = TaxonomyTable::bulk_insert(&client, &taxonomy_tree.get_taxonomies()).await;
        assert!(res.is_ok());
    }
}
