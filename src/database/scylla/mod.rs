/// Table for storing binary data in the database
pub mod blob_table;
/// Trait definition for generic client to use with database API and a client struct keeping the session.
pub mod client;
pub mod configuration_table;
pub mod database_build;
pub mod migrations;
/// Different filter architectures for filtering peptides from database
pub mod peptide_search;
pub mod peptide_table;
pub mod protein_table;
pub mod schema;
/// Database API for (de-)serializing the taxonomy tree
pub mod taxonomy_tree_table;

/// Custom errors for failed database interactions
pub mod errors;

// internal imports
use crate::database::scylla::client::Client;
use crate::database::scylla::schema::{CREATE_KEYSPACE, DROP_KEYSPACE, UP};

use super::generic_client::GenericClient;

pub async fn drop_keyspace(client: &Client) {
    let drop_statement = DROP_KEYSPACE.replace(":KEYSPACE:", client.get_database());
    client.query_unpaged(drop_statement, &[]).await.unwrap();
}

pub async fn create_keyspace_if_not_exists(client: &Client) {
    let create_statement = CREATE_KEYSPACE.replace(":KEYSPACE:", client.get_database());
    client.query_unpaged(create_statement, &[]).await.unwrap();
}

pub async fn prepare_database_for_tests(client: &Client) {
    // Empty the prepared statement cache
    client.reset_prepared_statement_cache().await;
    // Dropping a keyspace automatically drops all contained tables
    drop_keyspace(client).await;
    create_keyspace_if_not_exists(client).await;

    for statement in UP.iter() {
        let statement = statement.replace(":KEYSPACE:", client.get_database());
        client
            .query_unpaged(statement.to_owned(), &[])
            .await
            .unwrap();
    }
}

#[cfg(test)]
pub mod tests {
    pub const DATABASE_URL: &str = "scylla://127.0.0.1:9042,127.0.0.1:9043/macpepdb";
}
