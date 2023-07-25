/// Trait definition for generic client to use with database API and a client struct keeping the session.
pub mod client;
pub mod configuration_table;
pub mod database_build;
pub mod migrations;
pub mod peptide_table;
pub mod protein_table;
pub mod schema;

// 3rd party imports
use anyhow::Result;

// internal imports
use crate::database::scylla::client::{Client, GenericClient};
use crate::database::scylla::schema::{DROP_KEYSPACE, UP};

pub const SCYLLA_KEYSPACE_NAME: &str = "macpep";
pub const DATABASE_URL: &str = "127.0.0.1:9042";

pub async fn get_client() -> Result<Client> {
    Client::new(DATABASE_URL).await
}

pub async fn prepare_database_for_tests(client: &Client) {
    // Dropping a keyspace automatically drops all contained tables
    client
        .get_session()
        .query(DROP_KEYSPACE, &[])
        .await
        .unwrap();

    for statement in UP {
        client.get_session().query(statement, &[]).await.unwrap();
    }
}

#[cfg(test)]
pub mod tests {}
