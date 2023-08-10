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

use self::schema::CREATE_KEYSPACE;

pub const SCYLLA_KEYSPACE_NAME: &str = "macpep";
pub const DATABASE_URL: &str = "127.0.0.1:9042";

pub async fn get_client(database_url: Option<&str>) -> Result<Client> {
    Client::new(database_url.unwrap_or(DATABASE_URL)).await
}

pub async fn drop_keyspace(client: &Client) {
    client
        .get_session()
        .query(DROP_KEYSPACE, &[])
        .await
        .unwrap();
}

pub async fn create_keyspace_if_not_exists(client: &Client) {
    client
        .get_session()
        .query(CREATE_KEYSPACE, &[])
        .await
        .unwrap();
}

pub async fn prepare_database_for_tests(client: &Client) {
    // Dropping a keyspace automatically drops all contained tables
    drop_keyspace(&client).await;
    create_keyspace_if_not_exists(&client).await;

    for statement in UP {
        client.get_session().query(statement, &[]).await.unwrap();
    }
}

#[cfg(test)]
pub mod tests {}
