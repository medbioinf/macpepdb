/// Trait definition for generic client to use with database API and a client struct keeping the session.
pub mod client;
pub mod configuration_table;
pub mod database_build;
pub mod migrations;
pub mod peptide_table;
pub mod protein_table;
pub mod schema;

// internal imports
use crate::database::scylla::client::{Client, GenericClient};
use crate::database::scylla::schema::{CREATE_KEYSPACE, DROP_KEYSPACE, UP};

pub async fn drop_keyspace(client: &Client) {
    let drop_statement = DROP_KEYSPACE.replace(":KEYSPACE:", client.get_database());
    client
        .get_session()
        .query(drop_statement, &[])
        .await
        .unwrap();
}

pub async fn create_keyspace_if_not_exists(client: &Client) {
    let create_statement = CREATE_KEYSPACE.replace(":KEYSPACE:", client.get_database());
    client
        .get_session()
        .query(create_statement, &[])
        .await
        .unwrap();
}

pub async fn prepare_database_for_tests(client: &Client) {
    // Dropping a keyspace automatically drops all contained tables
    drop_keyspace(&client).await;
    create_keyspace_if_not_exists(&client).await;

    for statement in UP {
        let statement = statement.replace(":KEYSPACE:", client.get_database());
        client.get_session().query(statement, &[]).await.unwrap();
    }
}

#[cfg(test)]
pub mod tests {
    pub const DATABASE_URL: &'static str = "scylla://127.0.0.1:9042/macpepdb";
}
