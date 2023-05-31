/// Trait definition for generic client to use with database API and a client struct keeping the session.
pub mod client;
pub mod configuration_table;
pub mod migrations;

#[cfg(test)]
mod tests {

    // 3rd party imports
    use anyhow::Result;

    // internal imports
    use crate::database::scylla::client::{Client, GenericClient};
    use crate::database::scylla::migrations::{DROP_KEYSPACE, UP};

    pub const DATABASE_URL: &str = "127.0.0.1:9042";
    // let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| DATABASE_URL.to_string());
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
}
