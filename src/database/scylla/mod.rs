/// Trait definition for generic client to use with database API and a client struct keeping the session.
pub mod client;
pub mod configuration_table;
pub mod migrations;
pub mod protein_table;

pub const SCYLLA_KEYSPACE_NAME: &str = "macpep";

#[cfg(test)]
mod tests {
    use crate::database::scylla::migrations::{DROP_KEYSPACE, UP};
    use scylla::{transport::session::Session, SessionBuilder};

    pub const DATABASE_URL: &str = "127.0.0.1:9042";
    // let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| DATABASE_URL.to_string());
    pub async fn get_session() -> Session {
        return SessionBuilder::new()
            .known_node(DATABASE_URL)
            .build()
            .await
            .unwrap();
    }

    pub async fn prepare_database_for_tests(session: &Session) {
        // Dropping a keyspace automatically drops all contained tables
        session.query(DROP_KEYSPACE, &[]).await.unwrap();

        for statement in UP {
            session.query(statement, &[]).await.unwrap();
        }
    }
}
