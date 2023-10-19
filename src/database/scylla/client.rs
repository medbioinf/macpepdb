// 3rd party imports
use anyhow::Result;
use scylla::{transport::session::Session, SessionBuilder};

/// Wrapper trait for a ScyllaDB session to make it more convenient to use with generic implementation
///
pub trait GenericClient: Send + Sync {
    async fn new(hostnames: &Vec<String>, database: &str) -> Result<Self>
    where
        Self: Sized;
    fn get_session(&self) -> &Session;
    fn get_database(&self) -> &str;
}

pub struct Client {
    session: Session,
    database: String,
}

impl GenericClient for Client {
    /// Creates a new ScyllaDB client
    ///
    /// # Arguments
    /// * `hostnames` - List of hostnames to connect to
    /// * `database` - Name of the keyspace to use
    ///
    async fn new(hostnames: &Vec<String>, database: &str) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            session: SessionBuilder::new()
                .known_nodes(hostnames)
                .build()
                .await
                .unwrap(),
            database: database.to_owned(),
        })
    }

    fn get_session(&self) -> &Session {
        &self.session
    }

    fn get_database(&self) -> &str {
        &self.database
    }
}
