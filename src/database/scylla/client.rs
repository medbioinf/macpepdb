// 3rd party imports
use anyhow::{anyhow, Result};
use scylla::{transport::session::Session, SessionBuilder};

/// Wrapper trait for a ScyllaDB session to make it more convenient to use with generic implementation
///
pub trait GenericClient: Send + Sync {
    fn new(database_url: &str) -> impl std::future::Future<Output = Result<Self>> + Send
    where
        Self: Sized;
    fn get_session(&self) -> &Session;
    fn get_database(&self) -> &str;
}

pub struct Client {
    session: Session,
    database: String,
}

impl Client {
    /// Parses the database URL and returns a list of hostnames and the keyspace
    ///
    /// # Arguments
    /// * `database_url` - Database URL in the format `scylla://<hostname1>,<hostname2>,<hostname3>/keyspace`
    ///
    pub fn parse_database_url(database_url: &str) -> Result<(Vec<String>, String)> {
        // Remove protocol `scylla://`
        let plain_database_url = database_url[9..].to_string();
        // Split into hostnames and database
        let (hostnames, keyspace): (String, String) = match plain_database_url.split_once("/") {
            Some((hostnames, keyspace)) => (hostnames.to_string(), keyspace.to_string()),
            None => {
                return Err(anyhow!(
                    "Database URL {} is not valid. It should be in the form `scylla://<hostname1>,<hostname2>,<hostname3>/keyspace`",
                    database_url
                ))
            }
        };
        Ok((
            hostnames
                .split(",")
                .map(|node| node.trim().to_string())
                .collect(),
            keyspace.trim().to_string(),
        ))
    }
}

impl GenericClient for Client {
    /// Creates a new ScyllaDB client
    ///
    /// # Arguments
    /// * `hostnames` - List of hostnames to connect to
    /// * `database` - Name of the keyspace to use
    ///
    async fn new(database_url: &str) -> Result<Self>
    where
        Self: Sized,
    {
        let (hostnames, keyspace) = Self::parse_database_url(database_url)?;
        Ok(Self {
            session: SessionBuilder::new()
                .known_nodes(hostnames)
                .build()
                .await
                .unwrap(),
            database: keyspace,
        })
    }

    fn get_session(&self) -> &Session {
        &self.session
    }

    fn get_database(&self) -> &str {
        &self.database
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_database_url() {
        let database_url = "scylla://localhost01,localhost02,localhost03.org/some_keyspace";
        let (hostnames, keyspace) = Client::parse_database_url(database_url).unwrap();
        assert_eq!(
            hostnames,
            vec![
                "localhost01".to_string(),
                "localhost02".to_string(),
                "localhost03.org".to_string()
            ]
        );
        assert_eq!(keyspace, "some_keyspace".to_string());
    }
}
