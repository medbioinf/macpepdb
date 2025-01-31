// std imports
use std::{collections::HashMap, num::NonZeroUsize, ops::Deref, time::Duration};

// 3rd party imports
use anyhow::{anyhow, Result};
use fancy_regex::Regex;
use scylla::{
    transport::session::{PoolSize, Session},
    SessionBuilder,
};
use tokio::sync::RwLock;

// local imports
use crate::database::generic_client::GenericClient;

lazy_static! {
    pub static ref URL_PASER_REGEX: Regex = Regex::new(r"(?m)scylla://((?P<credentials>[^:]*?:[^:]+)@){0,1}(?P<hosts>.+)/(?P<keyspace>[^/?]+)(\?(?P<attributes>.+)){0,1}").unwrap();
}

/// Pool type for the ScyllaDB client
/// default is PerHost
///
#[derive(PartialEq, Eq, Debug)]
enum PoolType {
    PerHost,
    PerShard,
}

impl From<&str> for PoolType {
    fn from(s: &str) -> Self {
        match s {
            "host" => PoolType::PerHost,
            "shard" => PoolType::PerShard,
            _ => PoolType::PerHost,
        }
    }
}

struct ClientSettings {
    pub hosts: Vec<String>,
    pub keyspace: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub connection_timeout: Option<Duration>,
    pub pool_size: Option<usize>,
    pub pool_type: PoolType,
}

impl ClientSettings {
    pub fn new(database_url: &str) -> Result<Self> {
        // Parse url
        let matches = match URL_PASER_REGEX.captures(database_url)? {
            Some(matches) => matches,
            None => return Err(anyhow!("Invalid database URL")),
        };

        // Extract hosts and keyspace as they are mandatory
        let hosts: Vec<String> = match matches.name("hosts") {
            Some(hosts) => hosts
                .as_str()
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            None => return Err(anyhow!("Invalid database URL, hosts not found")),
        };

        let keyspace = match matches.name("keyspace") {
            Some(keyspace) => keyspace.as_str().to_string(),
            None => return Err(anyhow!("Invalid database URL, keyspace not found")),
        };

        // Create settings
        let mut settings = Self {
            hosts,
            keyspace,
            user: None,
            password: None,
            connection_timeout: None,
            pool_size: None,
            pool_type: PoolType::PerHost,
        };

        // Extract credentials and attributes if present
        match matches.name("credentials") {
            Some(credentials) => {
                let credentials = credentials.as_str().split(':').collect::<Vec<&str>>();
                settings.user = Some(credentials[0].to_string());
                settings.password = Some(credentials[1].to_string());
            }
            None => (),
        };

        // Extract attributes if present
        let attributes = match matches.name("attributes") {
            Some(attributes) => attributes.as_str().to_owned(),
            None => "".to_owned(),
        };

        // Parse attributes
        if !attributes.is_empty() {
            let attributes: HashMap<&str, &str> = attributes
                .split('&')
                .map(|attribute| {
                    let attribute: Vec<&str> = attribute.split('=').collect();
                    (attribute[0], attribute[1])
                })
                .collect();

            if let Some(timeout) = attributes.get("connection_timeout") {
                settings.connection_timeout = Some(Duration::from_secs(timeout.parse()?));
            }

            if let Some(pool_size) = attributes.get("pool_size") {
                settings.pool_size = Some(pool_size.parse()?);
            }

            if let Some(pool_type) = attributes.get("pool_type") {
                settings.pool_type = PoolType::from(*pool_type);
            }
        }
        Ok(settings)
    }

    pub async fn to_session(&self) -> Result<Session> {
        let mut builder = SessionBuilder::new().known_nodes(self.hosts.clone());

        if self.user.is_some() && self.password.is_some() {
            builder = builder.user(self.user.as_ref().unwrap(), self.password.as_ref().unwrap());
        }

        if let Some(timeout) = self.connection_timeout {
            builder = builder.connection_timeout(timeout);
        }

        if let Some(pool_size) = self.pool_size {
            let pool_size = match NonZeroUsize::new(pool_size) {
                Some(pool_size) => pool_size,
                None => return Err(anyhow!("Invalid pool size must be larger than 0")),
            };

            let pool_size = match self.pool_type {
                PoolType::PerHost => PoolSize::PerHost(pool_size),
                PoolType::PerShard => PoolSize::PerShard(pool_size),
            };
            builder = builder.pool_size(pool_size);
        }

        Ok(builder.build().await?)
    }
}

pub struct Client {
    session: Session,
    database: String,
    url: String,
    prepared_statement_cache:
        RwLock<HashMap<String, scylla::prepared_statement::PreparedStatement>>,
}

impl Client {
    pub fn get_url(&self) -> &str {
        &self.url
    }

    /// Get a prepared statement from the cache
    /// if the statement is not in the cache it be added and returned
    pub async fn get_prepared_statement(
        &self,
        query: &str,
    ) -> Result<scylla::prepared_statement::PreparedStatement> {
        // Closure to make sure read guard is dropped
        {
            let read_guard = self.prepared_statement_cache.read().await;
            if let Some(statement) = read_guard.get(query) {
                return Ok(statement.clone());
            }
        }

        let statement = self
            .prepare(query.replace(":KEYSPACE:", &self.database))
            .await?;

        // Closure to make sure write guard is dropped
        {
            let mut write_guard = self.prepared_statement_cache.write().await;
            write_guard.insert(query.to_string(), statement.clone());
        }

        Ok(statement)
    }

    pub async fn reset_prepared_statement_cache(&self) {
        self.prepared_statement_cache.write().await.clear();
    }
}

impl Deref for Client {
    type Target = Session;

    fn deref(&self) -> &Self::Target {
        &self.session
    }
}

impl GenericClient<Session> for Client {
    /// Creates a new ScyllaDB client
    ///
    /// # Arguments
    /// * `database_url` - A string slice that holds the database URL
    ///
    async fn new(database_url: &str) -> Result<Self>
    where
        Self: Sized,
    {
        let settings = ClientSettings::new(database_url)?;
        Ok(Self {
            session: settings.to_session().await?,
            database: settings.keyspace.clone(),
            url: database_url.to_string(),
            prepared_statement_cache: RwLock::new(HashMap::new()),
        })
    }

    fn get_inner_client(&self) -> &Session {
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
        let url_credentials_attributes = "scylla://gandalf:mellon@10.0.0.168,10.0.0.35,10.0.0.139,10.0.0.11,10.0.0.73,10.0.0.194/mdb_uniprot?connection_timeout=60&pool_size=1&pool_type=shard";
        let url_attributes = "scylla://10.0.0.168,10.0.0.35,10.0.0.139,10.0.0.11,10.0.0.73,10.0.0.194/mdb_uniprot?connection_timeout=60&pool_size=1";
        let url_mandatory =
            "scylla://10.0.0.168,10.0.0.35,10.0.0.139,10.0.0.11,10.0.0.73,10.0.0.194/mdb_uniprot";

        let settings = ClientSettings::new(url_credentials_attributes).unwrap();
        assert_eq!(settings.user, Some("gandalf".to_string()));
        assert_eq!(settings.password, Some("mellon".to_string()));
        assert_eq!(
            settings.hosts,
            vec![
                "10.0.0.168".to_string(),
                "10.0.0.35".to_string(),
                "10.0.0.139".to_string(),
                "10.0.0.11".to_string(),
                "10.0.0.73".to_string(),
                "10.0.0.194".to_string()
            ]
        );
        assert_eq!(settings.connection_timeout, Some(Duration::from_secs(60)));
        assert_eq!(settings.pool_size, Some(1));
        assert_eq!(settings.pool_type, PoolType::PerShard);

        let settings = ClientSettings::new(url_attributes).unwrap();
        assert_eq!(settings.user, None);
        assert_eq!(settings.password, None);
        assert_eq!(
            settings.hosts,
            vec![
                "10.0.0.168".to_string(),
                "10.0.0.35".to_string(),
                "10.0.0.139".to_string(),
                "10.0.0.11".to_string(),
                "10.0.0.73".to_string(),
                "10.0.0.194".to_string()
            ]
        );
        assert_eq!(settings.connection_timeout, Some(Duration::from_secs(60)));
        assert_eq!(settings.pool_size, Some(1));
        assert_eq!(settings.pool_type, PoolType::PerHost);

        let settings = ClientSettings::new(url_mandatory).unwrap();
        assert_eq!(settings.user, None);
        assert_eq!(settings.password, None);
        assert_eq!(
            settings.hosts,
            vec![
                "10.0.0.168".to_string(),
                "10.0.0.35".to_string(),
                "10.0.0.139".to_string(),
                "10.0.0.11".to_string(),
                "10.0.0.73".to_string(),
                "10.0.0.194".to_string()
            ]
        );
        assert_eq!(settings.connection_timeout, None);
        assert_eq!(settings.pool_size, None);
        assert_eq!(settings.pool_type, PoolType::PerHost);
    }
}
