// std imports
use std::{collections::HashMap, num::NonZeroUsize, ops::Deref, time::Duration};

// 3rd party imports
use anyhow::{anyhow, Result};
use fancy_regex::Regex;
use scylla::{
    client::{session::Session, session_builder::SessionBuilder, PoolSize},
    statement::{prepared::PreparedStatement, Consistency},
};
use tokio::sync::RwLock;

// local imports
use crate::database::generic_client::GenericClient;

lazy_static! {
    pub static ref URL_PASER_REGEX: Regex = Regex::new(r"(?m)scylla://((?P<credentials>[^:]*?:[^:]+)@){0,1}(?P<hosts>.+)/(?P<keyspace>[^/?]+)(\?(?P<attributes>.+)){0,1}").unwrap();
}

/// Default replication factor for the ScyllaDB keyspace
/// 1 is not recommended for production use
///
const DEFAULT_REPLICATION_FACTOR: usize = 1;

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
    pub replication_factor: usize,
    pub user: Option<String>,
    pub password: Option<String>,
    pub connection_timeout: Option<Duration>,
    pub pool_size: Option<usize>,
    pub pool_type: PoolType,
    pub read_consistency_level: Option<Consistency>,
    pub write_consistency_level: Option<Consistency>,
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
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            user: None,
            password: None,
            connection_timeout: None,
            pool_size: None,
            pool_type: PoolType::PerHost,
            read_consistency_level: None,
            write_consistency_level: None,
        };

        // Extract credentials and attributes if present
        if let Some(credentials) = matches.name("credentials") {
            let credentials = credentials.as_str().split(':').collect::<Vec<&str>>();
            settings.user = Some(credentials[0].to_string());
            settings.password = Some(credentials[1].to_string());
        }

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

            if let Some(level) = attributes.get("read_consistency_level") {
                settings.read_consistency_level = Some(Self::str_to_consistency_level(level)?);
            };

            if let Some(level) = attributes.get("write_consistency_level") {
                settings.write_consistency_level =
                    Some(Self::str_to_write_consistency_level(level)?);
            };

            if let Some(replication_factor) = attributes.get("replication_factor") {
                settings.replication_factor = replication_factor.parse()?;
            };
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

    /// Returns the ScyllaDB consistency level from a string representation
    /// which can be used for read and write operations.
    ///
    fn str_to_consistency_level(s: &str) -> Result<Consistency> {
        match s {
            "one" => Ok(Consistency::One),
            "two" => Ok(Consistency::Two),
            "three" => Ok(Consistency::Three),
            "quorum" => Ok(Consistency::Quorum),
            "local_quorum" => Ok(Consistency::LocalQuorum),
            "all" => Ok(Consistency::All),
            "local_one" => Ok(Consistency::LocalOne),
            "local_serial" => Ok(Consistency::LocalSerial),
            "serial" => Ok(Consistency::Serial),
            _ => Err(anyhow!("Invalid consistency level: {}", s)),
        }
    }

    /// Returns the ScyllaDB consistency level from a string representation
    /// which can be used for write operations.
    ///
    fn str_to_write_consistency_level(s: &str) -> Result<Consistency> {
        if let Ok(level) = Self::str_to_consistency_level(s) {
            return Ok(level);
        }

        match s {
            "any" => Ok(Consistency::Any),
            "each_quorum" => Ok(Consistency::EachQuorum),
            _ => Err(anyhow!("Invalid consistency level: {}", s)),
        }
    }
}

pub struct Client {
    session: Session,
    database: String,
    replication_factor: usize,
    url: String,
    prepared_statement_cache: RwLock<HashMap<String, PreparedStatement>>,
    num_nodes: usize,
    read_consistency_level: Option<Consistency>,
    write_consistency_level: Option<Consistency>,
}

impl Client {
    pub fn get_url(&self) -> &str {
        &self.url
    }

    pub fn get_num_nodes(&self) -> usize {
        self.num_nodes
    }

    pub fn get_replication_factor(&self) -> usize {
        self.replication_factor
    }

    /// Get a prepared statement from the cache
    /// if the statement is not in the cache it be added and returned
    pub async fn get_prepared_statement(&self, query: &str) -> Result<PreparedStatement> {
        let read_guard = self.prepared_statement_cache.read().await;
        if let Some(statement) = read_guard.get(query) {
            return Ok(statement.clone());
        }
        drop(read_guard);

        let mut write_guard = self.prepared_statement_cache.write().await;
        // Check if statement was added while waiting for the write guard
        if let Some(statement) = write_guard.get(query) {
            return Ok(statement.clone());
        }

        let mut statement = self
            .prepare(query.replace(":KEYSPACE:", &self.database))
            .await?;

        if let Some(level) = self.read_consistency_level {
            if !Self::is_write_query(query) {
                statement.set_consistency(level);
            }
        }

        if let Some(level) = self.write_consistency_level {
            if Self::is_write_query(query) {
                statement.set_consistency(level);
            }
        }

        write_guard.insert(query.to_string(), statement.clone());

        Ok(statement)
    }

    pub async fn reset_prepared_statement_cache(&self) {
        self.prepared_statement_cache.write().await.clear();
    }

    fn is_write_query(query: &str) -> bool {
        let query = query.to_lowercase();
        query.starts_with("insert") || query.starts_with("update") || query.starts_with("delete")
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
            replication_factor: settings.replication_factor,
            url: database_url.to_string(),
            prepared_statement_cache: RwLock::new(HashMap::new()),
            num_nodes: settings.hosts.len(),
            read_consistency_level: settings.read_consistency_level,
            write_consistency_level: settings.write_consistency_level,
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
        let url_attributes = "scylla://10.0.0.168,10.0.0.35,10.0.0.139,10.0.0.11,10.0.0.73,10.0.0.194/mdb_uniprot?connection_timeout=60&pool_size=1&write_consistency_level=quorum&read_consistency_level=one";
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
        assert_eq!(settings.read_consistency_level, Some(Consistency::One));
        assert_eq!(settings.write_consistency_level, Some(Consistency::Quorum));

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
