// std imports
use std::{collections::HashMap, num::NonZeroUsize, ops::Deref, sync::LazyLock, time::Duration};

use fancy_regex::Regex;
use scylla::{
    client::{PoolSize, caching_session::CachingSession, session_builder::SessionBuilder},
    statement::Consistency,
};
use thiserror::Error;

pub static URL_PASER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?m)scylla://((?P<credentials>[^:]*?:[^:]+)@){0,1}(?P<hosts>.+)/(?P<keyspace>[^/?]+)(\?(?P<attributes>.+)){0,1}").unwrap()
});

static DEFAULT_CACHE_SIZE: usize = 100;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Poolsize need to be larger than 0")]
    InvalidPoolSize,
    #[error("Invalid database URL: {0}")]
    InvalidUrl(String),
    #[error("Failed to create ScyllaDB session: {0}")]
    Session(#[from] scylla::errors::NewSessionError),
    #[error("Failed to parse attribute {0} with value {2} into {1}")]
    ParsingAttribute(&'static str, &'static str, String),
    #[error("Unable to prepare statement `{0}`: {1}")]
    CqlPrepare(String, Box<scylla::errors::PrepareError>),
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
    pub cache_size: usize,
    pub user: Option<String>,
    pub password: Option<String>,
    pub connection_timeout: Option<Duration>,
    pub pool_size: Option<usize>,
    pub pool_type: PoolType,
    pub read_consistency_level: Option<Consistency>,
    pub write_consistency_level: Option<Consistency>,
}

impl ClientSettings {
    pub fn new(database_url: &str) -> Result<Self, Error> {
        // Parse url
        let matches = URL_PASER_REGEX
            .captures(database_url)
            .map_err(|_| {
                Error::InvalidUrl("Format seems not to correspond to the expectations".to_string())
            })?
            .ok_or(Error::InvalidUrl(
                "Format seems not to correspond to the expectations".to_string(),
            ))?;

        // Extract hosts and keyspace as they are mandatory
        let hosts: Vec<String> = matches
            .name("hosts")
            .ok_or(Error::InvalidUrl("Hosts not found".to_string()))?
            .as_str()
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let keyspace = matches
            .name("keyspace")
            .ok_or(Error::InvalidUrl("keyspace not found".to_string()))?
            .as_str()
            .to_string();

        // Create settings
        let mut settings = Self {
            hosts,
            keyspace,
            cache_size: DEFAULT_CACHE_SIZE,
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
            settings.user = credentials.first().map(|value| value.to_string());
            settings.password = credentials.get(1).map(|value| value.to_string());
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
                settings.connection_timeout =
                    Some(Duration::from_secs(timeout.parse().map_err(|_| {
                        Error::ParsingAttribute("timeout", "u64", timeout.to_string())
                    })?));
            }

            if let Some(pool_size) = attributes.get("pool_size") {
                settings.pool_size = Some(pool_size.parse().map_err(|_| {
                    Error::ParsingAttribute("pool_size", "usize", pool_size.to_string())
                })?);
            }

            if let Some(pool_type) = attributes.get("pool_type") {
                settings.pool_type = PoolType::from(*pool_type);
            }

            if let Some(level) = attributes.get("read_consistency_level") {
                settings.read_consistency_level =
                    Some(Self::str_to_consistency_level(level).map_err(|_| {
                        Error::ParsingAttribute(
                            "read_consistency_level",
                            "Consistency",
                            level.to_string(),
                        )
                    })?);
            };

            if let Some(level) = attributes.get("write_consistency_level") {
                settings.write_consistency_level =
                    Some(Self::str_to_write_consistency_level(level).map_err(|_| {
                        Error::ParsingAttribute(
                            "write_consistency_level",
                            "Consistency",
                            level.to_string(),
                        )
                    })?);
            };

            if let Some(cache_size) = attributes.get("cache_size") {
                settings.cache_size = (cache_size).parse().map_err(|_| {
                    Error::ParsingAttribute("cache_size", "usize", cache_size.to_string())
                })?;
            }
        }
        Ok(settings)
    }

    pub async fn to_session(&self) -> Result<CachingSession, Error> {
        let mut builder = SessionBuilder::new()
            .known_nodes(self.hosts.clone())
            .use_keyspace(self.keyspace.clone(), true);

        if let Some(user) = self.user.as_ref()
            && let Some(password) = self.password.as_ref()
        {
            builder = builder.user(user, password);
        }

        if let Some(timeout) = self.connection_timeout {
            builder = builder.connection_timeout(timeout);
        }

        if let Some(pool_size) = self.pool_size {
            let pool_size = NonZeroUsize::new(pool_size).ok_or(Error::InvalidPoolSize)?;

            let pool_size = match self.pool_type {
                PoolType::PerHost => PoolSize::PerHost(pool_size),
                PoolType::PerShard => PoolSize::PerShard(pool_size),
            };
            builder = builder.pool_size(pool_size);
        }

        let session = builder.build().await?;

        Ok(CachingSession::from(session, self.cache_size))
    }

    /// Returns the ScyllaDB consistency level from a string representation
    /// which can be used for read and write operations.
    ///
    fn str_to_consistency_level(s: &str) -> Result<Consistency, ()> {
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
            _ => Err(()),
        }
    }

    /// Returns the ScyllaDB consistency level from a string representation
    /// which can be used for write operations.
    ///
    fn str_to_write_consistency_level(s: &str) -> Result<Consistency, ()> {
        if let Ok(level) = Self::str_to_consistency_level(s) {
            return Ok(level);
        }

        match s {
            "any" => Ok(Consistency::Any),
            "each_quorum" => Ok(Consistency::EachQuorum),
            _ => Err(()),
        }
    }
}

pub struct Client {
    session: CachingSession,
    database: String,
    url: String,
    num_nodes: usize,
    _read_consistency_level: Option<Consistency>,
    _write_consistency_level: Option<Consistency>,
}

impl Client {
    /// Creates a new ScyllaDB client
    ///
    /// # Arguments
    /// * `database_url` - A string slice that holds the database URL
    ///
    pub async fn new(database_url: &str) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let settings = ClientSettings::new(database_url)?;
        Ok(Self {
            session: settings.to_session().await?,
            database: settings.keyspace.clone(),
            url: database_url.to_string(),
            num_nodes: settings.hosts.len(),
            _read_consistency_level: settings.read_consistency_level,
            _write_consistency_level: settings.write_consistency_level,
        })
    }

    pub fn inner_client(&self) -> &CachingSession {
        &self.session
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn num_nodes(&self) -> usize {
        self.num_nodes
    }
}

impl Deref for Client {
    type Target = CachingSession;

    fn deref(&self) -> &Self::Target {
        &self.session
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
