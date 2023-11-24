// 3rd party imports
use anyhow::Result;
use serde::{de::DeserializeOwned, ser::Serialize};

// internal imports
use crate::database::table::Table;
use crate::entities::configuration::Configuration;

pub const TABLE_NAME: &'static str = "config";
pub const JSON_KEY: &'static str = "wrapper";
pub const PROTEASE_NAME_KEY: &'static str = "enzyme_name";
pub const MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY: &'static str = "max_number_of_missed_cleavages";
pub const MIN_PEPTIDE_LENGTH_KEY: &'static str = "min_peptide_length";
pub const MAX_PEPTIDE_LENGTH_KEY: &'static str = "max_peptide_length";
pub const REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY: &'static str =
    "remove_peptides_containing_unknown";
pub const PARTITION_LIMITS_KEY: &'static str = "partition_limits";

/// Error for incomplete configurations.
///
#[derive(Debug)]
pub struct ConfigurationIncompleteError {
    configuration_key: String,
}

impl ConfigurationIncompleteError {
    pub fn new(key: &str) -> Self {
        Self {
            configuration_key: format!("{} not found", key),
        }
    }
}

impl std::fmt::Display for ConfigurationIncompleteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "key '{}' is missing", self.configuration_key)
    }
}

/// Simple table for storing various types configuration values.
/// Table constists of two columns, conf_key (VARCHAR(256)) and value (JSONB).
/// Each value will be wrapped in a JSON object with a single key, "wrapper" and than stored.
///
pub trait ConfigurationTable<C>: Table {
    /// Returns the value for the saved for the given key.
    ///
    /// # Arguments
    /// * `client` - A database client
    /// * `key` - The key to look up
    ///
    fn get_setting<T>(
        client: &C,
        key: &str,
    ) -> impl std::future::Future<Output = Result<Option<T>>> + Send
    where
        T: DeserializeOwned;

    /// Sets the value for the given key.
    ///
    /// # Arguments
    /// * `client` - A database client
    /// * `key` - The key to look up
    /// * `value` - The value to save
    ///
    fn set_setting<T>(
        client: &C,
        key: &str,
        value: &T,
    ) -> impl std::future::Future<Output = Result<()>> + Send
    where
        T: Serialize + Sync;

    /// Selects the configuration from the database.
    ///
    /// # Arguments
    /// * `client` - A database client
    ///
    fn select(client: &C) -> impl std::future::Future<Output = Result<Configuration>> + Send;

    /// Inserts the configuration into the database.
    ///
    /// # Arguments
    /// * `client` - A database client
    /// * `configuration` - The configuration to insert
    ///
    fn insert(
        client: &mut C,
        configuration: &Configuration,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}
