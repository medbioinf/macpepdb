// 3rd party imports
use anyhow::{anyhow, Result};
use scylla::transport::query_result::FirstRowError;
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::{from_value as from_json_value, json, Value as JsonValue};

// internal imports
use crate::database::scylla::client::Client;
use crate::database::table::Table;
use crate::entities::configuration::Configuration;

pub const TABLE_NAME: &str = "config";
pub const JSON_KEY: &str = "wrapper";
pub const PROTEASE_NAME_KEY: &str = "enzyme_name";
pub const MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY: &str = "max_number_of_missed_cleavages";
pub const MIN_PEPTIDE_LENGTH_KEY: &str = "min_peptide_length";
pub const MAX_PEPTIDE_LENGTH_KEY: &str = "max_peptide_length";
pub const REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY: &str = "remove_peptides_containing_unknown";
pub const PARTITION_LIMITS_KEY: &str = "partition_limits";

lazy_static! {
    static ref SELECT_STATEMENT: String = format!(
        "SELECT conf_key, value FROM :KEYSPACE:.{} WHERE conf_key = ?;",
        TABLE_NAME
    );
    static ref INSERT_STATEMENT: String = format!(
        "INSERT INTO :KEYSPACE:.{} (conf_key, value) VALUES (?,?);",
        TABLE_NAME
    );
}

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
pub struct ConfigurationTable {}

impl Table for ConfigurationTable {
    /// Returns table name
    ///
    fn table_name() -> &'static str {
        TABLE_NAME
    }
}

impl ConfigurationTable {
    /// Returns the value for the saved for the given key.
    ///
    /// # Arguments
    /// * `client` - A database client
    /// * `key` - The key to look up
    ///
    async fn get_setting<T>(client: &Client, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let prepared_statement = client.get_prepared_statement(&SELECT_STATEMENT).await?;
        let row = client
            .execute_unpaged(&prepared_statement, (key,))
            .await?
            .into_rows_result()?
            .first_row::<(String, String)>();

        match row {
            Ok(row) => {
                let mut wrapper: JsonValue = serde_json::from_str(row.1.as_str())?;
                let config_value = match wrapper.get_mut(JSON_KEY) {
                    Some(val) => Some(from_json_value(val.take())?),
                    None => None,
                };

                Ok(config_value)
            }
            Err(err) => match err {
                FirstRowError::RowsEmpty => Ok(None),
                _ => Err(err.into()),
            },
        }
    }

    /// Sets the value for the given key.
    ///
    /// # Arguments
    /// * `client` - A database client
    /// * `key` - The key to look up
    /// * `value` - The value to save
    ///
    async fn set_setting<T>(client: &Client, key: &str, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let prepared_statement = client.get_prepared_statement(&INSERT_STATEMENT).await?;

        let wrapper = json!({ JSON_KEY: value });

        client
            .execute_unpaged(&prepared_statement, (key, wrapper.to_string()))
            .await?;
        Ok(())
    }

    /// Selects the configuration from the database.
    ///
    /// # Arguments
    /// * `client` - A database client
    ///
    pub async fn select(client: &Client) -> Result<Configuration> {
        let enzyme_name = Self::get_setting::<String>(client, PROTEASE_NAME_KEY)
            .await
            .map_err(|error| {
                anyhow!(ConfigurationIncompleteError::new(PROTEASE_NAME_KEY)).context(error)
            })?
            .ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(PROTEASE_NAME_KEY)))?;

        let max_number_of_missed_cleavages =
            Self::get_setting::<Option<usize>>(client, MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY)
                .await
                .map_err(|error| {
                    anyhow!(ConfigurationIncompleteError::new(
                        MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY
                    ))
                    .context(error)
                })?
                .ok_or_else(|| {
                    anyhow!(ConfigurationIncompleteError::new(
                        MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY
                    ))
                })?;

        let min_peptide_length = Self::get_setting::<Option<usize>>(client, MIN_PEPTIDE_LENGTH_KEY)
            .await
            .map_err(|error| {
                anyhow!(ConfigurationIncompleteError::new(MIN_PEPTIDE_LENGTH_KEY)).context(error)
            })?
            .ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(MIN_PEPTIDE_LENGTH_KEY)))?;

        let max_peptide_length = Self::get_setting::<Option<usize>>(client, MAX_PEPTIDE_LENGTH_KEY)
            .await
            .map_err(|error| {
                anyhow!(ConfigurationIncompleteError::new(MAX_PEPTIDE_LENGTH_KEY)).context(error)
            })?
            .ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(MAX_PEPTIDE_LENGTH_KEY)))?;

        let remove_peptides_containing_unknown =
            Self::get_setting::<bool>(client, REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY)
                .await
                .map_err(|error| {
                    anyhow!(ConfigurationIncompleteError::new(
                        REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY
                    ))
                    .context(error)
                })?
                .ok_or_else(|| {
                    anyhow!(ConfigurationIncompleteError::new(
                        REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY
                    ))
                })?;

        let partition_limits = Self::get_setting::<Vec<i64>>(client, PARTITION_LIMITS_KEY)
            .await
            .map_err(|error| {
                anyhow!(ConfigurationIncompleteError::new(PARTITION_LIMITS_KEY)).context(error)
            })?
            .ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(PARTITION_LIMITS_KEY)))?;

        Ok(Configuration::new(
            enzyme_name,
            max_number_of_missed_cleavages,
            min_peptide_length,
            max_peptide_length,
            remove_peptides_containing_unknown,
            partition_limits,
        ))
    }

    /// Inserts the configuration into the database.
    ///
    /// # Arguments
    /// * `client` - A database client
    /// * `configuration` - The configuration to insert
    ///
    pub async fn insert(client: &mut Client, configuration: &Configuration) -> Result<()> {
        ConfigurationTable::set_setting::<String>(
            client,
            PROTEASE_NAME_KEY,
            &configuration.get_protease_name().to_owned(),
        )
        .await?;

        ConfigurationTable::set_setting::<Option<usize>>(
            client,
            MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY,
            &(configuration.get_max_number_of_missed_cleavages()),
        )
        .await?;

        ConfigurationTable::set_setting::<Option<usize>>(
            client,
            MIN_PEPTIDE_LENGTH_KEY,
            &(configuration.get_min_peptide_length()),
        )
        .await?;

        ConfigurationTable::set_setting::<Option<usize>>(
            client,
            MAX_PEPTIDE_LENGTH_KEY,
            &(configuration.get_max_peptide_length()),
        )
        .await?;

        ConfigurationTable::set_setting::<bool>(
            client,
            REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY,
            &configuration.get_remove_peptides_containing_unknown(),
        )
        .await?;

        ConfigurationTable::set_setting::<Vec<i64>>(
            client,
            PARTITION_LIMITS_KEY,
            configuration.get_partition_limits(),
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // 3rd party imports
    use serial_test::serial;
    use tracing::info;

    // internal imports
    use super::*;
    use crate::database::generic_client::GenericClient;
    use crate::database::scylla::client::Client;
    use crate::database::scylla::prepare_database_for_tests;
    use crate::database::scylla::tests::DATABASE_URL;

    const EXPECTED_ENZYME_NAME: &str = "Trypsin";
    const EXPECTED_MAX_MISSED_CLEAVAGES: Option<usize> = Some(2);
    const EXPECTED_MIN_PEPTIDE_LEN: Option<usize> = Some(6);
    const EXPECTED_MAX_PEPTIDE_LEN: Option<usize> = Some(50);
    const EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN: bool = true;

    lazy_static! {
        static ref EXPECTED_PARTITION_LIMITS: Vec<i64> =
            vec![0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];
    }

    /// Tests selecting without inserting first.
    /// Should result in an `ConfigurationIncompleteError`.
    ///
    #[tokio::test]
    #[serial]
    async fn test_select_without_insert() {
        let client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        let configuration_res = ConfigurationTable::select(&client).await;
        info!("got config res");

        assert!(configuration_res.is_err());
        info!("{:?}", configuration_res);
        assert!(configuration_res
            .unwrap_err()
            .is::<ConfigurationIncompleteError>());
    }

    /// Tests inserting
    ///
    #[tokio::test]
    #[serial]
    async fn test_insert() {
        let mut client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        let configuration = Configuration::new(
            EXPECTED_ENZYME_NAME.to_owned(),
            EXPECTED_MAX_MISSED_CLEAVAGES,
            EXPECTED_MIN_PEPTIDE_LEN,
            EXPECTED_MAX_PEPTIDE_LEN,
            EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN,
            EXPECTED_PARTITION_LIMITS.clone(),
        );

        ConfigurationTable::insert(&mut client, &configuration)
            .await
            .unwrap();
    }

    /// Tests selecting after inserting
    ///
    #[tokio::test]
    #[serial]
    async fn test_select() {
        let mut client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        let expected_configuration = Configuration::new(
            EXPECTED_ENZYME_NAME.to_owned(),
            EXPECTED_MAX_MISSED_CLEAVAGES,
            EXPECTED_MIN_PEPTIDE_LEN,
            EXPECTED_MAX_PEPTIDE_LEN,
            EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN,
            EXPECTED_PARTITION_LIMITS.clone(),
        );

        ConfigurationTable::insert(&mut client, &expected_configuration)
            .await
            .unwrap();

        let actual_configuration = ConfigurationTable::select(&client).await.unwrap();

        assert_eq!(expected_configuration, actual_configuration);
    }
}
