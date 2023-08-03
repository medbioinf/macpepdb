// 3rd party imports
use anyhow::{anyhow, Result};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::{from_value as from_json_value, json, Value as JsonValue};
use tokio_postgres::GenericClient;

// internal imports
use crate::database::configuration_table::{
    ConfigurationIncompleteError, ConfigurationTable as ConfigurationTableTrait, ENZYME_NAME_KEY,
    JSON_KEY, MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY, MAX_PEPTIDE_LENGTH_KEY, MIN_PEPTIDE_LENGTH_KEY,
    PARTITION_LIMITS_KEY, REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY, TABLE_NAME,
};
use crate::database::table::Table;
use crate::entities::configuration::Configuration;

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

impl<C> ConfigurationTableTrait<C> for ConfigurationTable
where
    C: GenericClient + Send,
{
    async fn get_setting<T>(client: &C, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let statement = format!("SELECT value FROM {} WHERE conf_key = $1;", TABLE_NAME);
        match client.query_opt(&statement, &[&key]).await? {
            Some(row) => {
                let mut wrapper: JsonValue = row.get(0);
                let value = match wrapper.get_mut(JSON_KEY) {
                    Some(value) => Some(from_json_value(value.take())?),
                    None => None,
                };
                Ok(value)
            }
            None => Ok(None),
        }
    }

    async fn set_setting<T>(client: &C, key: &str, value: &T) -> Result<()>
    where
        T: Serialize + Sync,
    {
        let statement = format!(
            "INSERT INTO {} (conf_key, value) VALUES ($1, $2);",
            TABLE_NAME
        );
        let wrapper = json!({ JSON_KEY: value });
        client.execute(&statement, &[&key, &wrapper]).await?;
        Ok(())
    }

    async fn select(client: &C) -> Result<Configuration>
    where
        C: GenericClient,
    {
        let enzyme_name = Self::get_setting::<String>(client, ENZYME_NAME_KEY)
            .await?
            .ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(ENZYME_NAME_KEY)))?;

        let max_number_of_missed_cleavages =
            Self::get_setting::<i16>(client, MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY)
                .await?
                .ok_or_else(|| {
                    anyhow!(ConfigurationIncompleteError::new(
                        MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY
                    ))
                })?;

        let min_peptide_length = Self::get_setting::<i16>(client, MIN_PEPTIDE_LENGTH_KEY)
            .await?
            .ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(MIN_PEPTIDE_LENGTH_KEY)))?;

        let max_peptide_length = Self::get_setting::<i16>(client, MAX_PEPTIDE_LENGTH_KEY)
            .await?
            .ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(MAX_PEPTIDE_LENGTH_KEY)))?;

        // remove_peptides_containing_unknown
        let remove_peptides_containing_unknown =
            Self::get_setting::<bool>(client, REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY)
                .await?
                .ok_or_else(|| {
                    anyhow!(ConfigurationIncompleteError::new(
                        REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY
                    ))
                })?;

        // remove_peptides_containing_unknown
        let partition_limits = Self::get_setting::<Vec<i64>>(client, PARTITION_LIMITS_KEY)
            .await?
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

    async fn insert(client: &mut C, configuration: &Configuration) -> Result<()> {
        let transaction = client.transaction().await?;

        ConfigurationTable::set_setting::<String>(
            &transaction,
            ENZYME_NAME_KEY,
            &configuration.get_enzyme_name().to_owned(),
        )
        .await?;

        ConfigurationTable::set_setting::<i16>(
            &transaction,
            MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY,
            &(configuration.get_max_number_of_missed_cleavages() as i16),
        )
        .await?;

        ConfigurationTable::set_setting::<i16>(
            &transaction,
            MIN_PEPTIDE_LENGTH_KEY,
            &(configuration.get_min_peptide_length() as i16),
        )
        .await?;

        ConfigurationTable::set_setting::<i16>(
            &transaction,
            MAX_PEPTIDE_LENGTH_KEY,
            &(configuration.get_max_peptide_length() as i16),
        )
        .await?;

        ConfigurationTable::set_setting::<bool>(
            &transaction,
            REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY,
            &configuration.get_remove_peptides_containing_unknown(),
        )
        .await?;

        ConfigurationTable::set_setting::<Vec<i64>>(
            &transaction,
            PARTITION_LIMITS_KEY,
            configuration.get_partition_limits(),
        )
        .await?;

        transaction.commit().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // 3rd party imports
    use serial_test::serial;
    use tracing::error;

    // internal imports
    use super::*;
    use crate::database::citus::tests::{get_client, prepare_database_for_tests};

    const EXPECTED_ENZYME_NAME: &'static str = "Trypsin";
    const EXPECTED_MAX_MISSED_CLEAVAGES: usize = 2;
    const EXPECTED_MIN_PEPTIDE_LEN: usize = 6;
    const EXPECTED_MAX_PEPTIDE_LEN: usize = 50;
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
        let (mut client, connection) = get_client().await;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        prepare_database_for_tests(&mut client).await;

        let configuration_res = ConfigurationTable::select(&client).await;

        assert!(configuration_res.is_err());
        assert!(configuration_res
            .unwrap_err()
            .is::<ConfigurationIncompleteError>());

        connection_handle.abort();
        let _ = connection_handle.await;
    }

    /// Tests inserting
    /// Unfortunately you cannot call an async test from another async test. So we need outsource the implementation,
    /// as other tests depends on it
    ///
    async fn test_insert_internal() {
        let (mut client, connection) = get_client().await;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        prepare_database_for_tests(&mut client).await;

        let configuration = Configuration::new(
            EXPECTED_ENZYME_NAME.to_owned(),
            EXPECTED_MAX_MISSED_CLEAVAGES as i16,
            EXPECTED_MIN_PEPTIDE_LEN as i16,
            EXPECTED_MAX_PEPTIDE_LEN as i16,
            EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN,
            EXPECTED_PARTITION_LIMITS.clone(),
        );

        ConfigurationTable::insert(&mut client, &configuration)
            .await
            .unwrap();

        connection_handle.abort();
        let _ = connection_handle.await;
    }

    /// Tests inserting
    ///
    #[tokio::test]
    #[serial]
    async fn test_insert() {
        test_insert_internal().await;
    }

    /// Tests selecting after inserting
    ///
    #[tokio::test]
    #[serial]
    async fn test_select() {
        test_insert_internal().await;
        let (client, connection) = get_client().await;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        let configuration = ConfigurationTable::select(&client).await.unwrap();

        assert_eq!(configuration.get_enzyme_name(), EXPECTED_ENZYME_NAME);
        assert_eq!(
            configuration.get_max_number_of_missed_cleavages(),
            EXPECTED_MAX_MISSED_CLEAVAGES
        );
        assert_eq!(
            configuration.get_min_peptide_length(),
            EXPECTED_MIN_PEPTIDE_LEN
        );
        assert_eq!(
            configuration.get_max_peptide_length(),
            EXPECTED_MAX_PEPTIDE_LEN
        );
        assert_eq!(
            configuration.get_remove_peptides_containing_unknown(),
            EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN
        );
        assert_eq!(
            configuration.get_partition_limits(),
            EXPECTED_PARTITION_LIMITS.as_slice()
        );

        connection_handle.abort();
        let _ = connection_handle.await;
    }
}
