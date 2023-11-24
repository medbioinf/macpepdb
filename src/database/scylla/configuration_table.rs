// 3rd party imports
use anyhow::{anyhow, Ok, Result};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::{from_value as from_json_value, json, Value as JsonValue};

// internal imports
use crate::database::configuration_table::{
    ConfigurationIncompleteError, ConfigurationTable as ConfigurationTableTrait, JSON_KEY,
    MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY, MAX_PEPTIDE_LENGTH_KEY, MIN_PEPTIDE_LENGTH_KEY,
    PARTITION_LIMITS_KEY, PROTEASE_NAME_KEY, REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY, TABLE_NAME,
};
use crate::database::scylla::client::GenericClient;
use crate::database::table::Table;
use crate::entities::configuration::Configuration;

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
        let statement = format!(
            "SELECT conf_key, value FROM {}.{} WHERE conf_key = ?;",
            client.get_database(),
            Self::table_name()
        );
        let row = client
            .get_session()
            .query(statement, (key,))
            .await?
            .first_row()?;
        let (_, value) = row.into_typed::<(String, String)>()?;

        let mut wrapper: JsonValue = serde_json::from_str(value.as_str())?;
        let config_value = match wrapper.get_mut(JSON_KEY) {
            Some(val) => Some(from_json_value(val.take())?),
            None => None,
        };

        Ok(config_value)
    }

    async fn set_setting<T>(client: &C, key: &str, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let statement = format!(
            "INSERT INTO {}.{} (conf_key, value) VALUES (?,?);",
            client.get_database(),
            Self::table_name()
        );

        let wrapper = json!({ JSON_KEY: value });

        client
            .get_session()
            .query(statement, (key, wrapper.to_string()))
            .await?;
        Ok(())
    }

    async fn select(client: &C) -> Result<Configuration> {
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

    async fn insert(client: &mut C, configuration: &Configuration) -> Result<()> {
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
    use crate::database::scylla::client::Client;
    use crate::database::scylla::prepare_database_for_tests;
    use crate::database::scylla::tests::DATABASE_URL;

    const EXPECTED_ENZYME_NAME: &'static str = "Trypsin";
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
        let mut client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        let configuration_res = ConfigurationTable::select(&mut client).await;
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

        let actual_configuration = ConfigurationTable::select(&mut client).await.unwrap();

        assert_eq!(expected_configuration, actual_configuration);
    }
}
