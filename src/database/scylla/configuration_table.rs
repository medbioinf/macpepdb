// 3rd party imports
use anyhow::{anyhow, Context, Ok, Result};
use log::info;
use scylla::_macro_internal::CqlValue;
use scylla::cql_to_rust::FromCqlVal;
use scylla::Session;
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::{from_value as from_json_value, json, Value as JsonValue};

// internal imports
use crate::database::configuration_table::{
    ConfigurationIncompleteError, ConfigurationTable as ConfigurationTableTrait, ENZYME_NAME_KEY,
    JSON_KEY, MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY, MAX_PEPTIDE_LENGTH_KEY, MIN_PEPTIDE_LENGTH_KEY,
    PARTITION_LIMITS_KEY, REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY, TABLE_NAME,
};
use crate::database::table::Table;
use crate::entities::configuration::Configuration;

pub const SCYLLA_KEYSPACE_NAME: &str = "macpep";

pub struct ConfigurationTable {}

impl Table for ConfigurationTable {
    /// Returns table name
    ///
    fn table_name() -> &'static str {
        TABLE_NAME
    }
}

impl ConfigurationTable {
    async fn get_setting<T>(session: &Session, key: &str) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let statement = format!(
            "SELECT conf_key, value FROM {}.{} WHERE conf_key = ?;",
            SCYLLA_KEYSPACE_NAME,
            Self::table_name()
        );
        let row = session.query(statement, (key,)).await?.first_row()?;
        let (conf_key, value) = row.into_typed::<(String, String)>()?;

        let mut wrapper: JsonValue = serde_json::from_str(value.as_str())?;
        let config_value = match wrapper.get_mut(JSON_KEY) {
            Some(val) => Some(from_json_value(val.take())?),
            None => None,
        };

        Ok(config_value)
    }

    async fn set_setting<T>(session: &Session, key: &str, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let statement = format!(
            "INSERT INTO {}.{} (conf_key, value) VALUES (?,?);",
            SCYLLA_KEYSPACE_NAME,
            Self::table_name()
        );

        let wrapper = json!({ JSON_KEY: value });

        session.query(statement, (key, wrapper.to_string())).await?;
        Ok(())
    }

    async fn select(session: &Session) -> Result<Configuration> {
        let enzyme_name = Self::get_setting::<String>(session, ENZYME_NAME_KEY)
            .await
            .map_err(|error| {
                anyhow!(ConfigurationIncompleteError::new(ENZYME_NAME_KEY)).context(error)
            })?
            .ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(ENZYME_NAME_KEY)))?;

        let max_number_of_missed_cleavages =
            Self::get_setting::<i16>(session, MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY)
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

        let min_peptide_length = Self::get_setting::<i16>(session, MIN_PEPTIDE_LENGTH_KEY)
            .await
            .map_err(|error| {
                anyhow!(ConfigurationIncompleteError::new(MIN_PEPTIDE_LENGTH_KEY)).context(error)
            })?
            .ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(MIN_PEPTIDE_LENGTH_KEY)))?;

        let max_peptide_length = Self::get_setting::<i16>(session, MAX_PEPTIDE_LENGTH_KEY)
            .await
            .map_err(|error| {
                anyhow!(ConfigurationIncompleteError::new(MAX_PEPTIDE_LENGTH_KEY)).context(error)
            })?
            .ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(MAX_PEPTIDE_LENGTH_KEY)))?;

        let remove_peptides_containing_unknown =
            Self::get_setting::<bool>(session, REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY)
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

        let partition_limits = Self::get_setting::<Vec<i64>>(session, PARTITION_LIMITS_KEY)
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

    async fn insert(session: &Session, configuration: &Configuration) -> Result<()> {
        ConfigurationTable::set_setting::<String>(
            &session,
            ENZYME_NAME_KEY,
            &configuration.get_enzyme_name().to_owned(),
        )
        .await?;

        ConfigurationTable::set_setting::<i16>(
            &session,
            MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY,
            &(configuration.get_max_number_of_missed_cleavages() as i16),
        )
        .await?;

        ConfigurationTable::set_setting::<i16>(
            &session,
            MIN_PEPTIDE_LENGTH_KEY,
            &(configuration.get_min_peptide_length() as i16),
        )
        .await?;

        ConfigurationTable::set_setting::<i16>(
            &session,
            MAX_PEPTIDE_LENGTH_KEY,
            &(configuration.get_max_peptide_length() as i16),
        )
        .await?;

        ConfigurationTable::set_setting::<bool>(
            &session,
            REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY,
            &configuration.get_remove_peptides_containing_unknown(),
        )
        .await?;

        ConfigurationTable::set_setting::<Vec<i64>>(
            &session,
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

    // internal imports
    use super::*;
    use crate::database::scylla::tests::{get_session, prepare_database_for_tests};

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
        let session = get_session().await;
        prepare_database_for_tests(&session).await;

        let configuration_res = ConfigurationTable::select(&session).await;

        assert!(configuration_res.is_err());
        println!("{:?}", configuration_res);
        assert!(configuration_res
            .unwrap_err()
            .is::<ConfigurationIncompleteError>());
    }

    /// Tests inserting
    ///
    #[tokio::test]
    #[serial]
    async fn test_insert() {
        let session = get_session().await;
        prepare_database_for_tests(&session).await;

        let configuration = Configuration::new(
            EXPECTED_ENZYME_NAME.to_owned(),
            EXPECTED_MAX_MISSED_CLEAVAGES as i16,
            EXPECTED_MIN_PEPTIDE_LEN as i16,
            EXPECTED_MAX_PEPTIDE_LEN as i16,
            EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN,
            EXPECTED_PARTITION_LIMITS.clone(),
        );

        ConfigurationTable::insert(&session, &configuration)
            .await
            .unwrap();
    }

    /// Tests selecting after inserting
    ///
    #[tokio::test]
    #[serial]
    async fn test_select() {
        let session = get_session().await;
        prepare_database_for_tests(&session).await;

        let expected_configuration = Configuration::new(
            EXPECTED_ENZYME_NAME.to_owned(),
            EXPECTED_MAX_MISSED_CLEAVAGES as i16,
            EXPECTED_MIN_PEPTIDE_LEN as i16,
            EXPECTED_MAX_PEPTIDE_LEN as i16,
            EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN,
            EXPECTED_PARTITION_LIMITS.clone(),
        );

        ConfigurationTable::insert(&session, &expected_configuration)
            .await
            .unwrap();

        let actual_configuration = ConfigurationTable::select(&session).await.unwrap();

        assert_eq!(expected_configuration, actual_configuration);
    }
}
