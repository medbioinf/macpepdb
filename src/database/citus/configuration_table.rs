// 3rd party imports
use anyhow::{
    anyhow,
    Result
};
use postgres::GenericClient;
use serde::{
    ser::Serialize,
    de::DeserializeOwned
};
use serde_json::{
    json,
    from_value as from_json_value,
    Value as JsonValue
};

// internal imports
use crate::entities::configuration::Configuration;

const TABLE_NAME: &'static str = "config";
const JSON_KEY: &'static str = "wrapper";

const ENZYME_NAME_KEY: &'static str = "enzyme_name";

const MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY: &'static str = "max_number_of_missed_cleavages";
const MIN_PEPTIDE_LENGTH_KEY: &'static str = "min_peptide_length";
const MAX_PEPTIDE_LENGTH_KEY: &'static str = "max_peptide_length";
const REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY: &'static str = "remove_peptides_containing_unknown";
const PARTITION_LIMITS_KEY: &'static str = "partition_limits";


/// Error for incomplete configurations.
/// 
#[derive(Debug)]
pub struct ConfigurationIncompleteError {
    configuration_key: String
}

impl ConfigurationIncompleteError {
    pub fn new(key: &str) -> Self {
        Self {
            configuration_key: format!("{} not found", key)
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

impl ConfigurationTable {
    /// Returns table name
    /// 
    pub fn table_name() -> &'static str {
        TABLE_NAME
    }

    /// Returns the value for the saved for the given key.
    /// 
    /// # Arguments
    /// * `client` - A database client
    /// * `key` - The key to look up
    /// 
    pub fn get_setting<C, T>(
        client: &mut C, key: &str
    ) -> Result<Option<T>> where C: GenericClient, T: DeserializeOwned {
        let statement = format!("SELECT value FROM {} WHERE conf_key = $1;", TABLE_NAME);
        match client.query_opt(&statement, &[&key])? {
            Some(row) => {
                let mut wrapper: JsonValue = row.get(0);
                let value = match wrapper.get_mut(JSON_KEY) {
                    Some(value) => Some(from_json_value(value.take())?),
                    None => None
                };
                Ok(value)
            },
            None => {
                Ok(None)
            }
        }
    }

    /// Sets the value for the given key.
    /// 
    /// # Arguments
    /// * `client` - A database client
    /// * `key` - The key to look up
    /// * `value` - The value to save
    /// 
    pub fn set_setting<C, T>(
        client: &mut C, key: &str, value: &T
    ) -> Result<()> where C: GenericClient, T: Serialize {
        let statement = format!("INSERT INTO {} (conf_key, value) VALUES ($1, $2);", TABLE_NAME);
        let wrapper = json!({
            JSON_KEY: value
        });
        client.execute(
            &statement, 
            &[&key, &wrapper]
        )?;
        Ok(())
    }

    /// Selects the configuration from the database.
    /// 
    /// # Arguments
    /// * `client` - A database client
    /// 
    pub fn select<C>(client: &mut C) -> Result<Configuration>
    where C: GenericClient {
        let enzyme_name = Self::get_setting::<C, String>(
            client,
            ENZYME_NAME_KEY
        )?.ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(ENZYME_NAME_KEY)))?;

        let max_number_of_missed_cleavages = Self::get_setting::<C, i16>(
            client,
            MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY
        )?.ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY)))?;

        let min_peptide_length = Self::get_setting::<C, i16>(
            client,
            MIN_PEPTIDE_LENGTH_KEY,
        )?.ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(MIN_PEPTIDE_LENGTH_KEY)))?;

        let max_peptide_length = Self::get_setting::<C, i16>(
            client,
            MAX_PEPTIDE_LENGTH_KEY,
        )?.ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(MAX_PEPTIDE_LENGTH_KEY)))?;

        // remove_peptides_containing_unknown
        let remove_peptides_containing_unknown = Self::get_setting::<C, bool>(
            client,
            REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY
        )?.ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY)))?;

        // remove_peptides_containing_unknown
        let partition_limits = Self::get_setting::<C, Vec<i64>>(
            client,
            PARTITION_LIMITS_KEY
        )?.ok_or_else(|| anyhow!(ConfigurationIncompleteError::new(PARTITION_LIMITS_KEY)))?;

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
    pub fn insert<C>(client: &mut C, configuration: &Configuration) -> Result<()>
    where C: GenericClient {
        let mut transaction = client.transaction()?;

        Self::set_setting::<_, String>(
            &mut transaction,
            ENZYME_NAME_KEY,
            &configuration.get_enzyme_name().to_owned()
        )?;

        Self::set_setting::<_, i16>(
            &mut transaction,
            MAX_NUMBER_OF_MISSED_CLEAVAGES_KEY,
            &(configuration.get_max_number_of_missed_cleavages() as i16)
        )?;


        Self::set_setting::<_, i16>(
            &mut transaction,
            MIN_PEPTIDE_LENGTH_KEY,
            &(configuration.get_min_peptide_length() as i16)
            
        )?;

        Self::set_setting::<_, i16>(
            &mut transaction,
            MAX_PEPTIDE_LENGTH_KEY,
            &(configuration.get_max_peptide_length() as i16)
        )?;

        Self::set_setting::<_, bool>(
            &mut transaction,
            REMOVE_PEPTIDES_CONTAINING_UNKNOWN_KEY,
            &configuration.get_remove_peptides_containing_unknown()
        )?;

        Self::set_setting::<_, Vec<i64>>(
            &mut transaction,
            PARTITION_LIMITS_KEY,
            configuration.get_partition_limits()
        )?;

        transaction.commit()?;

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    // 3rd party imports
    use serial_test::serial;

    // internal imports
    use crate::database::citus::tests::{prepare_database_for_tests, get_client};
    use super::*;

    const EXPECTED_ENZYME_NAME: &'static str = "Trypsin";
    const EXPECTED_MAX_MISSED_CLEAVAGES: usize = 2;
    const EXPECTED_MIN_PEPTIDE_LEN: usize = 6;
    const EXPECTED_MAX_PEPTIDE_LEN: usize = 50;
    const EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN: bool = true;

    lazy_static! {
        static ref EXPECTED_PARTITION_LIMITS: Vec<i64> = vec![0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];
    }

    /// Tests selecting without inserting first.
    /// Should result in an `ConfigurationIncompleteError`.
    /// 
    #[test]
    #[serial]
    fn test_select_without_insert() {
        prepare_database_for_tests();
        let mut client = get_client();

        let configuration_res = ConfigurationTable::select(&mut client);

        assert!(configuration_res.is_err());
        assert!(configuration_res.unwrap_err().is::<ConfigurationIncompleteError>());
    }

    /// Tests inserting
    /// 
    #[test]
    #[serial]
    fn test_insert() {
        prepare_database_for_tests();
        let mut client = get_client();

        let configuration = Configuration::new(
            EXPECTED_ENZYME_NAME.to_owned(),
            EXPECTED_MAX_MISSED_CLEAVAGES as i16,
            EXPECTED_MIN_PEPTIDE_LEN as i16,
            EXPECTED_MAX_PEPTIDE_LEN as i16,
            EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN,
            EXPECTED_PARTITION_LIMITS.clone()
        );

        ConfigurationTable::insert(&mut client, &configuration).unwrap();  
    }

    /// Tests selecting after inserting
    /// 
    #[test]
    #[serial]
    fn test_select() {
        test_insert();
        let mut client = get_client();

        let configuration = ConfigurationTable::select(&mut client).unwrap();

        assert_eq!(configuration.get_enzyme_name(), EXPECTED_ENZYME_NAME);
        assert_eq!(configuration.get_max_number_of_missed_cleavages(), EXPECTED_MAX_MISSED_CLEAVAGES);
        assert_eq!(configuration.get_min_peptide_length(), EXPECTED_MIN_PEPTIDE_LEN);
        assert_eq!(configuration.get_max_peptide_length(), EXPECTED_MAX_PEPTIDE_LEN);
        assert_eq!(configuration.get_remove_peptides_containing_unknown(), EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN);
        assert_eq!(configuration.get_partition_limits(), EXPECTED_PARTITION_LIMITS.as_slice());
    }
}