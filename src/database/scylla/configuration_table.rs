// 3rd party imports
use anyhow::{anyhow, Result};
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
            SCYLLA_KEYSPACE_NAME, TABLE_NAME
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

    fn set_setting<T>(client: &mut Session, key: &str, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        todo!()
    }

    fn select(client: &mut Session) -> Result<Configuration> {
        todo!()
    }

    fn insert(client: &mut Session, configuration: &Configuration) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::scylla::tests::{get_session, prepare_database_for_tests};

    fn print_type_of<T>(_: &T) {
        println!("{}", std::any::type_name::<T>())
    }

    #[tokio::test]
    pub async fn test_deserialization() {
        let session = get_session().await;
        prepare_database_for_tests(&session).await;

        session
            .query(
                "insert into macpep.config (conf_key, value) VALUES (?, ?);",
                ("test", "{\"wrapper\": [12,14]}"),
            )
            .await
            .unwrap();

        let a = ConfigurationTable::get_setting::<Vec<i32>>(&session, "test")
            .await
            .unwrap()
            .unwrap();

        print!("a: {:?}\n", a);
        print_type_of(&a);
    }
}
