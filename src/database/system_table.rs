use std::sync::LazyLock;

use serde::{de::DeserializeOwned, ser::Serialize};

use crate::database::client::Client;
use crate::database::errors::system_table_error::SystemTableError;
use crate::database::table::Table;

pub const TABLE_NAME: &str = "system";
pub const ID_COL: &str = "id";
pub const VALUE_COL: &str = "value";

static SELECT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!("SELECT {ID_COL}, {VALUE_COL} FROM {TABLE_NAME} WHERE {ID_COL} = $1;")
});

static UPSERT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!("INSERT INTO {TABLE_NAME} ({ID_COL}, {VALUE_COL}) VALUES ($1, $2) ON CONFLICT ({ID_COL}) DO UPDATE SET {VALUE_COL} = EXCLUDED.{VALUE_COL};")
});

pub trait IsSystemTableCompatible: DeserializeOwned + Serialize {
    fn system_table_id() -> &'static str;
}

/// Simple table for storing various types configuration values.
/// Table constists of two columns, conf_key (VARCHAR(256)) and value (JSONB).
/// Each value will be wrapped in a JSON object with a single key, "wrapper" and than stored.
///
pub struct SystemTable {}

impl Table for SystemTable {
    /// Returns table name
    ///
    fn table_name() -> &'static str {
        TABLE_NAME
    }
}

impl SystemTable {
    /// Returns the saved document fit the given ID
    ///
    /// # Arguments
    /// * `client` - A database client
    /// * `key` - The key to look up
    ///
    pub async fn select<T>(client: &Client) -> Result<Option<T>, SystemTableError>
    where
        T: IsSystemTableCompatible,
    {
        let conn = client.pool().get().await?;
        let prepared_statement = conn.prepare_cached(SELECT_STATEMENT.as_str()).await?;

        if let Some(row) = conn
            .query_opt(&prepared_statement, &[&T::system_table_id()])
            .await?
        {
            let value: serde_json::Value = row.get("value");
            let value: T =
                serde_json::from_value(value).map_err(SystemTableError::DeserializationError)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Inserts the configuration into the database.
    ///
    /// # Arguments
    /// * `client` - A database client
    /// * `configuration` - The configuration to insert
    ///
    pub async fn upsert<T>(client: &mut Client, value: &T) -> Result<(), SystemTableError>
    where
        T: IsSystemTableCompatible,
    {
        let conn = client.pool().get().await?;
        let prepared_statement = conn.prepare_cached(UPSERT_STATEMENT.as_str()).await?;

        let value = serde_json::to_value(value).map_err(SystemTableError::SerializationError)?;

        conn.execute(&prepared_statement, &[&T::system_table_id(), &value])
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::{database::client::tests::get_test_client, entities::configuration::Configuration};

    const EXPECTED_ENZYME_NAME: &str = "Trypsin";
    const EXPECTED_MAX_MISSED_CLEAVAGES: Option<usize> = Some(2);
    const EXPECTED_MIN_PEPTIDE_LEN: Option<usize> = Some(6);
    const EXPECTED_MAX_PEPTIDE_LEN: Option<usize> = Some(50);
    const EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN: bool = true;

    static EXPECTED_PARTITION_LIMITS: LazyLock<Vec<i64>> =
        LazyLock::new(|| vec![0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]);

    /// Tests selecting without inserting first.
    /// Should result in an `ConfigurationIncompleteError`.
    ///
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_select_non_existing_configuration() {
        let client = get_test_client().await;

        let configuration_res = SystemTable::select::<Configuration>(&client).await;

        assert!(configuration_res.is_ok());
        assert!(configuration_res.unwrap().is_none());
    }

    /// Tests inserting
    ///
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_upsert_and_select() {
        let mut client = get_test_client().await;

        let configuration = Configuration::new(
            EXPECTED_ENZYME_NAME.to_owned(),
            EXPECTED_MAX_MISSED_CLEAVAGES,
            EXPECTED_MIN_PEPTIDE_LEN,
            EXPECTED_MAX_PEPTIDE_LEN,
            EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN,
            EXPECTED_PARTITION_LIMITS.clone(),
        );

        let upsert_result = SystemTable::upsert(&mut client, &configuration).await;

        assert!(upsert_result.is_ok(), "{}", upsert_result.err().unwrap());

        let retrieved_configuration = SystemTable::select::<Configuration>(&client).await;
        assert!(retrieved_configuration.is_ok());
        let retrieved_configuration = retrieved_configuration.unwrap();
        assert!(retrieved_configuration.is_some());
        let retrieved_configuration = retrieved_configuration.unwrap();
        assert_eq!(configuration, retrieved_configuration);

        // Update the configuration and test if the update works
        let updated_configuration = Configuration::new(
            "Unspecific".to_owned(),
            EXPECTED_MAX_MISSED_CLEAVAGES,
            EXPECTED_MIN_PEPTIDE_LEN,
            EXPECTED_MAX_PEPTIDE_LEN,
            EXPECTED_REMOVE_PEPTIDES_CONTAINING_UNKNOWN,
            vec![1, 2, 3, 4, 5],
        );
        let upsert_result = SystemTable::upsert(&mut client, &updated_configuration).await;
        assert!(upsert_result.is_ok(), "{}", upsert_result.err().unwrap());

        let retrieved_configuration = SystemTable::select::<Configuration>(&client).await;
        assert!(retrieved_configuration.is_ok());
        let retrieved_configuration = retrieved_configuration.unwrap();
        assert!(retrieved_configuration.is_some());
        let retrieved_configuration = retrieved_configuration.unwrap();
        assert_eq!(updated_configuration, retrieved_configuration);
    }
}
