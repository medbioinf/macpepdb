// 3rd party imports
use anyhow::{Context, Ok, Result};
use chrono::Utc;
use scylla::_macro_internal::CqlValue;
use tracing::{debug, info};

// internal imports
use crate::{
    database::{
        generic_client::GenericClient,
        scylla::{create_keyspace_if_not_exists, schema::UP},
    },
    tools::cql::get_cql_value,
};

use crate::database::scylla::client::Client;

pub async fn run_migrations(client: &Client) -> Result<()> {
    info!("Running Scylla migrations");

    create_keyspace_if_not_exists(&client).await;

    let latest_migration_id = get_latest_migration_id(&client).await.unwrap_or(0);
    info!("Latest migration id in DB: {}", latest_migration_id);

    for (i, statement) in UP.iter().skip(latest_migration_id as usize).enumerate() {
        info!("Running migration {}", i + 1);
        debug!(statement);

        let migration_history_statement = format!(
            "INSERT INTO {}.migrations (pk, id, created, description) VALUES ('pk1', ?, ?, ?);",
            client.get_database()
        );

        let statement = statement.replace(":KEYSPACE:", client.get_database());
        // Have to do it consecutively because batch statements dont allow CREATE
        client.query(statement.as_str(), &[]).await.unwrap();
        client
            .query(
                migration_history_statement,
                (
                    CqlValue::Int(1 + latest_migration_id + i as i32),
                    Utc::now().to_string(),
                    CqlValue::Text(statement.to_string()),
                ),
            )
            .await
            .context(format!("Error on migration statement: {}", statement))?;
    }
    Ok(())
}

pub async fn get_latest_migration_id(client: &Client) -> Result<i32> {
    let latest_migration_id_query: String = format!(
        "SELECT id FROM {}.migrations WHERE pk='pk1' ORDER BY id DESC LIMIT 1",
        client.get_database()
    );

    let row = client
        .query(latest_migration_id_query, [])
        .await?
        .first_row()?;

    let id: i32 = get_cql_value(&row.columns, 0).unwrap().as_int().unwrap();

    Ok(id)
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use tracing_test::traced_test;

    use crate::database::generic_client::GenericClient;
    use crate::database::scylla::tests::DATABASE_URL;
    use crate::database::scylla::{
        client::Client, drop_keyspace, migrations::get_latest_migration_id,
        prepare_database_for_tests, schema::UP,
    };

    use super::run_migrations;

    #[tokio::test]
    #[serial]
    async fn test_get_latest_migration_id() {
        let client = Client::new(DATABASE_URL).await.unwrap();

        prepare_database_for_tests(&client).await;

        let prepared = client
            .prepare(format!(
                "INSERT INTO {}.migrations (pk, id, created, description) VALUES (?,?,?,?)",
                client.get_database()
            ))
            .await
            .unwrap();

        client
            .execute(&prepared, ("pk1", 1, 123, "yo"))
            .await
            .unwrap();

        client
            .execute(&prepared, ("pk1", 2, 123, "zzz"))
            .await
            .unwrap();

        client
            .execute(&prepared, ("pk1", 3, 123, "oy"))
            .await
            .unwrap();

        assert_eq!(get_latest_migration_id(&client).await.unwrap(), 3);
    }

    #[tokio::test]
    #[serial]
    #[traced_test]
    pub async fn test_run_migrations() {
        let client = Client::new(DATABASE_URL).await.unwrap();
        drop_keyspace(&client).await;

        run_migrations(&client).await.unwrap();
        assert_eq!(
            get_latest_migration_id(&client).await.unwrap(),
            UP.len() as i32
        );
        run_migrations(&client).await.unwrap();
        assert_eq!(
            get_latest_migration_id(&client).await.unwrap(),
            UP.len() as i32
        );
    }
}
