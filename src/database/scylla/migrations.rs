// 3rd party imports
use anyhow::{Context, Ok, Result};
use chrono::Utc;
use scylla::value::CqlValue;
use tracing::{debug, info};

// internal imports
use crate::database::{
    generic_client::GenericClient,
    scylla::{create_keyspace_if_not_exists, schema::UP},
};

use crate::database::scylla::client::Client;

pub async fn run_migrations(client: &Client) -> Result<()> {
    info!("Running Scylla migrations");

    create_keyspace_if_not_exists(client).await;

    let latest_migration_id = get_latest_migration_id(client).await.unwrap_or(0);
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
        client.query_unpaged(statement.as_str(), &[]).await.unwrap();
        client
            .query_unpaged(
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
        .query_unpaged(latest_migration_id_query, [])
        .await?
        .into_rows_result()?
        .first_row::<(i32,)>()?;

    Ok(row.0 as i32)
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use crate::database::generic_client::GenericClient;
    use crate::database::scylla::tests::get_test_database_url;
    use crate::database::scylla::{
        client::Client, drop_keyspace, migrations::get_latest_migration_id,
        prepare_database_for_tests, schema::UP,
    };

    use super::run_migrations;

    #[tokio::test]
    #[serial]
    async fn test_get_latest_migration_id() {
        let client = Client::new(&get_test_database_url()).await.unwrap();

        prepare_database_for_tests(&client).await;

        let prepared = client
            .prepare(format!(
                "INSERT INTO {}.migrations (pk, id, created, description) VALUES (?,?,?,?)",
                client.get_database()
            ))
            .await
            .unwrap();

        client
            .execute_unpaged(&prepared, ("pk1", 1, "123", "yo"))
            .await
            .unwrap();

        client
            .execute_unpaged(&prepared, ("pk1", 2, "123", "zzz"))
            .await
            .unwrap();

        client
            .execute_unpaged(&prepared, ("pk1", 3, "123", "oy"))
            .await
            .unwrap();

        assert_eq!(get_latest_migration_id(&client).await.unwrap(), 3);
    }

    #[tokio::test]
    #[serial]
    pub async fn test_run_migrations() {
        let client = Client::new(&get_test_database_url()).await.unwrap();
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
