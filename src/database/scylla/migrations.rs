// 3rd party imports
use anyhow::Result;
use chrono::Utc;
use scylla::_macro_internal::CqlValue;
use tracing::{debug, info};

// internal imports
use crate::{
    database::scylla::{create_keyspace_if_not_exists, schema::UP},
    tools::cql::get_cql_value,
};

use super::{
    client::{Client, GenericClient},
    SCYLLA_KEYSPACE_NAME,
};

pub async fn run_migrations(client: &Client) {
    info!("Running Scylla migrations");
    let session = client.get_session();

    create_keyspace_if_not_exists(&client).await;

    let latest_migration_id = get_latest_migration_id(&client).await.unwrap_or(0);
    info!("Latest migration id in DB: {}", latest_migration_id);

    for (i, statement) in UP.iter().skip(latest_migration_id as usize).enumerate() {
        info!("Running migration {}", i + 1);
        debug!(statement);

        let migration_history_statement = format!(
            "INSERT INTO {}.migrations (pk, id, created, description) VALUES ('pk1', ?, ?, ?);",
            SCYLLA_KEYSPACE_NAME
        );

        // Have to do it consecutively because batch statements dont allow CREATE
        session.query(*statement, &[]).await.unwrap();
        session
            .query(
                migration_history_statement,
                (
                    CqlValue::Int(1 + latest_migration_id + i as i32),
                    Utc::now().to_string(),
                    CqlValue::Text(statement.to_string()),
                ),
            )
            .await
            .unwrap();
    }
}

pub async fn get_latest_migration_id(client: &Client) -> Result<i32> {
    let session = client.get_session();

    let latest_migration_id_query: String = format!(
        "SELECT id FROM {}.migrations WHERE pk='pk1' ORDER BY id DESC LIMIT 1",
        SCYLLA_KEYSPACE_NAME
    );

    let row = session
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

    use crate::database::scylla::{
        client::GenericClient, drop_keyspace, get_client, migrations::get_latest_migration_id,
        prepare_database_for_tests, schema::UP,
    };

    use super::run_migrations;

    #[tokio::test]
    #[serial]
    async fn test_get_latest_migration_id() {
        let client = get_client(None).await.unwrap();
        let session = client.get_session();

        prepare_database_for_tests(&client).await;

        let prepared = session
            .prepare(
                "INSERT INTO macpep.migrations (pk, id, created, description) VALUES (?,?,?,?)",
            )
            .await
            .unwrap();

        session
            .execute(&prepared, ("pk1", 1, 123, "yo"))
            .await
            .unwrap();

        session
            .execute(&prepared, ("pk1", 2, 123, "zzz"))
            .await
            .unwrap();

        session
            .execute(&prepared, ("pk1", 3, 123, "oy"))
            .await
            .unwrap();

        assert_eq!(get_latest_migration_id(&client).await.unwrap(), 3);
    }

    #[tokio::test]
    #[serial]
    #[traced_test]
    pub async fn test_run_migrations() {
        let client = get_client(None).await.unwrap();
        drop_keyspace(&client).await;

        run_migrations(&client).await;
        assert_eq!(
            get_latest_migration_id(&client).await.unwrap(),
            UP.len() as i32
        );
        run_migrations(&client).await;
        assert_eq!(
            get_latest_migration_id(&client).await.unwrap(),
            UP.len() as i32
        );
    }
}
