use std::ops::Index;

use crate::{database::scylla::get_client, tools::cql::get_cql_value};
use anyhow::{bail, Result};

use super::{client::GenericClient, SCYLLA_KEYSPACE_NAME};

pub async fn run_migrations() {}

pub async fn get_latest_migration_id() -> Result<i32> {
    let mut client = get_client().await.unwrap();
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
    use crate::database::scylla::{
        client::GenericClient, get_client, migrations::get_latest_migration_id,
        prepare_database_for_tests,
    };

    #[tokio::test]
    async fn test_get_latest_migration_id() {
        let mut client = get_client().await.unwrap();
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

        assert_eq!(get_latest_migration_id().await.unwrap(), 3);
    }
}
