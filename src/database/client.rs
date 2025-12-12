use std::{
    ops::{Deref, DerefMut},
    str::FromStr,
};

use deadpool_postgres::{Manager, Pool};
use refinery::Report;
use tokio_postgres::{Config, NoTls};

use crate::database::errors::client_error::ClientError;

pub struct Client {
    connection_config: Config,
    pool: Pool,
}

impl Client {
    pub async fn from_config(
        connection_config: Config,
        num_connections: usize,
    ) -> Result<Self, ClientError> {
        let manager = Manager::new(connection_config.clone(), NoTls);
        let pool = Pool::builder(manager)
            .max_size(num_connections)
            .build()
            .map_err(ClientError::PoolCreationError)?;

        Ok(Client {
            connection_config,
            pool,
        })
    }

    pub async fn from_url(
        connection_url: &str,
        num_connections: usize,
    ) -> Result<Self, ClientError> {
        let connection_config =
            Config::from_str(connection_url).map_err(ClientError::ConfigError)?;

        Self::from_config(connection_config, num_connections).await
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    pub fn connection_config(&self) -> &Config {
        &self.connection_config
    }

    pub async fn migrate(&self) -> Result<Report, ClientError> {
        let mut conn = self.pool.get().await?;
        let client = conn.as_mut().deref_mut();
        Ok(embedded::migrations::runner().run_async(client).await?)
    }

    /// Creates the database if it does not exists and installes the Citus extension.
    /// User must have the rights to create databases.
    ///
    /// # Arguments
    /// * connection_url - The connection URL to the database server
    ///
    pub async fn create_database(connection_url: &str) -> Result<(), ClientError> {
        let mut config = Config::from_str(connection_url).map_err(ClientError::ConfigError)?;
        let database_name = config
            .get_dbname()
            .ok_or(ClientError::NoDatabaseSpecified)?
            .to_string();

        // Connect to the default "postgres" database to create the target database
        config.dbname("postgres");
        let client = Self::from_config(config.clone(), 1).await?;
        let conn = client.pool().get().await?;
        conn.execute(format!("CREATE DATABASE {database_name}").as_str(), &[])
            .await
            .map_err(ClientError::DatabaseCreationError)?;

        loop {
            if conn
                .query_opt(
                    "SELECT FROM pg_database WHERE datname = $1",
                    &[&database_name],
                )
                .await
                .map_err(ClientError::DatabaseDroppingError)?
                .is_none()
            {
                println!("Waiting for database {database_name} to be created...");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
            break;
        }

        drop(conn);
        drop(client);

        // Reconnect to the newly created database to install the Citus extension
        config.dbname(&database_name);
        let client = Self::from_config(config, 1).await?;
        let conn = client.pool().get().await?;
        conn.execute("CREATE EXTENSION IF NOT EXISTS citus;", &[])
            .await
            .map_err(ClientError::ExtensionInstallationError)?;

        Ok(())
    }
}

impl Deref for Client {
    type Target = Pool;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

mod embedded {
    use refinery::embed_migrations;

    embed_migrations!("./migrations");
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const CONNECTION_URL: &str = "postgres://postgres:postgres@localhost:5432/macpepdb";

    /// Drops the specified database.
    ///
    /// # Arguments
    /// * connection_url - The connection URL to the database server
    ///
    async fn drop_database(connection_url: &str) -> Result<(), ClientError> {
        let mut config = Config::from_str(connection_url).map_err(ClientError::ConfigError)?;
        let database_name = config
            .get_dbname()
            .ok_or(ClientError::NoDatabaseSpecified)?
            .to_string();

        // Connect to the default "postgres" database to drop the target database
        config.dbname("postgres");
        let client = Client::from_config(config.clone(), 1).await?;
        let conn = client.pool().get().await?;
        conn.execute(
            format!("DROP DATABASE IF EXISTS {database_name}").as_str(),
            &[],
        )
        .await
        .map_err(ClientError::DatabaseDroppingError)?;

        loop {
            if conn
                .query_opt(
                    "SELECT FROM pg_database WHERE datname = $1",
                    &[&database_name],
                )
                .await
                .map_err(ClientError::DatabaseDroppingError)?
                .is_some()
            {
                println!("Waiting for database {database_name} to be dropped...");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
            break;
        }

        Ok(())
    }

    /// Helper function to create a fresh test database and client wihout running migrations
    ///
    pub async fn get_test_client_without_migrations() -> Client {
        drop_database(CONNECTION_URL).await.unwrap();
        Client::create_database(CONNECTION_URL).await.unwrap();
        Client::from_url(CONNECTION_URL, 4).await.unwrap()
    }

    /// Helper function to create a fresh test database and client
    ///
    pub async fn get_test_client() -> Client {
        let client = get_test_client_without_migrations().await;
        client.migrate().await.unwrap();
        client
    }

    #[tokio::test()]
    async fn test_migrations() {
        let client = get_test_client_without_migrations().await;
        let report = client.migrate().await.unwrap();
        assert_eq!(
            report.applied_migrations().len(),
            embedded::migrations::runner().get_migrations().len()
        );
    }
}
