use std::ops::Deref;

// 3rd party imports
use anyhow::Result;
use tokio::task::JoinHandle;
use tokio_postgres::Client as PgClient;
use tracing::error;

// local imports
use super::blob_table::BlobTable;
use crate::database::alternative::blob_table::BlobTable as BlobTableTrait;
use crate::database::alternative::client::Client as ClientTrait;

pub struct Client {
    inner_client: PgClient,
    connection_task: JoinHandle<()>,
    database: String,
    url: String,
}

impl Client {
    /// Returns the task that keeps the connection allive
    ///
    pub fn connection_task(&self) -> &JoinHandle<()> {
        &self.connection_task
    }
}

impl Deref for Client {
    type Target = PgClient;

    fn deref(&self) -> &Self::Target {
        &self.inner_client
    }
}

impl<'a> ClientTrait<'a, PgClient> for Client {
    type BlobTable = BlobTable<'a>;

    /// Creates a new ScyllaDB client
    ///
    /// # Arguments
    /// * `database_url` - A string slice that holds the database URL
    ///
    async fn new(database_url: &str) -> Result<Self>
    where
        Self: Sized,
    {
        let (inner_client, connection) =
            tokio_postgres::connect(database_url, tokio_postgres::NoTls).await?;

        let connection_task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        Ok(Self {
            inner_client,
            connection_task,
            database: "mdb_uniprot".to_string(), // TODO: parse from URL
            url: database_url.to_string(),
        })
    }

    fn get_database(&self) -> &str {
        &self.database
    }

    fn get_database_url(&self) -> &str {
        &self.url
    }

    fn get_inner_client(&self) -> &PgClient {
        &self.inner_client
    }

    fn blob_table(&'a self) -> Self::BlobTable {
        Self::BlobTable::new(self)
    }
}
