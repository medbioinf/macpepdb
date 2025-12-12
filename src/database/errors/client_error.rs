use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Configurartion error: {0}")]
    ConfigError(tokio_postgres::error::Error),
    #[error("Pool creation error: {0}")]
    PoolCreationError(#[from] deadpool_postgres::BuildError),
    #[error("Get connection error: {0}")]
    GetConnectionError(#[from] deadpool_postgres::PoolError),
    #[error("Migration error: {0}")]
    MigrationError(#[from] refinery::error::Error),
    #[error("Database creation error: {0}")]
    DatabaseCreationError(tokio_postgres::error::Error),
    #[error("Database dropping error: {0}")]
    DatabaseDroppingError(tokio_postgres::error::Error),
    #[error("Extension installation error: {0}")]
    ExtensionInstallationError(tokio_postgres::error::Error),
    #[error("Query error: {0}")]
    QueryError(#[from] tokio_postgres::error::Error),
    #[error("No database specified")]
    NoDatabaseSpecified,
}
