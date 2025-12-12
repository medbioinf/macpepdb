use thiserror::Error;

#[derive(Error, Debug)]
pub enum SystemTableError {
    #[error("(Serialization error: {0}")]
    SerializationError(serde_json::Error),
    #[error("(Deserialization error: {0}")]
    DeserializationError(serde_json::Error),
    #[error("Query error: {0}")]
    QueryError(#[from] tokio_postgres::error::Error),
    #[error("Get connection error: {0}")]
    GetConnectionError(#[from] deadpool_postgres::PoolError),
}
