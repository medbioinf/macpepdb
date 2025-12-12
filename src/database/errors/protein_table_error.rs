use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProteinTableError {
    #[error("Query error: {0}")]
    QueryError(#[from] tokio_postgres::error::Error),
    #[error("Get connection error: {0}")]
    GetConnectionError(#[from] deadpool_postgres::PoolError),
}
