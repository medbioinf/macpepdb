/// std import
use std::ops::Deref;

/// 3rd party imports
use anyhow::Result;

/// Wrapper for database client
///
/// # Generic parameters
/// * `C` - Database client type
pub trait GenericClient<C>: Send + Sync + Deref<Target = C> {
    fn new(database_url: &str) -> impl std::future::Future<Output = Result<Self>> + Send
    where
        Self: Sized;
    fn get_inner_client(&self) -> &C;
    fn get_database(&self) -> &str;
}
