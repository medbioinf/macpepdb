use anyhow::Result;
use futures::Future;

use super::blob_table::BlobTable as BlobTableTrait;

pub trait Client<'a, I>
where
    Self: Sized,
{
    type BlobTable: BlobTableTrait<'a, Self, I>;

    fn new(database_url: &str) -> impl Future<Output = Result<Self>> + Send;

    /// Return database name
    ///
    fn get_database(&self) -> &str;

    /// Returns initialized database underline
    ///
    fn get_database_url(&self) -> &str;

    /// Returns the inner client
    ///
    fn get_inner_client(&self) -> &I;

    /// Return blob table
    ///
    fn blob_table(&'a self) -> Self::BlobTable;
}

impl<'a, C: ?Sized> Client<'a, C> for Box<C>
where
    C: Client<'a, C>,
    <C as crate::database::alternative::client::Client<'a, C>>::BlobTable:
        crate::database::alternative::blob_table::BlobTable<'a, Box<C>, C>,
{
    type BlobTable = C::BlobTable;

    async fn new(database_url: &str) -> Result<Self> {
        Ok(Box::new(C::new(database_url).await?))
    }

    fn get_database(&self) -> &str {
        self.as_ref().get_database()
    }

    fn get_database_url(&self) -> &str {
        self.as_ref().get_database_url()
    }

    fn get_inner_client(&self) -> &C {
        self.as_ref()
    }

    fn blob_table(&'a self) -> Self::BlobTable {
        self.as_ref().blob_table()
    }
}
