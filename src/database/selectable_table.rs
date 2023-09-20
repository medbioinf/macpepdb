use std::fmt::Display;
// std imports
use std::result::Result as StdResult;

// 3rd party imports
use anyhow::Result;
use futures::Stream;

// internal imports
use crate::database::table::Table;

/// Defines select operations for a database table, like selecting one or multiple a records and streaming record.
///
///
/// Generic parameters:
/// * `C` - The database client type
/// * `P` - The query parameter type
/// * `R` - The row type
/// * `E` - The entity type
/// * `I` - Row iterator type
///
pub trait SelectableTable<'a, C>: Table
where
    Self: Sized + 'a,
    C: Send + Sync + Unpin,
{
    type Parameter: ?Sized + Sync;
    type Record: Sized + Sync + Send + Unpin;
    type RecordIterErr: Send + Sync + Display;
    type RecordIter: Stream<Item = StdResult<Self::Record, Self::RecordIterErr>>;
    type Entity: From<Self::Record>;

    /// Returns select columns
    ///
    fn select_cols() -> &'static str;

    /// Selects proteins and returns them as rows.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    async fn raw_select_multiple<'b>(
        client: &C,
        cols: &str,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Vec<Self::Record>>;

    /// Selects a protein and returns it as row. If no protein is found, None is returned.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    async fn raw_select<'b>(
        client: &C,
        cols: &str,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Option<Self::Record>>;

    /// Selects proteins and returns them.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    async fn select_multiple<'b>(
        client: &C,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Vec<Self::Entity>>;

    /// Selects a protein and returns it. If no protein is found, None is returned.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    async fn select<'b>(
        client: &C,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Option<Self::Entity>>;

    /// Selects proteins and returns it them row iterator.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// * `num_rows` - The number of rows to fetch at once
    ///
    async fn raw_stream(
        client: &'a mut C,
        cols: &str,
        additional: &str,
        params: &'a [&'a Self::Parameter],
        num_rows: i32,
    ) -> Result<impl Stream<Item = Result<Self::Record>>>;

    /// Selects entities and returns them as iterator.
    ///
    /// TODO: Actually this is can be implemented right here by creating a raw stream
    /// using async_stream to iterate over it and just calling `Self::Entity::from` on each row.
    /// Unfortunately the compiler complains about a lifetime which is indirectly captured by AsyncStream.
    /// If everything becomes more stable we should come back to it and fix this.
    /// For now it's implemented in the individual structs.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// * `num_rows` - The number of rows to fetch at once
    ///
    async fn stream(
        client: &'a mut C,
        additional: &str,
        params: &'a [&'a Self::Parameter],
        num_rows: i32,
    ) -> Result<impl Stream<Item = Result<Self::Entity>>>;
}
