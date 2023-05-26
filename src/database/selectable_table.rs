// 3rd party imports
use anyhow::{Error, Result};
use fallible_iterator::FallibleIterator;

// internal imports
use crate::database::table::Table;

/// Iterator over a stream of records
///
/// Generic parameters:
/// * `R` - The row type
/// * `E` - The entity type
/// * `I` - Row iterator type
///
pub struct EntityStream<R, I, E>
where
    R: Sized,
    I: FallibleIterator<Item = R>,
    E: From<R>,
{
    inner_iter: I,
    record_type: std::marker::PhantomData<E>,
}

/// Fallible iterator implementation for Record Stream
///
/// Generic parameters:
/// * `R` - The row type
/// * `E` - The entity type
/// * `I` - Row iterator type
///
impl<R, I, E> FallibleIterator for EntityStream<R, I, E>
where
    I: FallibleIterator<Item = R, Error = Error>,
    E: From<R>,
{
    type Item = E;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>> {
        while let Some(record) = self.inner_iter.next()? {
            return Ok(Some(E::from(record)));
        }
        return Ok(None);
    }
}

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
    Self: Sized,
{
    type Parameter: ?Sized;
    type Record: Sized;
    type RecordIter: FallibleIterator<Item = Self::Record>;
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
    fn raw_select_multiple<'b>(
        client: &mut C,
        cols: &str,
        additional: &str,
        params: &[&Self::Parameter],
    ) -> Result<Vec<Self::Record>>;

    /// Selects a protein and returns it as row. If no protein is found, None is returned.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    fn raw_select<'b>(
        client: &mut C,
        cols: &str,
        additional: &str,
        params: &[&Self::Parameter],
    ) -> Result<Option<Self::Record>>;

    /// Selects proteins and returns them.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    fn select_multiple<'b>(
        client: &mut C,
        additional: &str,
        params: &[&Self::Parameter],
    ) -> Result<Vec<Self::Entity>>;

    /// Selects a protein and returns it. If no protein is found, None is returned.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    fn select<'b>(
        client: &mut C,
        additional: &str,
        params: &[&Self::Parameter],
    ) -> Result<Option<Self::Entity>>;

    /// Selects proteins and returns it them row iterator.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    fn raw_stream<'b>(
        client: &'a mut C,
        cols: &str,
        additional: &str,
        params: &[&Self::Parameter],
    ) -> Result<Self::RecordIter>;

    /// Selects proteins and returns them as iterator.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    fn stream<'b>(
        client: &'a mut C,
        additional: &str,
        params: &[&Self::Parameter],
    ) -> Result<EntityStream<Self::Record, Self::RecordIter, Self::Entity>> {
        let iter = Self::raw_stream(client, Self::select_cols(), additional, params)?;
        return Ok(EntityStream {
            inner_iter: iter,
            record_type: std::marker::PhantomData::<Self::Entity>,
        });
    }
}
