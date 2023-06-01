use std::fmt::Display;
// std imports
use std::result::Result as StdResult;

// 3rd party imports
use anyhow::Result;
use futures::Stream;

// internal imports
use crate::database::table::Table;

////////
// TODO::
// Can't figure out how to save the actual row stream
// in a separate iterator to be able to convert the row
// to an entity while iterating over the stream.
//
/// Iterator over a stream of records
///
/// Generic parameters:
/// * `R` - The row type
/// * `E` - The entity type
/// * `I` - Row iterator type
///
// pub struct EntityStream<R, IE, I, E>
// where
//     R: Sized,
//     IE: Sync + Send,
//     I: Stream<Item = StdResult<R, IE>>,
//     E: From<R>,
// {
//     inner_iter: Pin<Box<I>>,
//     record_type: std::marker::PhantomData<E>,
// }
//
// ///
// /// Generic parameters:
// /// * `R` - The row type
// /// * `E` - The entity type
// /// * `I` - Row iterator type
// ///
// impl<R, IE, I, E> Stream for EntityStream<R, IE, I, E>
// where
//     IE: Sync + Send + Display,
//     I: Stream<Item = StdResult<R, IE>>,
//     E: From<R>,
// {
//     type Item = Result<E>;

//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         match pin!(&mut self.inner_iter.poll_next(cx)) {
//             Poll::Ready(Some(Ok(record))) => Poll::Ready(Some(Ok(E::from(record)))),
//             Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(anyhow!("{}", err)))),
//             Poll::Ready(None) => Poll::Ready(None),
//             Poll::Pending => Poll::Pending,
//         }

//         Poll::Pending
//     }
// }
////////

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
    C: Send + Sync,
{
    type Parameter: ?Sized + Sync;
    type Record: Sized;
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
    ///
    async fn raw_stream<'b>(
        client: &'a C,
        cols: &str,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Self::RecordIter>;

    ////////
    // TODO::
    // Will work if EntityStream is implemented.
    //
    // /// Selects proteins and returns them as iterator.
    // ///
    // /// # Arguments
    // /// * `cols` - The columns to select
    // /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    // /// * `params` - The parameters to use in the query
    // ///
    // async fn stream<'b>(
    //     client: &'a C,
    //     additional: &str,
    //     params: &[&'b Self::Parameter],
    // ) -> Result<EntityStream<Self::Record, Self::RecordIterErr, Self::RecordIter, Self::Entity>>
    // {
    //     let iter = Self::raw_stream(client, Self::select_cols(), additional, params).await?;
    //     return Ok(EntityStream {
    //         inner_iter: Box::pin(iter),
    //         record_type: std::marker::PhantomData::<Self::Entity>,
    //     });
    // }
    ////////
}
