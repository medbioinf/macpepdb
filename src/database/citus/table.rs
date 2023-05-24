// 3rd party imports
use anyhow::Result;
use fallible_iterator::FallibleIterator;
use postgres::types::{BorrowToSql, ToSql};
use postgres::{GenericClient, Row, RowIter, ToStatement};

/// Iterator over a stream of records
///
pub struct RecordStream<'a, R: From<Row>> {
    inner_iter: RowIter<'a>,
    record_type: std::marker::PhantomData<R>,
}

/// Fallible iterator implementation for Record Stream
///
impl<R> FallibleIterator for RecordStream<'_, R>
where
    R: From<Row>,
{
    type Item = R;
    type Error = anyhow::Error;

    fn next(&mut self) -> Result<Option<Self::Item>> {
        if let Some(row) = self.inner_iter.next()? {
            return Ok(Some(R::from(row)));
        }
        return Ok(None);
    }
}

/// Defines read-only operations for a database table
///
pub trait Table<R: From<Row>> {
    /// Returns the name of the table
    ///
    fn table_name() -> &'static str;

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
    fn raw_select_multiple<'a, C: GenericClient>(
        client: &mut C,
        cols: &str,
        additional: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>> {
        let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        return Ok(client.query(&statement, params)?);
    }

    /// Selects a protein and returns it as row. If no protein is found, None is returned.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    fn raw_select<'a, C: GenericClient>(
        client: &mut C,
        cols: &str,
        additional: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>> {
        let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        return Ok(client.query_opt(&statement, params)?);
    }

    /// Selects proteins and returns them.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    fn select_multiple<'a, C: GenericClient>(
        client: &mut C,
        additional: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<R>> {
        let rows = Self::raw_select_multiple(client, Self::select_cols(), additional, params)?;
        let mut records = Vec::new();
        for row in rows {
            records.push(R::from(row));
        }
        return Ok(records);
    }

    /// Selects a protein and returns it. If no protein is found, None is returned.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    fn select<'a, C: GenericClient>(
        client: &mut C,
        additional: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<R>> {
        let row = Self::raw_select(client, Self::select_cols(), additional, params)?;
        if row.is_none() {
            return Ok(None);
        }
        return Ok(Some(R::from(row.unwrap())));
    }

    /// Selects proteins and returns it them row iterator.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    fn raw_stream<'a, C: GenericClient, T, P, I>(
        client: &'a mut C,
        cols: &str,
        additional: &str,
        params: I,
    ) -> Result<RowIter<'a>>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        return Ok(client.query_raw(&statement, params)?);
    }

    /// Selects proteins and returns them as iterator.
    ///
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    ///
    fn stream<'a, C: GenericClient, T, P, I>(
        client: &'a mut C,
        additional: &str,
        params: I,
    ) -> Result<RecordStream<'a, R>>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        return Ok(RecordStream {
            inner_iter: Self::raw_stream::<C, T, P, I>(
                client,
                Self::select_cols(),
                additional,
                params,
            )?,
            record_type: std::marker::PhantomData::<R>,
        });
    }
}
