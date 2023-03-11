
// 3rd party imports
use anyhow::Result;
use fallible_iterator::FallibleIterator;
use postgres::{Client, Row, RowIter, ToStatement};
use postgres::types::{BorrowToSql, ToSql};

/// Iterator over a stream of records
/// 
pub struct RecordStream<'a, R: From<Row>>{
    inner_iter: RowIter<'a>,
    record_type: std::marker::PhantomData<R>,
}

/// Fallible iterator implementation for Record Stream
/// 
impl<R> FallibleIterator for RecordStream<'_, R> where R: From<Row> {
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
pub trait Table<'a, R: From<Row>> {
    /// Creates a new instance of the table
    fn new(client: &'a mut Client) -> Self;

    /// Return client
    /// 
    fn get_client(&mut self) -> &mut Client;

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
    fn raw_select_multiple(&mut self, cols: &str, additional: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        return Ok(self.get_client().query(&statement, params)?);
    }

    /// Selects a protein and returns it as row. If no protein is found, None is returned.
    /// 
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// 
    fn raw_select(&mut self, cols: &str, additional: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>> {
        let mut statement = format!("SELECT {} FROM {}", cols, Self::select_cols());
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        return Ok(self.get_client().query_opt(&statement, params)?);
    }

    /// Selects proteins and returns them.
    /// 
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// 
    fn select_multiple(&mut self, additional: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<R>> {
        let rows = self.raw_select_multiple(Self::select_cols(), additional, params)?;
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
    fn select(&mut self, additional: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<R>> {
        let row = self.raw_select(Self::select_cols(), additional, params)?;
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
    fn raw_stream<T, P, I>(&mut self, cols: &str, additional: &str, params: I) -> Result<RowIter<'_>>
        where T: ?Sized + ToStatement, P: BorrowToSql, I : IntoIterator<Item = P>, I::IntoIter: ExactSizeIterator {
        let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        return Ok(self.get_client().query_raw(&statement, params)?);
    }

    /// Selects proteins and returns them as iterator.
    /// 
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// 
    fn stream<T, P, I>(&mut self, additional: &str, params: I) -> Result<RecordStream<'_, R>>
        where T: ?Sized + ToStatement, P: BorrowToSql, I : IntoIterator<Item = P>, I::IntoIter: ExactSizeIterator {
        return Ok(RecordStream {
            inner_iter: self.raw_stream::<T, P, I>(Self::select_cols(), additional, params)?,
            record_type: std::marker::PhantomData::<R>,
        });
    }
}