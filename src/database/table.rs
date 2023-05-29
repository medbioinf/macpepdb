pub trait Table {
    /// Returns the name of the table
    ///
    fn table_name() -> &'static str;
}
