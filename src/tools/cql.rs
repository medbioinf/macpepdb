use scylla::value::CqlValue;

pub fn get_cql_value(columns: &[Option<CqlValue>], index: usize) -> Option<CqlValue> {
    columns.get(index).unwrap().to_owned()
}
