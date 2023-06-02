use scylla::frame::response::result::CqlValue;

pub fn get_cql_value(columns: &Vec<Option<CqlValue>>, index: usize) -> CqlValue {
    columns.get(index).unwrap().to_owned().unwrap()
}
