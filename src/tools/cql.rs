use scylla::{
    deserialize::{row::RawColumn, DeserializationError, DeserializeValue},
    frame::response::result::CqlValue,
};

pub fn get_cql_value(columns: &[Option<CqlValue>], index: usize) -> Option<CqlValue> {
    columns.get(index).unwrap().to_owned()
}

pub fn convert_raw_col<'frame, 'metadata, T>(
    col: RawColumn<'frame, 'metadata>,
) -> Result<T, DeserializationError>
where
    T: DeserializeValue<'frame, 'metadata>,
{
    T::deserialize(col.spec.typ(), col.slice)
}
