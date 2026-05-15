use scylla::{
    cluster::metadata::ColumnType,
    deserialize::{
        FrameSlice,
        value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind},
    },
    errors::SerializationError,
};
use thiserror::Error;

// 10 MB is the recommended disk size per partition.
// We use 8MB due to Cassandra overhead
pub static MAX_PARTITION_SIZE: usize = 8_000_000; // 8MB

#[derive(Debug, Error)]
pub enum Error {
    #[error("CQL value is null")]
    CqlValueNull,
}

pub fn ensure_not_null_frame_slice<'frame, T>(
    _typ: &ColumnType,
    v: Option<FrameSlice<'frame>>,
) -> Result<FrameSlice<'frame>, scylla::errors::DeserializationError> {
    v.ok_or_else(|| scylla::errors::DeserializationError::new(Error::CqlValueNull))
}

pub fn ensure_not_null_slice<'frame, T>(
    typ: &scylla::cluster::metadata::ColumnType,
    v: Option<FrameSlice<'frame>>,
) -> Result<&'frame [u8], scylla::errors::DeserializationError> {
    ensure_not_null_frame_slice::<T>(typ, v).map(|frame_slice| frame_slice.as_slice())
}

pub fn mk_typck_err<T: ?Sized>(
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    mk_typck_err_named(std::any::type_name::<T>(), got, kind)
}

pub fn mk_typck_err_named(
    name: &'static str,
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: name,
        cql_type: got.clone().into_owned(),
        kind: kind.into(),
    })
}
