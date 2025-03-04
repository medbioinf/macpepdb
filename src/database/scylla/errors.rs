use std::{error::Error, fmt::Display};

use scylla::frame::response::result::ColumnType;

#[derive(Debug)]
pub struct UnknownColumnError {
    pub name: String,
    pub typ: ColumnType<'static>,
}

impl Error for UnknownColumnError {}

impl Display for UnknownColumnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Unknown column: {} of type {:?}", self.name, self.typ)
    }
}

#[derive(Debug)]
pub struct ColumnTypeMismatchError {
    pub name: String,
    pub expected: ColumnType<'static>,
    pub actual: ColumnType<'static>,
}

impl Error for ColumnTypeMismatchError {}

impl Display for ColumnTypeMismatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Column type mismatch: {} expected {:?}, got {:?}",
            self.name, self.expected, self.actual
        )
    }
}

#[derive(Debug)]
pub struct UnexpectedEndOfRowError;

impl Error for UnexpectedEndOfRowError {}

impl Display for UnexpectedEndOfRowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "End of row")
    }
}
