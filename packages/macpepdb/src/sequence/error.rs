use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    AminoAcid(#[from] crate::amino_acid::Error),
    #[error(
        "Intermediate bit vector from &[u8] to ByteArraySeqeunce needs to be at least 6 bit long to store the length, but is only {0} bits long"
    )]
    InvalidByteArrayByteVectorRepresentation(usize),
    #[error(
        "Intermediate bit vector from &[u8] to ByteArraySeqeunce should be {0} bits long after removing the length, but is {1}"
    )]
    InvalidByteArraySequenceLength(usize, usize),

    #[error("{0}")]
    Bytes(deku::error::DekuError),

    #[error("Byte sequence too large for blob")]
    ByteSequenceTooLargeForCqlBlob,
    #[error("Expected {0:?} got {1:?}")]
    UnexpectedCqlValueType(
        scylla::cluster::metadata::ColumnType<'static>,
        scylla::cluster::metadata::ColumnType<'static>,
    ),
    #[error("Column `{0}` is null")]
    CqlValueNone(&'static str),
}
