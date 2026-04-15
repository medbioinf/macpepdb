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
}
