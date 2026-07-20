use thiserror::Error;

#[derive(Debug, Error)]
pub enum PeptideSearchPageError {
    #[error("Peptide sequence is too short, min length is {0}")]
    PeptideTooShortError(usize),
}
