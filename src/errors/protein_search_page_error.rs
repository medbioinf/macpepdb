use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum ProteinSearchPageError {
    #[error("Search term too short, must be at least {0} characters")]
    SearchTermTooShort(usize),
}
