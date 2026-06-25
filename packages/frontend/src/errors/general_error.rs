use thiserror::Error;

use crate::errors::{
    api_client_error::ApiClientError, peptide_search_page_error::PeptideSearchPageError,
    protein_search_page_error::ProteinSearchPageError,
};

#[derive(Debug, Error)]
pub enum GeneralError {
    #[error("Configuration not loaded yet")]
    ConfigurationNotLoaded,
    #[error("{0}")]
    ProteinSearchPageError(#[from] ProteinSearchPageError),
    #[error("{0}")]
    PeptideSearchPageError(#[from] PeptideSearchPageError),
    #[error("{0}")]
    ApiError(#[from] ApiClientError),
}
