use thiserror::Error;

use reqwest::StatusCode;

#[derive(Debug, Error)]
pub enum ApiClientError {
    #[error("Creating the http client failed: {0}")]
    ClientCreationError(reqwest::Error),

    #[error("Network error: {0}")]
    NetworkError(reqwest::Error),

    #[error("Failed to fetch data from API: {0} (status code: {1})")]
    UnsuccessfulResponse(String, StatusCode),

    #[error("JSON parsing error: {0}")]
    JsonParsingError(reqwest::Error),

    #[error("Unexpected response format")]
    UnexpectedResponseFormat,
}
