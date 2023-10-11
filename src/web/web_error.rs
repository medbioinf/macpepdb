// 3rd party imports
use anyhow;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

///  General error type
///
pub struct WebError {
    status_code: StatusCode,
    msg: String,
}

impl WebError {
    /// Create new WebError
    ///
    /// # Arguments
    /// * `status_code` - HTTP status code
    ///
    pub fn new(status_code: StatusCode, msg: String) -> Self {
        Self { status_code, msg }
    }
}

/// Tell axum how to convert `WebError` into a response.
///
impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        (self.status_code, self.msg).into_response()
    }
}

/// Create WebError from anyhow::Error
///
impl From<anyhow::Error> for WebError {
    fn from(err: anyhow::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", err))
    }
}
