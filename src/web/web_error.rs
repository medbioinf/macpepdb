// std imports
use std::fmt;

// 3rd party imports
use async_stream::stream;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures::Stream;

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

    /// Creates a new `WebError` as stream
    /// TODO: StatusCode is not deserializable by serde, so we directly convert to string.
    ///
    /// # Arguments
    /// * `status_code` - HTTP status code
    /// * `msg` - error message
    ///
    pub fn new_string_stream(status_code: StatusCode, msg: String) -> impl Stream<Item = String> {
        stream! {
            yield format!("{}", Self::new(status_code, msg));
        }
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
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", err))
    }
}

impl fmt::Display for WebError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}", self.status_code, self.msg)
    }
}
