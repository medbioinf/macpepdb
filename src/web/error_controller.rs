// 3rd party imports
use axum::http::StatusCode;

// internal imports
use crate::web::web_error::WebError;

pub async fn page_not_found() -> WebError {
    WebError::new(StatusCode::NOT_FOUND, "Page not found.".to_string())
}
