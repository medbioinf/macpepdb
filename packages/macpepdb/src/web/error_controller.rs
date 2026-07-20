use axum::{http::StatusCode, response::IntoResponse};

/// Fallback route handler for any request that matches no defined route.
/// Returns `404 Not Found` with a plain-text body.
pub async fn page_not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Page not found.".to_string())
}
