use axum::{http::StatusCode, response::IntoResponse};

pub async fn page_not_found() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Page not found.".to_string())
}
