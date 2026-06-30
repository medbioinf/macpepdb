use std::sync::LazyLock;

use http::{HeaderMap, HeaderValue, header};

pub mod configuration_controller;
pub mod error_controller;
pub mod headers;
pub mod middleware;
pub mod peptide_controller;
pub mod protein_controller;
pub mod server;
pub mod server_state;
pub mod status_controller;
pub mod taxonomy_controller;

pub static DEFAULT_ERROR_HEADER_MAP: LazyLock<HeaderMap> = LazyLock::new(|| {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers
});
