use std::sync::LazyLock;

use http::{HeaderMap, HeaderValue, header};

/// Route handlers for `/api/admin` (runtime DB client rebuild). Only compiled with the
/// `admin-api` feature — must never be enabled on an internet-facing build.
#[cfg(feature = "admin-api")]
pub mod admin_controller;
/// Route handlers for `/api/chemistry` (amino acid info, hydrophobicity).
pub mod chemistry_controller;
/// Route handlers for `/api/configuration`.
pub mod configuration_controller;
/// Fallback handler for unmatched routes.
pub mod error_controller;
/// Custom HTTP header names used by the web server.
pub mod headers;
/// Axum middleware, e.g. Matomo request tracking.
pub mod middleware;
/// Route handlers for `/api/peptides` (search, existence check, lookup by sequence).
pub mod peptide_controller;
/// Route handlers for `/api/proteins`.
pub mod protein_controller;
/// Builds the router and starts the axum web server.
pub mod server;
/// Shared state (`ServerState`) handed to every route handler.
pub mod server_state;
/// Route handler for the liveness/status endpoint.
pub mod status_controller;
/// Route handlers for `/api/taxonomies`.
pub mod taxonomy_controller;

/// Default response headers (plain-text content type) used for error responses.
pub static DEFAULT_ERROR_HEADER_MAP: LazyLock<HeaderMap> = LazyLock::new(|| {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers
});
