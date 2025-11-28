use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{connect_info::ConnectInfo, MatchedPath, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
};
use tracing::error;
use urlencoding::encode as url_encode;
use xxhash_rust::xxh3::xxh3_64;

use crate::web::app_state::AppState;

/// Sends the tracking request to the Matomo server
/// IP and user agent are used to create a unique visitor id by hashing them together
///
/// Arguments:
/// * `state` - Application state
/// * `ip` - Client IP address
/// * `base_url` - Base URL of the request (protocol + host)
/// * `path` - Path of the request
/// * `user_agent` - User agent of the client
///
async fn tracking_task(
    state: Arc<AppState>,
    ip: String,
    base_url: String,
    path: String,
    user_agent: String,
) {
    let matomo_info = match state.get_matomo_info() {
        Some(info) => info,
        None => return,
    };

    let id = format!("{ip}{user_agent}");

    let tracing_id = format!("{:0>16X}", xxh3_64(id.as_bytes()));

    let url = format!("{base_url}{path}");

    let params = [
        // required parameters
        format!("idsite={}", matomo_info.site_id()),
        "rec=1".to_string(),
        // recommended parameters
        "apiv=1".to_string(),
        format!(
            "rand={}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        ),
        "_id=".to_string() + tracing_id.as_str(),
        format!("url={}", url_encode(&url)),
        format!("action_name={}", url_encode(&path)),
    ]
    .join("&");

    let tracking_url = format!("{}?{}", matomo_info.url(), params);

    let client = reqwest::Client::new();

    let response = match client.post(&tracking_url).send().await {
        Ok(response) => response,
        Err(e) => {
            error!("Failed to track page visit @ {}: {}", &tracking_url, e);
            return;
        }
    };

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        error!(
            "Failed to track page visit @ {}: status {}, body {}",
            &tracking_url, status, body
        );
    }
}

/// Matomo based tracking middleware
/// Tracking is only performed if DNT / X-Do-Not-Track headers are not set to 1
///
/// * Ip is extracted from X-Forwarded-For header if present, otherwise from client address
/// * Protocol is extracted from X-Forwarded-Proto header if present, otherwise defaults to http
/// * Host is extracted from X-Forwarded-Host header if present, otherwise from Host
/// * Path is extracted from MatchedPath extension
/// * User agent is extracted from User-Agent header if present, otherwise set to unknown
///
/// IP is used together with user agent to create a unique visitor id by hashing them together
///
/// Arguments:
/// * `State(state)` - Application state
/// * `ConnectInfo(client_addr)` - Client address
/// * `headers` - Request headers
/// * `request` - Incoming request
/// * `next` - Next middleware / handler
///
pub async fn track_middleware(
    State(state): State<Arc<AppState>>,
    ConnectInfo(client_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let do_not_track = headers
        .get("DNT")
        .or_else(|| headers.get("X-Do-Not-Track"))
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "1")
        .unwrap_or(false);

    if !do_not_track {
        let ip = match headers.get("X-Forwarded-For") {
            Some(v) => v.to_str().unwrap_or("").to_string(),
            None => client_addr.ip().to_string(),
        };

        let protocol = headers
            .get("X-Forwarded-Proto")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("http")
            .to_string();

        let host = headers
            .get("X-Forwarded-Host")
            .or_else(|| headers.get("Host"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        let path = request
            .extensions()
            .get::<MatchedPath>()
            .map(|mp| mp.as_str().to_string())
            .unwrap_or_else(|| "/could-not-extract-matched-path".to_string());

        let user_agent = headers
            .get("User-Agent")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        let task_state = state.clone();
        tokio::spawn(async move {
            tracking_task(
                task_state,
                ip,
                format!("{protocol}://{host}"),
                path,
                user_agent,
            )
            .await;
        });
    }

    let response = next.run(request).await;
    Ok(response)
}
