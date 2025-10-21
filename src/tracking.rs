use chrono::{DateTime, Utc};
use dioxus::prelude::*;
use dioxus_logger::tracing;
use urlencoding::encode as url_encode;
use uuid::Uuid;
use web_sys::window;
use xxhash_rust::xxh3::xxh3_64;

use crate::{configuration::Configuration, routes::Routes};

/// Track page visit by sending data to Matomo analytics server
///
/// # Arguments
/// * `path_segment_overrides` - Vector of (segment to replace, replacement) tuples. Important to generalize the path segments which can have high cardinality like protein accessions or peptide sequences.
pub async fn track_page_visit(path_segment_overrides: Vec<(String, String)>) {
    let config = use_context::<Configuration>();

    if config.matomo_url().is_none() || config.matomo_site_id().is_none() {
        return;
    }

    let tracing_id = use_context::<TrackingId>();
    let route: Routes = use_route();
    let mut path = route.to_string();
    let mut url = window()
        .and_then(|w| w.location().href().ok())
        .unwrap_or("unretrievable".to_string());

    // Due to high cardinality of some path segments (like protein accessions or peptide sequences),
    // we need to replace them with generic placeholders for tracking purposes
    for (segment, replacement) in path_segment_overrides {
        path = path.replace(&segment, &replacement);
        url = url.replace(&segment, &replacement);
    }

    let params = [
        // required parameters
        format!("idsite={}", config.matomo_site_id().unwrap()),
        "rec=1".to_string(),
        // recommended parameters
        "apiv=1".to_string(),
        format!(
            "rand={}",
            (Utc::now() - DateTime::UNIX_EPOCH).num_milliseconds()
        ),
        "_id=".to_string() + tracing_id.as_str(),
        format!("url={}", url_encode(&url)),
        format!("action_name={}", url_encode(&path)),
    ]
    .join("&");

    let tracking_url = format!("{}?{}", config.matomo_url().unwrap(), params);

    match reqwest::get(&tracking_url).await {
        Ok(_) => {}
        Err(e) => tracing::error!("Failed to track page visit: {}", e),
    }
}

/// Tracking ID for the session
///
#[derive(Clone)]
pub struct TrackingId(String);

impl TrackingId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Creates a random hexadezimal tracking ID for this session
/// to sync between frontend and backend tracking
/// The ID does not contain any personally identifiable information
/// just a random UUID + timestamp hashed with xxh3
///
pub fn create_tracking_id() -> TrackingId {
    let uuid = Uuid::new_v4();
    let now = (Utc::now() - DateTime::UNIX_EPOCH).num_milliseconds();
    let user_agent = window()
        .and_then(|w| w.navigator().user_agent().ok())
        .unwrap_or("unknown".to_string());
    let id = format!("{uuid}{now}{user_agent}");
    TrackingId(format!("{:0>16X}", xxh3_64(id.as_bytes())))
}
