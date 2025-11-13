use dioxus::prelude::*;
use serde::Deserialize;

/// Only ment fo overriding the MaCPepDB base URL at compile time for tesing purposes
static OVERRIDE_MACPEPDB_BASE_URL: Option<&str> = option_env!("MACPEPDB_BASE_URL");

/// Configuration for the frontend app
///
#[derive(PartialEq, Clone, Debug, Deserialize)]
pub struct Configuration {
    /// The base URL for the MaCPepDB backend, e.g. http://localhost:8080
    macpepdb_base_url: String,
    /// Matomo base URL, e.g. http://localhost:8080/matomo.php
    matomo_url: Option<String>,
    /// Matomo site ID
    matomo_site_id: Option<u64>,
    /// Controls with the de.NBI survey banner is shown
    #[serde(default)]
    show_denbi_survey_banner: bool,
}

impl Configuration {
    /// Creates the configuration
    /// 1. Trying to fetch it from `http(s)://<current_domain>/assets/config.toml`
    /// 2. Falling back to default configuration where the MaCPepDB base URL is `https://macpepdb.cubimed.rub.de` or option in development the value of the `MACPEPDB_BASE_URL` env var
    ///
    ///
    pub async fn new() -> Self {
        let domain = match document::eval("return window.location.host").await {
            Ok(d) => d.to_string().replace('"', ""),
            Err(err) => {
                error!("Could not retrieve domain from window.location.host: {err}",);
                "".to_string()
            }
        };

        let scheme = match document::eval("return window.location.protocol").await {
            Ok(d) => d.to_string().replace('"', ""),
            Err(err) => {
                error!("Could not retrieve domain from window.location.host: {err}",);
                "http:".to_string()
            }
        };

        let url = format!("{scheme}//{}/assets/config.toml", domain);

        let response = match reqwest::get(&url).await {
            Ok(response) => response,
            Err(_) => {
                error!(
                    "Could not fetch configuration from `{url}`, falling back to default configuration",
                );
                return Self::default();
            }
        };

        if !response.status().is_success() {
            error!(
                "Could not fetch configuration from `{url}` (status: {}), falling back to default configuration",
                response.status()
            );
            return Self::default();
        }

        let text = match response.text().await {
            Ok(text) => text,
            Err(_) => {
                error!(
                    "Could not decode fetched configuration from `{url}`, falling back to default configuration",
                );
                return Self::default();
            }
        };

        toml::from_str::<Configuration>(&text).unwrap_or_default()
    }

    /// Get the base URL for the MaCPepDB backend
    ///
    pub fn get_macpepdb_base_url(&self) -> &str {
        &self.macpepdb_base_url
    }

    /// Get the Matomo base URL
    ///
    pub fn matomo_url(&self) -> Option<&str> {
        self.matomo_url.as_deref()
    }

    /// Get the Matomo site ID
    ///
    pub fn matomo_site_id(&self) -> Option<u64> {
        self.matomo_site_id
    }

    /// Whether to show the de.NBI survey banner
    ///
    pub fn show_denbi_survey_banner(&self) -> bool {
        self.show_denbi_survey_banner
    }
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            macpepdb_base_url: OVERRIDE_MACPEPDB_BASE_URL
                .unwrap_or("https://macpepdb.cubimed.rub.de")
                .to_string(),
            matomo_url: None,
            matomo_site_id: None,
            show_denbi_survey_banner: true,
        }
    }
}
