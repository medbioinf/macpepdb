use serde::Deserialize;

/// TOML formatted configuration file copied/selected via build.rs
/// Once compiled the application is deployed into a browser where without any access to the file system   
///
const COMPILED_CONFIG: &str = include_str!(concat!(env!("OUT_DIR"), "/config.toml"));

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
}

impl Configuration {
    /// Create a new instance of the configuration form the compiled TOML file
    ///
    pub fn new() -> Self {
        toml::from_str::<Configuration>(COMPILED_CONFIG).unwrap()
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
}
