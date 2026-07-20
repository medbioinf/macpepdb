// std imports
use std::{num::NonZeroUsize, sync::Arc};

// internal imports
use crate::{
    client::Client, configuration::RuntimeConfiguration, peptide_search::PeptideSearchType,
};

/// Matomo tracking endpoint configuration, used by the tracking middleware
/// to report requests unless the client sent `X-Do-Not-Track`.
pub struct MatomoInfo {
    url: String,
    site_id: u32,
}

impl MatomoInfo {
    /// Creates a new `MatomoInfo`.
    ///
    /// # Arguments
    /// * `url` - The Matomo instance URL to report requests to.
    /// * `site_id` - The Matomo site ID to report requests under.
    pub fn new(url: String, site_id: u32) -> Self {
        Self { url, site_id }
    }

    /// Returns the Matomo instance URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the Matomo site ID.
    pub fn site_id(&self) -> u32 {
        self.site_id
    }
}

/// Shared state handed to every axum route handler: the database client,
/// the build-time configuration, and the search/tracking settings the
/// server was started with.
pub struct ServerState {
    db_client: Arc<Client>,
    configuration: Arc<RuntimeConfiguration>,
    matomo_info: Option<MatomoInfo>,
    concurrent_searches: NonZeroUsize,
    search_type: PeptideSearchType,
}

impl ServerState {
    /// Creates a new `ServerState`.
    ///
    /// # Arguments
    /// * `db_client` - Database client the handlers will query through.
    /// * `configuration` - Build-time configuration (protease, mass partitioning).
    /// * `matomo_info` - Optional Matomo tracking endpoint to report requests to.
    /// * `concurrent_searches` - Number of concurrent searches allowed.
    /// * `search_type` - Whether search results are ProForma or canonical-only.
    pub fn new(
        db_client: Client,
        configuration: RuntimeConfiguration,
        matomo_info: Option<MatomoInfo>,
        concurrent_searches: NonZeroUsize,
        search_type: PeptideSearchType,
    ) -> Self {
        Self {
            concurrent_searches,
            search_type,
            matomo_info,
            db_client: Arc::new(db_client),
            configuration: Arc::new(configuration),
        }
    }

    /// Returns a new Arc of the db client
    ///
    pub fn db_client(&self) -> Arc<Client> {
        self.db_client.clone()
    }

    /// Returns a reference to the db client
    ///
    pub fn db_client_as_ref(&self) -> &Client {
        self.db_client.as_ref()
    }

    /// Returns a new ARC of the configuration
    ///
    pub fn configuration(&self) -> Arc<RuntimeConfiguration> {
        self.configuration.clone()
    }

    /// Returns a reference to the configuration
    ///
    pub fn configuration_as_ref(&self) -> &RuntimeConfiguration {
        self.configuration.as_ref()
    }

    /// Returns the number of concurrent searches allowed
    ///
    pub fn concurrent_searches(&self) -> NonZeroUsize {
        self.concurrent_searches
    }

    /// Returns a reference to the matomo info if it exists
    ///
    pub fn matomo_info(&self) -> Option<&MatomoInfo> {
        self.matomo_info.as_ref()
    }

    /// Returns whether search results are returned as ProForma or canonical-only.
    pub fn search_type(&self) -> PeptideSearchType {
        self.search_type
    }
}
