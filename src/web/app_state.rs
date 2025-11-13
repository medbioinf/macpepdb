// std imports
use std::sync::Arc;

// 3rd party imports
use dihardts_omicstools::biology::taxonomy::TaxonomyTree;
use indicium::simple::SearchIndex;

// internal imports
use crate::{database::scylla::client::Client, entities::configuration::Configuration};

/// Information required for Matomo tracking
///
pub struct MatomoInfo {
    url: String,
    site_id: u32,
}

impl MatomoInfo {
    pub fn new(url: String, site_id: u32) -> Self {
        Self { url, site_id }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn site_id(&self) -> u32 {
        self.site_id
    }
}

pub struct AppState {
    db_client: Arc<Client>,
    configuration: Arc<Configuration>,
    taxonomy_tree: Arc<TaxonomyTree>,
    taxonomy_index: Arc<Option<SearchIndex<u64>>>,
    /// Number of concurrent search threads (and connections)
    num_search_threads: usize,
    /// Matomo information
    matomo_info: Option<MatomoInfo>,
}

impl AppState {
    pub fn new(
        db_client: Client,
        configuration: Configuration,
        taxonomy_tree: TaxonomyTree,
        taxonomy_index: Option<SearchIndex<u64>>,
        num_search_threads: usize,
        matomo_info: Option<MatomoInfo>,
    ) -> Self {
        Self {
            num_search_threads,
            matomo_info,
            db_client: Arc::new(db_client),
            configuration: Arc::new(configuration),
            taxonomy_tree: Arc::new(taxonomy_tree),
            taxonomy_index: Arc::new(taxonomy_index),
        }
    }

    /// Returns a new ARC of the db client
    ///
    pub fn get_db_client(&self) -> Arc<Client> {
        self.db_client.clone()
    }

    /// Returns a reference to the db client
    ///
    pub fn get_db_client_as_ref(&self) -> &Client {
        self.db_client.as_ref()
    }

    /// Returns a new ARC of the configuration
    ///
    pub fn get_configuration(&self) -> Arc<Configuration> {
        self.configuration.clone()
    }

    /// Returns a reference to the configuration
    ///
    pub fn get_configuration_as_ref(&self) -> &Configuration {
        self.configuration.as_ref()
    }

    /// Returns a new ARC of the taxonomy tree
    ///
    pub fn get_taxonomy_tree(&self) -> Arc<TaxonomyTree> {
        self.taxonomy_tree.clone()
    }

    /// Returns a reference to the taxonomy tree
    ///
    pub fn get_taxonomy_tree_as_ref(&self) -> &TaxonomyTree {
        self.taxonomy_tree.as_ref()
    }

    /// Returns a reference to the taxonomy tree
    ///
    pub fn get_taxonomy_index(&self) -> Arc<Option<SearchIndex<u64>>> {
        self.taxonomy_index.clone()
    }

    /// Returns a reference to the taxonomy tree
    ///
    pub fn get_taxonomy_index_as_ref(&self) -> &Option<SearchIndex<u64>> {
        self.taxonomy_index.as_ref()
    }

    /// Returns the number of search threads
    ///
    pub fn get_num_search_threads(&self) -> usize {
        self.num_search_threads
    }

    /// Returns the matomo information
    ///
    pub fn get_matomo_info(&self) -> Option<&MatomoInfo> {
        self.matomo_info.as_ref()
    }
}
