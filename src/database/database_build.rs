// std imports
use std::path::PathBuf;

// 3rd party imports
use anyhow::Result;

// internal imports
use crate::entities::configuration::Configuration;

pub trait DatabaseBuild {
    /// Creates a new instance of the database builder for the given database
    ///
    /// # Arguments
    /// * `database_url` - URL of the database.
    /// * `database` - Name of the database. For some databases, e.g. ScyllaDB (keyspace), you cannot include it in the URL.
    ///
    fn new(database_url: &str) -> Self;

    /// Builds / Maintains the database.
    /// 1. Builds the deserializes the taxonomy tree and saves it to the database.
    /// 2. Inserts / updates the proteins and peptides from the files
    /// 3. Collects and updates peptide metadata like taxonomies, proteomes and review status
    ///
    /// Will panic if database contains not configuration and not initial configuration is provided.
    ///
    /// # Arguments
    /// * `protein_file_paths` - Paths to the protein files.
    /// * `taxonomy_file_path` - Path to the taxonomy file.
    /// * `num_threads` - Number of threads to use.
    /// * `num_partitions` - Number of partitions to use.
    /// * `allowed_ram_usage` - Allowed RAM usage in GB for the partitioner Bloom filter.
    /// * `partitioner_false_positive_probability` - False positive probability of the partitioners Bloom filters.
    /// * `initial_configuration_opt` - Optional initial configuration.
    /// * `log_folder` - Path to the log folder.
    /// * `include_domains` - Whether to include domain parsing.
    /// * `metrics_log_interval` - Intervals in seconds for logging metrics
    ///
    fn build(
        &self,
        protein_file_paths: &Vec<PathBuf>,
        taxonomy_file_path: &Option<PathBuf>,
        num_threads: usize,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        initial_configuration_opt: Option<Configuration>,
        log_folder: &PathBuf,
        include_domains: bool,
        metrics_log_interval: u64,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}
