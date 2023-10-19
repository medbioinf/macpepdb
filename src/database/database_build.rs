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
    fn new(database_url: String, database: String) -> Self;

    /// Builds / Maintains the database.
    /// 1. Inserts / updates the proteins and peptides from the files
    /// 2. TODO: Collects and updates peptide metadata like taxonomies, proteomes and review status
    /// 3. TODO: Inserts / updates taxonomy tree
    ///
    /// Will panic if database contains not configuration and not initial configuration is provided.
    ///
    /// # Arguments
    /// * `protein_file_paths` - Paths to the protein files.
    /// * `num_threads` - Number of threads to use.
    /// * `num_partitions` - Number of partitions to use.
    /// * `allowed_ram_usage` - Allowed RAM usage in GB for the partitioner Bloom filter.
    /// * `partitioner_false_positive_probability` - False positive probability of the partitioners Bloom filters.
    /// * `initial_configuration_opt` - Optional initial configuration.
    ///
    async fn build(
        &self,
        protein_file_paths: &Vec<PathBuf>,
        num_threads: usize,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        initial_configuration_opt: Option<Configuration>,
        log_folder: &PathBuf,
        is_test_run: bool,
        only_metadata_update: bool,
    ) -> Result<()>;
}
