use serde::{Deserialize, Serialize};

use crate::{blob_table::IsBlob, database_build::MassPartitionMap, protease::Protease};

/// Information necessary to make query the database
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeConfiguration {
    comment: Option<String>,
    mass_partitioning: MassPartitionMap,
    protease: Protease,
}

impl RuntimeConfiguration {
    /// Key the configuration is stored under in the `blobs` table.
    pub const BLOB_KEY: &str = "configuration";

    /// Creates a new configuration from the outcome of a build.
    pub fn new(
        comment: Option<String>,
        mass_partitioning: MassPartitionMap,
        protease: Protease,
    ) -> Self {
        Self {
            comment,
            mass_partitioning,
            protease,
        }
    }

    /// Returns the mass-to-partitions map to use for search and build.
    pub fn mass_partitioning(&self) -> &MassPartitionMap {
        &self.mass_partitioning
    }

    /// Returns the protease the database was digested with.
    pub fn protease(&self) -> &Protease {
        &self.protease
    }

    /// Returns the optional comment stored with the build.
    pub fn comment(&self) -> Option<&String> {
        self.comment.as_ref()
    }
}

impl IsBlob for RuntimeConfiguration {
    fn key(&self) -> &str {
        Self::BLOB_KEY
    }
}

impl From<&RuntimeConfiguration>
    for macpepdb_web_common::responses::configuration::RuntimeConfigurationResponse
{
    fn from(configuration: &RuntimeConfiguration) -> Self {
        Self {
            comment: configuration.comment.clone(),
            protease: (&configuration.protease).into(),
        }
    }
}
