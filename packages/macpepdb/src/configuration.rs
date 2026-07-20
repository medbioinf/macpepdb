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
    pub const BLOB_KEY: &str = "configuration";

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

    pub fn mass_partitioning(&self) -> &MassPartitionMap {
        &self.mass_partitioning
    }

    pub fn protease(&self) -> &Protease {
        &self.protease
    }

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
