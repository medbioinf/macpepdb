use serde::{Deserialize, Serialize};

use crate::{blob::IsBlob, mass_partitioning::MassPartitioning, protease::Protease};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Configuration {
    mass_partitioning: MassPartitioning,
    protease: Protease,
}

impl Configuration {
    pub const BLOB_KEY: &str = "configuration";

    pub fn new(mass_partitioning: MassPartitioning, protease: Protease) -> Self {
        Self {
            mass_partitioning,
            protease,
        }
    }

    pub fn mass_partitioning(&self) -> &MassPartitioning {
        &self.mass_partitioning
    }

    pub fn protease(&self) -> &Protease {
        &self.protease
    }
}

impl IsBlob for Configuration {
    fn key(&self) -> &str {
        Self::BLOB_KEY
    }
}
