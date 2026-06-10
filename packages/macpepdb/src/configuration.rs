use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{blob_table::IsBlob, protease::Protease};

/// Information necessary to make query the database
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeConfiguration {
    mass_partitioning: HashMap<i64, Vec<i64>>,
    protease: Protease,
}

impl RuntimeConfiguration {
    pub const BLOB_KEY: &str = "configuration";

    pub fn new(mass_partitioning: HashMap<i64, Vec<i64>>, protease: Protease) -> Self {
        Self {
            mass_partitioning,
            protease,
        }
    }

    pub fn mass_partitioning(&self) -> &HashMap<i64, Vec<i64>> {
        &self.mass_partitioning
    }

    pub fn protease(&self) -> &Protease {
        &self.protease
    }
}

impl IsBlob for RuntimeConfiguration {
    fn key(&self) -> &str {
        Self::BLOB_KEY
    }
}
