use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{blob::IsBlob, protease::Protease};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Configuration {
    mass_partitioning: HashMap<i64, Vec<i64>>,
    protease: Protease,
}

impl Configuration {
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

impl IsBlob for Configuration {
    fn key(&self) -> &str {
        Self::BLOB_KEY
    }
}
