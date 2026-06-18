use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    blob_table::{BlobTable, IsBlob},
    client::Client,
    configuration::RuntimeConfiguration,
    database_build::MassPartitionMap,
    protease::Protease,
};

/// Information necessary to make query the database
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OldRuntimeConfiguration {
    mass_partitioning: HashMap<i64, Vec<i64>>,
    protease: Protease,
}

impl OldRuntimeConfiguration {
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

    pub fn into_pieces(self) -> (Protease, HashMap<i64, Vec<i64>>) {
        (self.protease, self.mass_partitioning)
    }
}

impl IsBlob for OldRuntimeConfiguration {
    fn key(&self) -> &str {
        Self::BLOB_KEY
    }
}

pub async fn convert_old_runtime_config_to_new(database_url: &str) {
    let client = Arc::new(Client::new(database_url).await.unwrap());

    let (protease, mass_partitioning) = BlobTable::select::<OldRuntimeConfiguration>(
        client.as_ref(),
        OldRuntimeConfiguration::BLOB_KEY,
    )
    .await
    .unwrap()
    .unwrap()
    .into_pieces();

    let mass_partition_map = MassPartitionMap::from(mass_partitioning);

    let config = RuntimeConfiguration::new(mass_partition_map, protease);

    BlobTable::insert(client.as_ref(), &config, NonZeroUsize::new(100).unwrap())
        .await
        .unwrap();
}
