//! The retired v1 configuration layout, kept only so `config migrate` can read a database that was
//! built before [`crate::database_build::MassPartitionMap`] switched from one entry per mass to one
//! mass range per partition.
//!
//! `postcard` is positional and not self-describing, so the only way to decode an old blob is with
//! structs whose field order matches the ones that wrote it. **Do not edit these to match changes in
//! [`crate::configuration`]** — they describe bytes already on disk. Field order below mirrors the
//! `RuntimeConfiguration` / `MassPartitionMap` definitions as of the `configuration` blob key.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{blob_table::IsBlob, protease::Protease};

/// v1 of the mass-to-partition map: one entry per distinct peptide mass, split into a sorted array
/// for masses in exactly one partition and a map for masses that spilled across several.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MassPartitionMapV1 {
    /// `(mass, partition)` pairs, sorted by mass.
    single: Vec<(i64, i64)>,
    /// Masses held by more than one partition.
    overflow: BTreeMap<i64, Vec<i64>>,
}

impl MassPartitionMapV1 {
    /// Every `(mass, partition)` association in the map.
    ///
    /// This is the input to [`crate::database_build::MassPartitionMap::from_mass_partition_pairs`],
    /// which folds it into one range per partition.
    pub fn mass_partition_pairs(&self) -> impl Iterator<Item = (i64, i64)> + '_ {
        self.single.iter().copied().chain(
            self.overflow
                .iter()
                .flat_map(|(&mass, partitions)| partitions.iter().map(move |&p| (mass, p))),
        )
    }

    /// Number of distinct masses in the map — what the range form replaces with a partition count.
    pub fn mass_count(&self) -> usize {
        self.single.len() + self.overflow.len()
    }
}

/// v1 of the stored configuration. Mirrors [`crate::configuration::RuntimeConfiguration`]'s field
/// order at the time the `configuration` blob key was written.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RuntimeConfigurationV1 {
    comment: Option<String>,
    mass_partitioning: MassPartitionMapV1,
    protease: Protease,
}

impl RuntimeConfigurationV1 {
    /// Key the v1 configuration is stored under in the `blobs` table.
    pub const BLOB_KEY: &str = "configuration";

    pub fn comment(&self) -> Option<&String> {
        self.comment.as_ref()
    }

    pub fn mass_partitioning(&self) -> &MassPartitionMapV1 {
        &self.mass_partitioning
    }

    pub fn protease(&self) -> &Protease {
        &self.protease
    }
}

impl IsBlob for RuntimeConfigurationV1 {
    fn key(&self) -> &str {
        Self::BLOB_KEY
    }
}
