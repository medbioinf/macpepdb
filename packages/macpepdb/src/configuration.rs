use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

use crate::mass_partitioning::MassPartitioning;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Configuration {
    mass_partitioning: MassPartitioning,
    min_peptide_length: Option<NonZeroUsize>,
    max_peptide_length: Option<NonZeroUsize>,
}

impl Configuration {
    pub fn new(
        mass_partitioning: MassPartitioning,
        min_peptide_length: Option<NonZeroUsize>,
        max_peptide_length: Option<NonZeroUsize>,
    ) -> Self {
        Self {
            mass_partitioning,
            min_peptide_length,
            max_peptide_length,
        }
    }

    pub fn mass_partitioning(&self) -> &MassPartitioning {
        &self.mass_partitioning
    }

    pub fn min_peptide_length(&self) -> Option<NonZeroUsize> {
        self.min_peptide_length
    }

    pub fn max_peptide_length(&self) -> Option<NonZeroUsize> {
        self.max_peptide_length
    }
}
