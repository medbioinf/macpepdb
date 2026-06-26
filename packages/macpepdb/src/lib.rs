extern crate static_assertions;

pub mod amino_acid;
pub mod configuration;
#[macro_use]
pub mod mass;
pub mod blob_table;
pub mod client;
#[macro_use]
pub mod error;
pub mod database_build;
pub mod mass_index;
pub mod molecules;
pub mod monitoring;
pub mod peptide;
pub mod peptide_metadata_table;
pub mod peptide_search;
pub mod peptide_table;
pub mod post_translational_modification;
pub mod protease;
pub mod protein;
pub mod protein_ids;
pub mod protein_table;
pub mod sequence;
pub mod stats_table;
pub mod taxonomy;
pub mod taxonomy_rank;
pub mod taxonomy_rank_table;
pub mod taxonomy_table;
pub mod temp;
pub mod tools;
pub mod web;

// Assert usize is u64. This is should prevent compiling on 32-bit platforms, which are not supported.
static_assertions::assert_eq_size!(usize, u64);
