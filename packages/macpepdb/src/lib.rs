//! # macpepdb
//!
//! Builds and serves a mass-indexed peptide database: digests UniProt protein files with a
//! protease, stores the resulting peptides in PostgreSQL/Citus, and answers mass-based peptide
//! searches (including variable post-translational modifications) over a CLI and an HTTP API.
//!
//! ## Data model
//!
//! Masses are stored as scaled integers ([`mass`]) rather than floats, and amino acid sequences
//! are bit-packed 5 bits per residue ([`sequence`]) to keep the in-memory mass index and DB blobs
//! small. Peptides are partitioned by mass; the partition layout is persisted as part of the
//! [`configuration::RuntimeConfiguration`] blob alongside the [`protease::Protease`] used to build the
//! database.
//!
//! ## Build pipeline
//!
//! [`database_build::DatabaseBuild`] runs three sequential, internally-concurrent stages:
//! proteins ([`protein_table`], [`protein`]) → a hybrid in-memory/disk mass index ([`mass_index`]) → peptides
//! ([`peptide_table`], [`peptide`]). [`taxonomy_table`]/[`taxonomy_rank_table`] load the NCBI
//! taxonomy tree used for search filtering.
//!
//! ## Search
//!
//! [`peptide_search`] expands a query mass plus a [`post_translational_modification::PTMCollection`]
//! into partition/mass conditions and streams matching [`peptide::Peptidoform`]s, filtered by
//! taxonomy/proteome/review status.
//!
//! ## Database & web layers
//!
//! [`client::Client`] wraps the `deadpool_postgres` connection pool used by every `*_table`
//! module. [`web`] exposes the same search and lookup functionality over an axum HTTP API.
//! [`monitoring`] wires `tracing`/`metrics` output to the TUI, terminal, log files, Loki, and
//! Prometheus.

extern crate static_assertions;

/// Compile-time-generated amino acid constants (mono/integer masses, bit codes); see `build.rs`.
pub mod amino_acid;
/// The persisted build configuration: protease, mass partitioning, and free-form comment.
pub mod configuration;
/// The retired v1 configuration layout, readable only so `config migrate` can convert old databases.
pub mod configuration_v1;
/// Integer mass conversion (`mass_to_int!`/`to_float`) and the `MASS_CONVERT_FACTOR` scale.
#[macro_use]
pub mod mass;
/// The `blobs` table: chunked binary storage backing [`configuration::RuntimeConfiguration`] and other blobs.
pub mod blob_table;
/// The pooled Postgres/Citus client, congestion control, and binary `COPY` support.
pub mod client;
/// The `into_thiserror_boxed!` macro used to box external errors into `thiserror` variants.
#[macro_use]
pub mod error;
/// The three-stage build pipeline: proteins → mass index → peptides.
pub mod database_build;
/// Experimental Koina (https://doi.org/10.1038/s41467-025-64870-5) client to add predicted peptide attributed like retention time to the PRM/SRM targets
pub mod koina;
/// Hybrid in-memory/disk `mass -> protein ids` index built by digesting every protein once.
pub mod mass_index;
/// Compile-time-generated molecule masses (e.g. `WATER_MONO_MASS`); see `build.rs`.
pub mod molecules;
/// `tracing`/`metrics` wiring: TUI, terminal, log file, Loki, Prometheus, tokio-console targets.
pub mod monitoring;
/// Peptide and peptidoform entities, and the peptide mass calculation.
pub mod peptide;
/// Mass + PTM search: expands a query into partition/mass conditions and streams matches.
pub mod peptide_search;
/// The `peptides` columnar table: build-time batch upsert and search-time reads.
pub mod peptide_table;
/// Simple client to search peptidoforms in MaCPepDB in ProForma complient format
pub mod peptidoform_search_client;
/// Test to rate the max peptide search performance your database (and web API) can handle
pub mod performance_test;
/// Post-translational modification definitions, parsing, and PTM-set expansion.
pub mod post_translational_modification;
/// Protease cleavage rules (e.g. trypsin, semi trypsin, unspecific) and missed-cleavage iteration.
pub mod protease;
/// Protein entity and its variants (isoforms/proteoforms sharing a base accession).
pub mod protein;
/// Compact ("variable integer delta encoding") encoding of a peptide's associated protein ids.
pub mod protein_ids;
/// The `proteins` table: build-time insert and read access for the digestion stage.
pub mod protein_table;
/// Various amino acid seqeunce encodings shared by peptides and proteins.
/// * Byte packed seqeunces encoding for common operations, e.g. cleavages
/// * Bit-packed (5 bits/residue) sequence encoding to safe memory.
pub mod sequence;
/// Partition -> Citus shard resolution, letting the search batch conditions that share a
/// shard into one statement.
pub mod shard_map;
/// The `stats` table: build/search counters and sizes persisted across runs.
pub mod stats_table;
/// Taxonomy entity (NCBI taxon: id, parent, scientific name, rank).
pub mod taxonomy;
/// Taxonomy rank entity (e.g. species, genus) referenced by [`taxonomy::Taxonomy`].
pub mod taxonomy_rank;
/// The `taxonomy_ranks` table.
pub mod taxonomy_rank_table;
/// The `taxonomies` table and taxonomy-tree queries used by search filtering.
pub mod taxonomy_table;
/// Small integer-encoding conversion helpers used to store unsigned values in Postgres.
pub mod tools;
/// The axum HTTP API: peptide/protein/taxonomy/configuration routes and server state.
pub mod web;

// Assert usize is u64. This is should prevent compiling on 32-bit platforms, which are not supported.
static_assertions::assert_eq_size!(usize, u64);
