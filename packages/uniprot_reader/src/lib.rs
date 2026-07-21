//! # macpepdb_uniprot_reader
//!
//! Streaming parser and indexer for UniProt text dumps (`.txt`, `.txt.gz`), used by the
//! `macpepdb` crate's build pipeline to read protein records.
//!
//! ## Reading
//!
//! [`reader::Reader`] streams entries from a UniProt text file in order; [`indexer::Indexer`]
//! records each entry's byte offsets so [`reader::IndexedReader`] can later re-read an arbitrary
//! entry without scanning the file from the start. The `async` feature adds
//! [`asynchronous::reader::AsyncReader`], a tokio-based counterpart to [`reader::Reader`].
//!
//! ## Parsing
//!
//! [`reader::Item`] wraps one raw entry's lines; [`entry::Entry`] is the parsed result, exposing
//! one getter per UniProt two-letter line code (accession, description, organism, etc.).
//! [`feature_table::FeatureTable`] and [`comment::Isoform`] parse the `FT` (feature table) and
//! `CC` (free-text comment) line groups referenced from an [`entry::Entry`].

#[cfg(feature = "async")]
pub mod asynchronous;
/// Parsing of UniProt `CC` (free-text comment) lines, e.g. alternative-product isoforms.
pub mod comment;
/// The parsed UniProt entry ([`entry::Entry`]) and its per-line-code accessors.
pub mod entry;
/// Parsing of UniProt `FT` (feature table) lines into [`feature_table::FeatureTable`].
pub mod feature_table;
/// Builds a byte-offset index over a UniProt text file for random-access re-reads.
pub mod indexer;
/// Synchronous streaming reader over a UniProt text file, with an indexed random-access variant.
pub mod reader;
