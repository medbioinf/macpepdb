# macpepdb_uniprot_reader

Streaming parser and indexer for [UniProt text dumps](https://web.expasy.org/docs/userman.html) (`.txt`, `.txt.gz`), used by the
[`macpepdb`](https://github.com/medbioinf/macpepdb) crate's build pipeline to read protein
records.
**Note:** The crate [uniprot](https://docs.rs/uniprot/latest/uniprot/) is probably more feature-complete, but it parses XML files, which are much larger than the text dumps.

## Reading

`reader::Reader` streams entries from a UniProt text file in order; `indexer::Indexer`
records each entry's byte offsets so `reader::IndexedReader` can later re-read an arbitrary
entry without scanning the file from the start. The `async` feature adds
`asynchronous::reader::AsyncReader`, a tokio-based counterpart to `reader::Reader`.

## Parsing

`reader::Item` wraps one raw entry's lines; `entry::Entry` is the parsed result, exposing one
getter per UniProt two-letter line code (accession, description, organism, etc.).
`feature_table::FeatureTable` and `comment::Isoform` parse the `FT` (feature table) and `CC`
(free-text comment) line groups referenced from an `entry::Entry`.
