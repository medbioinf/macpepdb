# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

MaCPepDB Lite builds and serves a mass-indexed peptide database. It digests UniProt protein
files with a protease, stores the resulting peptides in ScyllaDB (Cassandra-compatible), and
answers mass-based peptide searches (including variable post-translational modifications) over
a CLI and an HTTP API.

## Workspace layout

Cargo workspace (Rust edition 2024) with four members under `packages/`:

- **`macpepdb`** — the main library + binary. All domain logic, the build pipeline, search, and the web API.
- **`uniprot_reader`** — streaming/async parser + indexer for UniProt text dumps (`.txt`, `.txt.gz`).
- **`tui`** (`macpepdb_tui`) — `ratatui` dashboard that subscribes to `tracing` events and `metrics` recorders; see [packages/tui/README.md](packages/tui/README.md).
- **`metrics-peek`** — a `metrics::Recorder` that forwards values to a callback (immediate or periodic).

## Common commands

```bash
cargo build              # debug build
cargo build -r           # release build (what the Dockerfile runs)
cargo test               # all unit tests (no live DB needed — they use test_data/ fixtures)
cargo test -p macpepdb test_parse_database_url   # a single test by name in one package
cargo clippy             # lint
cargo fmt                # format
```

Tests are plain unit tests colocated in `#[cfg(test)] mod tests` blocks; none require a running
database. Fixtures live in `test_data/` and are located via `CARGO_MANIFEST_DIR`.

### Running against a database

The default DB URL is `scylla://127.0.0.1:9042,127.0.0.1:9043/macpepdb`. Bring up a local
two-node cluster and initialize the schema before building:

```bash
docker compose up -d                              # 2-node Cassandra on ports 9042/9043
cqlsh -f db.cql                                   # creates keyspace + tables (NOTE: DROPs existing tables)
cargo run -r -- build packages/.../proteins.txt   # digest + load (see subcommands below)
cargo run -r -- api 127.0.0.1:8080                # serve the HTTP API
```

System dependency: `openssl` or `libressl` must be present.

### CLI subcommands (`packages/macpepdb/src/main.rs`)

- `build [files...]` — the three-stage pipeline (see below). Key flags: `--protease`, `--min-length`/`--max-length`, `--max-missed-cleavages`, `--proteins-memory-limit`, `--threads`, `--batch-size-limit`. File args accept glob patterns.
- `search <mass> <output_file>` — PTM-aware mass search; `--lower/--upper-mass-tolerance-ppm`, `--ptm-file-path`, `--max-variable-modifications`, taxonomy/proteome filters. Writes a FASTA-like file.
- `api [socket]` — start the axum web server.
- `config show` — print the stored `Configuration` blob as JSON.

Global flags (before the subcommand) control monitoring output: `--tui`, `--terminal`,
`--prometheus`, `--loki`, `--log-file`, `--console` (tokio-console), and `-v` (repeatable verbosity).

## Architecture

### The build pipeline (`database_build.rs`, `main.rs::build_db`)

Three sequential stages, each concurrent internally:

1. **Proteins** — `ProteinTable::build` streams protein files and inserts proteins. Afterwards it
   decides protein access strategy based on `--proteins-memory-limit` (fraction of free RAM): if the
   proteins fit, it loads them into `InMemoryProteinAccess`; otherwise reads them back from the DB via
   `DatabaseProteinAccess`. Both implement the `IsProteinAccess` trait — keeping proteins in memory
   greatly speeds up digestion but competes with the mass index for RAM.
2. **Mass index** (`mass_index.rs`) — `MassIndex::build_concurrently` cleaves every protein and builds
   a `HashMap<mass, HashSet<protein_id>>` in memory. Capacity is pre-estimated from a protein sample.
3. **Peptides** (`peptide_table.rs`) — `PeptideTable::build_concurrently` walks the mass index, re-digests
   the proteins per mass, collects distinct peptides (with optional protein/taxonomy associations), and
   batch-upserts them. It returns a `mass → partitions` map that, together with the protease, is persisted
   as the `Configuration` blob.

### Data model & encoding

- **Masses are integers.** Floats (Dalton) are scaled by `MASS_CONVERT_FACTOR = 1e9` via `mass::to_int` /
  `mass::to_float` / the `mass_to_int!` macro (the macro works in `const` contexts). All DB columns and
  index keys use the integer form.
- **Sequences are bit-packed.** `CompactSequence` (`sequence.rs`) stores amino acids as 5 bits each
  (bit code = `char - 'A'`) to save ~30% memory for in-memory maps/sets and DB blobs.
- **ScyllaDB schema** (`db.cql`): `proteins`, `peptides` (partitioned by `partition` then `mass`, `sequence`),
  and `blobs` (chunked key/part storage backing the `Blob` / `Configuration` types).
- **Peptides are partitioned by mass.** The `Configuration.mass_partitioning` map records which partitions
  hold which mass ranges; search and build both rely on it.

### Compile-time codegen (`build.rs`)

Amino acid constants (mono masses, integer masses, bit codes) and molecule masses (e.g. `WATER_MONO_MASS`)
are generated at build time from `dihardts_omicstools` into `$OUT_DIR/amino_acid.rs` and `molecules.rs`,
then included via the `create_const_amino_acids!` macro in `amino_acid.rs` / `molecules.rs`. Changing the
amino acid set means editing `build.rs`, not a static table.

### Search (`peptide_search.rs`)

`MultiTaskSearch` expands a query mass + PTM set into a set of partition/mass conditions
(`PeptideConditionBuilder`), runs them concurrently across `--threads`, filters by taxonomy/proteome/review
status, and streams `Peptidoform` results (ProForma-compliant by default, or canonical-only).

### Database client & congestion control (`client.rs`)

`Client` wraps a Scylla `CachingSession` and parses a custom URL:
`scylla://[user[:pass]@]host[:port][,host...]/keyspace[?attr=val&...]`. Query attributes:
`connection_timeout`, `pool_size`, `pool_type` (`host`|`shard`), `read_consistency_level`,
`write_consistency_level`, `cache_size`.

Inserts during a build go through `run_congested`, an AIMD congestion controller: a semaphore-gated window
grows on sustained success and shrinks on Scylla overload signals. Errors are classified
`Retryable` (transient: overload, timeout, unavailable, broken connection) vs `Fatal` (deterministic: syntax,
auth). Writes are idempotent, so retryable errors are retried with jittered backoff indefinitely — important
to keep in mind when a long build appears to stall.

### Monitoring (`monitoring/`)

`tracing` (logs) and `metrics` (counters/gauges) are wired to pluggable targets selected by CLI flags:
TUI, terminal, rotating log file, Loki, Prometheus, tokio-console. Build/search stages register and
deregister metrics (progress bars, insert rates, queue depth) around each phase.

### Web API (`web/server.rs`, axum)

Routes: `GET/POST /api/peptides/search`, `GET /api/peptides/{sequence}` (and existence check),
`GET /api/configuration`. Optional Matomo tracking middleware respects the `X-Do-Not-Track` header.

## Allocator & build features (`packages/macpepdb/Cargo.toml`)

`default = ["mimalloc"]`. Mutually pick one allocator: `mimalloc`, `jemalloc`, or `tcmalloc`
(`tcmalloc` needs `libstdc++`, `libclang`, `libunwind`). The `tokio-console` feature additionally
requires building with `RUSTFLAGS="--cfg tokio_unstable"`.
