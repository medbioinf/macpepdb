# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

MaCPepDB Lite builds and serves a mass-indexed peptide database. It digests UniProt protein
files with a protease, stores the resulting peptides in PostgreSQL with the Citus extension, and
answers mass-based peptide searches (including variable post-translational modifications) over
a CLI and an HTTP API.

## Workspace layout

Cargo workspace (Rust edition 2024) under `packages/`, including:

- **`macpepdb`** — the main library + binary. All domain logic, the build pipeline, search, and the web API.
- **`uniprot_reader`** (`macpepdb_uniprot_reader`) — streaming/async parser + indexer for UniProt text dumps (`.txt`, `.txt.gz`).
- **`tui`** (`macpepdb_tui`) — `ratatui` dashboard that subscribes to `tracing` events and `metrics` recorders; see [packages/tui/README.md](packages/tui/README.md).
- **`metrics-peek`** (`macpepdb_metrics_peek`) — a `metrics::Recorder` that forwards values to a callback (immediate or periodic).
- **`hydrophobicity`** (`macpepdb_peptide_hydrophobicity`) — peptide hydrophobicity prediction (Krokhin et al).

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

The default DB URL is `postgresql://postgres@127.0.0.1:5432/macpepdb`. Bring up a local
Citus cluster and initialize the schema before building:

```bash
docker compose up -d --scale worker=2             # Citus coordinator on 5432 + 2 workers
psql -h 127.0.0.1 -U postgres -d macpepdb -f db.sql       # schema + build-mode tuning (NOTE: DROPs existing schema)
cargo run -r -- build packages/.../proteins.txt           # digest + load (see subcommands below)
psql -h 127.0.0.1 -U postgres -d macpepdb -f db_serve.sql # revert build tuning, ANALYZE, read tuning
cargo run -r -- api 127.0.0.1:8080                        # serve the HTTP API
```

`db.sql` applies aggressive build-mode settings (durability off, large WAL, autovacuum off) to the
coordinator and every worker; `db_serve.sql` reverts them, sets read tuning, and refreshes statistics.
The durability-off settings are safe only because the DB is rebuildable from the source files.

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
- **PostgreSQL/Citus schema** (`db.sql`): `peptides` is an **`UNLOGGED` columnar** table distributed by
  `partition`; `proteins` (distributed by `id`), `blobs` (by `key`, chunked storage backing
  `Blob`/`Configuration`), and `stats` (by `key`) are row-store distributed tables. Columnar `peptides`
  has no PK/index — selective reads rely on Citus shard pruning on `partition` plus columnar
  stripe/chunk-group pruning. `UNLOGGED` skips WAL during the build (rebuildable DB), but means a worker
  crash truncates its shards → rebuild required.
- **Peptides are partitioned by mass.** The `Configuration.mass_partitioning` map records which partitions
  hold which mass ranges; search and build both rely on it. The build COPYs exactly `STRIPE_ROW_LIMIT`
  (`cql.rs`, = `db.sql`'s `columnar.stripe_row_limit`) rows per partition so each partition is one full
  columnar stripe, loaded in `(partition, mass)` order for effective pruning.

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

`Client` wraps a `deadpool_postgres` connection pool (`tokio-postgres`, `NoTls`) and parses a
`postgresql://[user[:pass]@]host[:port][,host...]/dbname[?param=val&...]` URL. Standard libqp params
(`connect_timeout`, `sslmode`, ...) pass through to `tokio_postgres::Config`; the MaCPepDB-specific
`pool_size` is stripped out and sizes the pool. Bulk loads use binary `COPY` (`copy_in_binary`); reads
use `query`/`query_stream`. Custom blob columns (`Sequence`, `ProteinIds`) implement `ToSql`/`FromSql`
over `BYTEA`.

Writes during a build go through `run_congested`: a **fixed-size** concurrency semaphore (sized to the
pool) plus jittered-backoff retry. Errors are classified `is_retryable` (transient: connection drops,
SQLSTATE classes 08/40/53/57) vs fatal (deterministic: syntax, type, constraint, auth); writes are
idempotent so transient errors retry indefinitely. (Unlike the old Cassandra AIMD controller there is no
dynamic window — Postgres has no graded overload signal, and columnar has no background compaction to
ride out.)

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
requires building with `RUSTFLAGS="--cfg tokio_unstable"`. The `admin-api` feature (not in
`default`) adds `POST /api/admin/client`, letting a caller rebuild the DB client from a new
PostgreSQL URL, reload the `Configuration` (protease + mass partitioning) from that database, and
change `concurrent_searches` at runtime — never enable it on an internet-facing build.
