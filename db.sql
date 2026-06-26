-- MaCPepDB Lite — PostgreSQL/Citus schema (replaces db.cql).
--
-- NOTE: like the old db.cql, running this DROPs and recreates everything in the
-- `macpepdb` schema. Run against the Citus coordinator, e.g.:
--   psql -h 127.0.0.1 -U postgres -d macpepdb -f db.sql
--
-- Layout:
--   peptides            COLUMNAR (zstd), distributed by `partition`     -- the bulk of the data
--   peptide_metadata    row-store, distributed by `metadata_id`         -- deduplicated protein-id sets
--   proteins/blobs/stats: row-store, distributed (proteins by id; blobs/stats by key)
--
-- `peptides.metadata_id` references `peptide_metadata.metadata_id` (a shared, deduplicated
-- protein-id set). There is intentionally NO foreign key: Citus restricts FKs across tables
-- with different distribution columns, and the build relies on a completion barrier (all
-- metadata rows are committed before serving) rather than per-row referential checks.
--
-- The peptides table has NO primary key / index: columnar storage does not support
-- them. Selective reads (`WHERE partition = ANY($1) AND mass = $2`) rely on Citus shard
-- pruning on `partition` plus columnar stripe/chunk-group min/max pruning, which works
-- because the build loads each partition as one sorted (partition, mass) stripe.

CREATE EXTENSION IF NOT EXISTS citus;
CREATE EXTENSION IF NOT EXISTS citus_columnar;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- --------------------------------------------------------------------------
-- Row-store tables (distributed): proteins by `id`, blobs/stats by `key`.
-- --------------------------------------------------------------------------
DROP TABLE IF EXISTS proteins;
CREATE TABLE proteins (
    id          INTEGER PRIMARY KEY,
    accession   TEXT,
    sequence    BYTEA,
    taxonomy_id INTEGER,
    flags       "char" -- `"char"` is different from CHAR"
);
DROP TABLE IF EXISTS blobs;
CREATE TABLE blobs (
    key  TEXT,
    part SMALLINT,
    data BYTEA,
    PRIMARY KEY (key, part)
);
DROP TABLE IF EXISTS stats;
CREATE TABLE stats (
    key   TEXT PRIMARY KEY,
    value BIGINT
);

-- Deduplicated protein-id sets. Each distinct set is stored once; peptides reference it by
-- `metadata_id`. Row-store with a PK so resolution (`WHERE metadata_id = ANY($1)`) uses shard
-- pruning + a local index lookup (columnar has no indexes and would scan).
DROP TABLE IF EXISTS peptide_metadata;
CREATE TABLE peptide_metadata (
    metadata_id BIGINT PRIMARY KEY,
    protein_ids BYTEA
);

-- --------------------------------------------------------------------------
-- Peptides: columnar, distributed by `partition`.
-- --------------------------------------------------------------------------
DROP TABLE IF EXISTS peptides;
CREATE TABLE peptides (
    partition               BIGINT,
    mass                    BIGINT,
    sequence                BYTEA,
    amino_acid_counts       BYTEA,
    metadata_id             BIGINT,
    unique_taxonomy_ids     INTEGER[],
    non_unique_taxonomy_ids INTEGER[],
    flags                   "char" -- `"char"` is different from CHAR"
) USING columnar;

-- Columnar tuning. stripe_row_limit MUST match the build's STRIPE_ROW_LIMIT constant
-- (cql.rs): the build COPYs exactly one stripe worth of rows per partition.
ALTER TABLE peptides SET (
    columnar.compression       = 'zstd',
    columnar.compression_level = 9,
    columnar.stripe_row_limit  = 150000,
    columnar.chunk_group_row_limit = 10000
);

-- --------------------------------------------------------------------------
-- Taxonomy
-- --------------------------------------------------------------------------
DROP TABLE IF EXISTS taxonomies;
DROP TABLE IF EXISTS taxonomy_ranks;

CREATE TABLE taxonomy_ranks (
    id SMALLINT PRIMARY KEY,
    name TEXT NOT NULL
);


CREATE TABLE taxonomies (
    id INT PRIMARY KEY,
    parent_id INT REFERENCES taxonomies(id) DEFERRABLE INITIALLY DEFERRED,
    scientific_name TEXT NOT NULL,
    rank_id SMALLINT REFERENCES taxonomy_ranks(id)

);

CREATE INDEX tax_name_idx ON taxonomies USING GIN (scientific_name gin_trgm_ops);


-- --------------------------------------------------------------------------
-- Citus distribution.
-- --------------------------------------------------------------------------

-- shard_count governs read fan-out parallelism; rule of thumb ~2-4x total worker cores.
SET citus.shard_count = 1024;

SELECT create_distributed_table('peptides', 'partition');
SELECT create_distributed_table('peptide_metadata', 'metadata_id');
SELECT create_distributed_table('proteins', 'id');
SELECT create_distributed_table('blobs', 'key');
SELECT create_distributed_table('stats', 'key');

-- Secondary index on proteins (created on every shard).
CREATE INDEX prot_acc_idx ON proteins (accession);

-- ==========================================================================
-- BUILD-MODE performance settings.
--
-- Applied to the coordinator (ALTER SYSTEM) AND every worker (run_command_on_workers),
-- because the peptide shards live on the workers. Everything here is reloadable — no
-- restart needed. The durability-off settings are SAFE ONLY because the database is
-- fully rebuildable from the UniProt source files; revert them with db_serve.sql
-- after the build and before serving searches.
--
-- NOTE: each ALTER SYSTEM must be its own single statement (it cannot run inside a
-- transaction block, and a multi-statement string is one implicit transaction) — hence
-- one run_command_on_workers() call per setting.
--
-- This layers on top of a static base config in postgresql.conf (e.g. a PGTune "dw"
-- profile: shared_buffers, effective_cache_size, maintenance_work_mem, work_mem,
-- random_page_cost, io_method, max_connections, ...). The ALTER SYSTEM lines below are
-- TRANSIENT build overrides (written to postgresql.auto.conf); db_serve.sql RESETs them
-- so each setting falls back to the postgresql.conf baseline. Keep that baseline in
-- postgresql.conf (NOT via ALTER SYSTEM), or a RESET would fall back to built-in
-- defaults instead. Restart-only settings (shared_buffers, max_connections, wal_level)
-- live in postgresql.conf / `-c` flags, not here.
-- ==========================================================================

-- Coordinator.
ALTER SYSTEM SET synchronous_commit = 'off';
ALTER SYSTEM SET fsync = 'off';                       -- rebuildable DB only
ALTER SYSTEM SET full_page_writes = 'off';            -- rebuildable DB only
ALTER SYSTEM SET checkpoint_timeout = '60min';
ALTER SYSTEM SET autovacuum = 'off';
-- maintenance_work_mem intentionally NOT overridden — comes from the postgresql.conf baseline.
SELECT pg_reload_conf();

-- Workers (where the columnar shards are loaded).
SELECT run_command_on_workers($$ ALTER SYSTEM SET synchronous_commit = 'off' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET fsync = 'off' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET full_page_writes = 'off' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET checkpoint_timeout = '60min' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET autovacuum = 'off' $$);
SELECT run_command_on_workers($$ SELECT pg_reload_conf() $$);
