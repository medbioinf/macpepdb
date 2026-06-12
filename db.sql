-- MaCPepDB Lite — PostgreSQL/Citus schema (replaces db.cql).
--
-- NOTE: like the old db.cql, running this DROPs and recreates everything in the
-- `macpepdb` schema. Run against the Citus coordinator, e.g.:
--   psql -h 127.0.0.1 -U postgres -d macpepdb -f db.sql
--
-- Layout:
--   peptides            COLUMNAR (zstd), distributed by `partition`     -- the bulk of the data
--   proteins/blobs/stats: row-store, distributed (proteins by id; blobs/stats by key)
--
-- The peptides table has NO primary key / index: columnar storage does not support
-- them. Selective reads (`WHERE partition = ANY($1) AND mass = $2`) rely on Citus shard
-- pruning on `partition` plus columnar stripe/chunk-group min/max pruning, which works
-- because the build loads each partition as one sorted (partition, mass) stripe.

CREATE EXTENSION IF NOT EXISTS citus;
CREATE EXTENSION IF NOT EXISTS citus_columnar;

-- --------------------------------------------------------------------------
-- Row-store tables (distributed): proteins by `id`, blobs/stats by `key`.
-- --------------------------------------------------------------------------

CREATE TABLE proteins (
    id          INTEGER PRIMARY KEY,
    accession   TEXT,
    sequence    BYTEA,
    taxonomy_id INTEGER
);

CREATE TABLE blobs (
    key  TEXT,
    part SMALLINT,
    data BYTEA,
    PRIMARY KEY (key, part)
);

CREATE TABLE stats (
    key   TEXT PRIMARY KEY,
    value BIGINT
);

-- --------------------------------------------------------------------------
-- Peptides: columnar, distributed by `partition`.
-- --------------------------------------------------------------------------

CREATE TABLE peptides (
    partition               BIGINT,
    mass                    BIGINT,
    sequence                BYTEA,
    protein_ids             BYTEA,
    unique_taxonomy_ids     INTEGER[],
    non_unique_taxonomy_ids INTEGER[]
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
-- Citus distribution.
-- --------------------------------------------------------------------------

-- shard_count governs read fan-out parallelism; rule of thumb ~2-4x total worker cores.
SET citus.shard_count = 1024;

SELECT create_distributed_table('peptides', 'partition');
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
