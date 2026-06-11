-- MaCPepDB Lite — PostgreSQL/Citus schema (replaces db.cql).
--
-- NOTE: like the old db.cql, running this DROPs and recreates everything in the
-- `macpepdb` schema. Run against the Citus coordinator, e.g.:
--   psql -h 127.0.0.1 -U postgres -d macpepdb -f db.sql
--
-- Layout:
--   peptides            COLUMNAR (zstd), distributed by `partition`     -- the bulk of the data
--   proteins/blobs/stats  row-store, Citus reference tables (replicated to every node)
--
-- The peptides table has NO primary key / index: columnar storage does not support
-- them. Selective reads (`WHERE partition = ANY($1) AND mass = $2`) rely on Citus shard
-- pruning on `partition` plus columnar stripe/chunk-group min/max pruning, which works
-- because the build loads each partition as one sorted (partition, mass) stripe.

CREATE EXTENSION IF NOT EXISTS citus;

DROP SCHEMA IF EXISTS macpepdb CASCADE;
CREATE SCHEMA macpepdb;

-- --------------------------------------------------------------------------
-- Reference tables (row-store): small, replicated to all nodes, support PK/index.
-- --------------------------------------------------------------------------

CREATE TABLE macpepdb.proteins (
    id          INTEGER PRIMARY KEY,
    accession   TEXT,
    sequence    BYTEA,
    taxonomy_id INTEGER
);

CREATE TABLE macpepdb.blobs (
    key  TEXT,
    part SMALLINT,
    data BYTEA,
    PRIMARY KEY (key, part)
);

CREATE TABLE macpepdb.stats (
    key   TEXT PRIMARY KEY,
    value BIGINT
);

-- --------------------------------------------------------------------------
-- Peptides: columnar, distributed by `partition`.
-- --------------------------------------------------------------------------

CREATE TABLE macpepdb.peptides (
    partition               BIGINT,
    mass                    BIGINT,
    sequence                BYTEA,
    protein_ids             BYTEA,
    unique_taxonomy_ids     INTEGER[],
    non_unique_taxonomy_ids INTEGER[]
) USING columnar;

-- Columnar tuning. stripe_row_limit MUST match the build's STRIPE_ROW_LIMIT constant
-- (cql.rs): the build COPYs exactly one stripe worth of rows per partition.
ALTER TABLE macpepdb.peptides SET (
    columnar.compression       = 'zstd',
    columnar.compression_level = 9,
    columnar.stripe_row_limit  = 150000,
    columnar.chunk_group_row_limit = 10000
);

-- --------------------------------------------------------------------------
-- Citus distribution.
-- --------------------------------------------------------------------------

-- shard_count governs read fan-out parallelism; rule of thumb ~2-4x total worker cores.
SET citus.shard_count = 32;

SELECT create_distributed_table('macpepdb.peptides', 'partition');
SELECT create_reference_table('macpepdb.proteins');
SELECT create_reference_table('macpepdb.blobs');
SELECT create_reference_table('macpepdb.stats');

-- Secondary index on the proteins reference table (propagated to all placements).
CREATE INDEX prot_acc_idx ON macpepdb.proteins (accession);

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
-- NOT set here (require a restart; configure via docker-compose `-c` flags or
-- postgresql.conf): shared_buffers (~25% RAM), max_connections, wal_level.
-- ==========================================================================

-- Coordinator.
ALTER SYSTEM SET synchronous_commit = 'off';
ALTER SYSTEM SET fsync = 'off';                       -- rebuildable DB only
ALTER SYSTEM SET full_page_writes = 'off';            -- rebuildable DB only
ALTER SYSTEM SET max_wal_size = '64GB';
ALTER SYSTEM SET checkpoint_timeout = '60min';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET maintenance_work_mem = '2GB';        -- ADJUST to worker RAM
ALTER SYSTEM SET autovacuum = 'off';
SELECT pg_reload_conf();

-- Workers (where the columnar shards are loaded).
SELECT run_command_on_workers($$ ALTER SYSTEM SET synchronous_commit = 'off' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET fsync = 'off' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET full_page_writes = 'off' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET max_wal_size = '64GB' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET checkpoint_timeout = '60min' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET checkpoint_completion_target = 0.9 $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET maintenance_work_mem = '2GB' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET autovacuum = 'off' $$);
SELECT run_command_on_workers($$ SELECT pg_reload_conf() $$);
