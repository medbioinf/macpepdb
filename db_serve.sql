-- MaCPepDB Lite — SERVE-MODE settings.
--
-- Run AFTER the build completes and BEFORE serving searches:
--   psql -h 127.0.0.1 -U postgres -d macpepdb -f db_serve.sql
--
-- This reverts the durability-off / bulk-load tuning that db.sql applied for the
-- build, tunes the planner for read-mostly columnar scans, and refreshes statistics.
-- Like db.sql, it targets the coordinator (ALTER SYSTEM) and every worker
-- (run_command_on_workers), because the peptide shards live on the workers.
--
-- Each ALTER SYSTEM is its own single statement (it cannot run inside a transaction
-- block), hence one run_command_on_workers() call per setting.

-- --------------------------------------------------------------------------
-- 1. Restore durability and normal bulk-load knobs (RESET -> server default).
--    Drop the `; -- rebuildable` reasoning from db.sql: a served DB should be durable.
--    (If you treat the served DB as still disposable, you may keep synchronous_commit
--    off for lower latency — then comment out its RESET below.)
-- --------------------------------------------------------------------------

-- Coordinator.
ALTER SYSTEM RESET synchronous_commit;
ALTER SYSTEM RESET fsync;
ALTER SYSTEM RESET full_page_writes;
ALTER SYSTEM RESET max_wal_size;
ALTER SYSTEM RESET checkpoint_timeout;
ALTER SYSTEM RESET maintenance_work_mem;
ALTER SYSTEM RESET autovacuum;

-- 2. Read tuning: let the planner know most of the data is cached, favouring the
--    columnar stripe scans the mass search relies on.
ALTER SYSTEM SET effective_cache_size = '24GB';   -- ADJUST to ~75% of worker RAM

SELECT pg_reload_conf();

-- Workers (where the columnar shards are scanned).
SELECT run_command_on_workers($$ ALTER SYSTEM RESET synchronous_commit $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET fsync $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET full_page_writes $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET max_wal_size $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET checkpoint_timeout $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET maintenance_work_mem $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET autovacuum $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET effective_cache_size = '24GB' $$);  -- ADJUST
SELECT run_command_on_workers($$ SELECT pg_reload_conf() $$);

-- --------------------------------------------------------------------------
-- 3. Refresh planner statistics. Columnar tables are not autovacuumed, so the
--    planner needs fresh stats to prune stripes effectively. ANALYZE on a
--    distributed/reference table is propagated to the shards by Citus.
-- --------------------------------------------------------------------------
ANALYZE macpepdb.peptides;
ANALYZE macpepdb.proteins;
ANALYZE macpepdb.blobs;
ANALYZE macpepdb.stats;

-- --------------------------------------------------------------------------
-- 4. (Optional) Warm caches so the first searches are not cold. Requires the
--    pg_prewarm extension on every worker; prewarming distributed shards means
--    prewarming each shard placement, e.g.:
--
--    SELECT run_command_on_workers($$ CREATE EXTENSION IF NOT EXISTS pg_prewarm $$);
--    SELECT run_command_on_placements('macpepdb.peptides', $$ SELECT pg_prewarm('%s') $$);
--
--    Left commented: only worthwhile if cold first-query latency matters and the
--    working set fits in RAM.
-- --------------------------------------------------------------------------
