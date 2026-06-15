-- MaCPepDB Lite — SERVE-MODE settings.
--
-- Run AFTER the build completes and BEFORE serving searches:
--   psql -h 127.0.0.1 -U postgres -d macpepdb -f db_serve.sql
--
-- This reverts the TRANSIENT build-mode overrides that db.sql applied (durability off,
-- large WAL, autovacuum off) so the affected settings fall back to the postgresql.conf
-- baseline (your PGTune "dw" profile: effective_cache_size, maintenance_work_mem,
-- shared_buffers, work_mem, random_page_cost, io_method, ...), then refreshes statistics.
--
-- It targets the coordinator (ALTER SYSTEM) and every worker (run_command_on_workers),
-- because the peptide shards live on the workers. Each ALTER SYSTEM is its own single
-- statement (it cannot run inside a transaction block).
--
-- Read tuning (effective_cache_size, random_page_cost, work_mem, ...) is owned by
-- postgresql.conf and intentionally NOT set here — the RESETs below just let that
-- baseline take effect again.

-- --------------------------------------------------------------------------
-- Revert build-mode overrides -> fall back to the postgresql.conf baseline.
-- (If you treat the served DB as still disposable, you may keep synchronous_commit
-- off for lower latency — then comment out its RESET on both coordinator and workers.)
-- --------------------------------------------------------------------------

-- Coordinator.
ALTER SYSTEM RESET synchronous_commit;
ALTER SYSTEM RESET fsync;
ALTER SYSTEM RESET full_page_writes;
ALTER SYSTEM RESET max_wal_size;
ALTER SYSTEM RESET checkpoint_timeout;
ALTER SYSTEM RESET checkpoint_completion_target;
ALTER SYSTEM RESET autovacuum;
SELECT pg_reload_conf();

-- Workers (where the columnar shards are scanned).
SELECT run_command_on_workers($$ ALTER SYSTEM RESET synchronous_commit $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET fsync $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET full_page_writes $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET max_wal_size $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET checkpoint_timeout $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET checkpoint_completion_target $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET autovacuum $$);
SELECT run_command_on_workers($$ SELECT pg_reload_conf() $$);

-- --------------------------------------------------------------------------
-- Refresh planner statistics. Columnar tables are not autovacuumed, so the planner
-- needs fresh stats to prune stripes effectively. ANALYZE on a distributed table is
-- propagated to the shards by Citus.
-- --------------------------------------------------------------------------
ANALYZE peptides;
ANALYZE peptide_metadata;
ANALYZE proteins;
ANALYZE blobs;
ANALYZE stats;

-- --------------------------------------------------------------------------
-- (Optional) Warm caches so the first searches are not cold. Requires the
-- pg_prewarm extension on every worker; prewarming distributed shards means
-- prewarming each shard placement, e.g.:
--
--   SELECT run_command_on_workers($$ CREATE EXTENSION IF NOT EXISTS pg_prewarm $$);
--   SELECT run_command_on_placements('peptides', $$ SELECT pg_prewarm('%s') $$);
--
-- Left commented: only worthwhile if cold first-query latency matters and the
-- working set fits in RAM.
-- --------------------------------------------------------------------------
