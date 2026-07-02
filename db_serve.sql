ALTER SYSTEM RESET synchronous_commit;
ALTER SYSTEM RESET fsync;
ALTER SYSTEM RESET full_page_writes;
ALTER SYSTEM RESET max_wal_size;
ALTER SYSTEM RESET checkpoint_timeout;
ALTER SYSTEM RESET checkpoint_completion_target;
ALTER SYSTEM RESET autovacuum;
SELECT pg_reload_conf();

SELECT run_command_on_workers($$ ALTER SYSTEM RESET synchronous_commit $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET fsync $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET full_page_writes $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET max_wal_size $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET checkpoint_timeout $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET checkpoint_completion_target $$);
SELECT run_command_on_workers($$ ALTER SYSTEM RESET autovacuum $$);
SELECT run_command_on_workers($$ SELECT pg_reload_conf() $$);

ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
SELECT pg_reload_conf();
SELECT run_command_on_workers($$ ALTER SYSTEM SET max_parallel_workers_per_gather = 4 $$);
SELECT run_command_on_workers($$ SELECT pg_reload_conf() $$);

ALTER SYSTEM SET citus.max_shared_pool_size = 900;
ALTER SYSTEM SET citus.max_cached_conns_per_worker = 2;
SELECT pg_reload_conf();

ALTER SYSTEM SET max_parallel_maintenance_workers = 8;
SELECT pg_reload_conf();
SELECT run_command_on_workers($$ ALTER SYSTEM SET max_parallel_maintenance_workers = 8 $$);
SELECT run_command_on_workers($$ SELECT pg_reload_conf() $$);

CREATE INDEX prot_acc_idx ON proteins USING GIN (accession gin_trgm_ops);
CREATE INDEX prot_gene_str_idx ON proteins USING GIN (genes_as_text(genes) gin_trgm_ops);
CREATE INDEX prot_acc_eq_idx ON proteins (accession);

ALTER SYSTEM RESET max_parallel_maintenance_workers;
SELECT pg_reload_conf();
SELECT run_command_on_workers($$ ALTER SYSTEM RESET max_parallel_maintenance_workers $$);
SELECT run_command_on_workers($$ SELECT pg_reload_conf() $$);

ANALYZE peptides;
ANALYZE peptide_metadata;
ANALYZE proteins;
ANALYZE blobs;
ANALYZE stats;
