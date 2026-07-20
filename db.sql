CREATE EXTENSION IF NOT EXISTS citus;
CREATE EXTENSION IF NOT EXISTS citus_columnar;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE OR REPLACE FUNCTION genes_as_text(text[])
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT array_to_string($1, ' ')
$$;

DROP TABLE IF EXISTS proteins;
CREATE TABLE proteins (
    id          INTEGER PRIMARY KEY,
    accession   TEXT,
    sequence    BYTEA,
    taxonomy_id INTEGER,
    flags       "char", -- `"char"` is different from CHAR"
    genes       TEXT[]
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

DROP TABLE IF EXISTS peptides;
CREATE TABLE peptides (
    partition               BIGINT,
    mass                    BIGINT,
    sequence                BYTEA,
    amino_acid_counts       BYTEA,
    protein_ids             BYTEA,
    unique_taxonomy_ids     INTEGER[],
    non_unique_taxonomy_ids INTEGER[],
    flags                   "char" -- `"char"` is different from CHAR"
) USING columnar;

ALTER TABLE peptides SET (
    columnar.compression       = 'zstd',
    columnar.compression_level = 9,
    columnar.stripe_row_limit  = 150000,
    columnar.chunk_group_row_limit = 10000
);

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

SET citus.shard_count = 1024;

SELECT create_distributed_table('peptides', 'partition');
SELECT create_distributed_table('proteins', 'id');
SELECT create_distributed_table('blobs', 'key');
SELECT create_distributed_table('stats', 'key');

ALTER SYSTEM SET synchronous_commit = 'off';
ALTER SYSTEM SET fsync = 'off';                       -- rebuildable DB only
ALTER SYSTEM SET full_page_writes = 'off';            -- rebuildable DB only
ALTER SYSTEM SET checkpoint_timeout = '60min';
ALTER SYSTEM SET max_wal_size = '96GB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET autovacuum = 'off';
SELECT pg_reload_conf();

SELECT run_command_on_workers($$ ALTER SYSTEM SET synchronous_commit = 'off' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET fsync = 'off' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET full_page_writes = 'off' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET checkpoint_timeout = '60min' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET max_wal_size = '96GB' $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET checkpoint_completion_target = 0.9 $$);
SELECT run_command_on_workers($$ ALTER SYSTEM SET autovacuum = 'off' $$);
SELECT run_command_on_workers($$ SELECT pg_reload_conf() $$);
