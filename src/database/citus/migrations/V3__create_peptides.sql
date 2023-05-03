CREATE TABLE IF NOT EXISTS peptides (
    partition BIGINT NOT NULL,
    mass BIGINT NOT NULL,
    sequence VARCHAR(60) NOT NULL,
    missed_cleavages SMALLINT NOT NULL,
    aa_counts SMALLINT[26] NOT NULL,
    proteins VARCHAR(10)[] NOT NULL,
    is_swiss_prot BOOLEAN NOT NULL,
    is_trembl BOOLEAN NOT NULL,
    taxonomy_ids BIGINT[] NOT NULL,
    unique_taxonomy_ids BIGINT[] NOT NULL,
    proteome_ids CHAR(11)[] NOT NULL,
    is_metadata_updated BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (partition, mass, sequence)
);
SELECT create_distributed_table('peptides', 'partition');
-- CREATE INDEX IF NOT EXISTS peptides_is_metadata_updated_idx ON peptides (is_metadata_updated) WHERE is_metadata_updated = false;
