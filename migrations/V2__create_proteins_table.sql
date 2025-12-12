CREATE TABLE proteins (
    accession VARCHAR(10) PRIMARY KEY,
    secondary_accessions varchar(10)[] NOT NULL,
    entry_name VARCHAR(16) NOT NULL,
    name TEXT NOT NULL,
    genes TEXT[] NOT NULL,
    taxonomy_id BIGINT NOT NULL,
    proteome_id VARCHAR(11) NOT NULL,
    is_reviewed BOOLEAN NOT NULL,
    sequence TEXT NOT NULL,
    updated_at BIGINT NOT NULL
);

SELECT create_reference_table('proteins');

CREATE INDEX entry_name_idx ON proteins USING BTREE (entry_name);
CREATE INDEX name_idx ON proteins USING BTREE (name);
CREATE INDEX secondary_accessions_idx ON proteins USING GIN (secondary_accessions);
CREATE INDEX genes_idx ON proteins USING GIN (genes);
