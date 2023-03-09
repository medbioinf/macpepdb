CREATE TABLE IF NOT EXISTS proteins (
    accession VARCHAR(10) NOT NULL,
    secondary_accessions VARCHAR(10)[] NOT NULL,
    entry_name VARCHAR(16) NOT NULL,
    name TEXT NOT NULL,
    genes TEXT[] NOT NULL,
    taxonomy_id BIGINT NOT NULL,
    proteome_id CHAR(11) NOT NULL,
    is_reviewed BOOLEAN NOT NULL,
    sequence TEXT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (accession)
);
CREATE INDEX IF NOT EXISTS proteins_secondary_accessions_idx ON proteins USING gin (secondary_accessions);
CREATE INDEX IF NOT EXISTS proteins_genes_idx ON proteins USING gin (genes);
SELECT create_reference_table('proteins');