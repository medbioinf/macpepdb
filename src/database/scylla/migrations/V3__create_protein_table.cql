REATE TABLE macpep.proteins (
    accession text PRIMARY KEY,
    secondary_accessions list<text>,
    entry_name text,
    name text,
    genes list<text>,
    taxonomy_id int,
    proteome_id text,
    is_reviewed boolean,
    sequence text,
    updated_at bigint
);
