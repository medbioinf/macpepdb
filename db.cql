CREATE KEYSPACE IF NOT EXISTS macpepdb WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

DROP TABLE IF EXISTS macpepdb.proteins;
CREATE TABLE IF NOT EXISTS macpepdb.proteins (
    id int primary key,
    accession ascii,
    sequence blob,
    taxonomy_id int,
);
CREATE INDEX prot_acc_idx ON macpepdb.proteins (accession);

DROP TABLE IF EXISTS macpepdb.blobs;
CREATE TABLE IF NOT EXISTS macpepdb.blobs (
    key ascii,
    part smallint,
    data blob,
    PRIMARY KEY (key, part)
);

DROP TABLE IF EXISTS macpepdb.peptides;
CREATE TABLE IF NOT EXISTS macpepdb.peptides (
    partition bigint,
    mass bigint,
    sequence blob,
    protein_ids list<int>,
    unique_taxonomy_ids list<int>,
    non_unique_taxonomy_ids list<int>,
    PRIMARY KEY (partition, mass, sequence)
);
