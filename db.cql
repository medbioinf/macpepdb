CREATE KEYSPACE IF NOT EXISTS macpepdb WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

DROP TABLE IF EXISTS macpepdb.peptides;
CREATE TABLE IF NOT EXISTS macpepdb.peptides (
    mass bigint,
    sequence blob,
    PRIMARY KEY (mass, sequence)
);

DROP TABLE IF EXISTS macpepdb.proteins;
CREATE TABLE IF NOT EXISTS macpepdb.proteins (
    accession ascii primary key,
    sequence blob,
);
