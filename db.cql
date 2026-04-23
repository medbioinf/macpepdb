CREATE KEYSPACE IF NOT EXISTS macpepdb WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

DROP TABLE IF EXISTS macpepdb.proteins;
CREATE TABLE IF NOT EXISTS macpepdb.proteins (
    accession ascii primary key,
    sequence blob,
);

DROP TABLE IF EXISTS macpepdb.mass_index;
CREATE TABLE IF NOT EXISTS macpepdb.mass_index (
    mass bigint primary key,
    proteins set<text>,
);

DROP TABLE IF EXISTS macpepdb.mass_counts;
CREATE TABLE IF NOT EXISTS macpepdb.mass_counts (
    mass bigint primary key,
    count bigint,
);

DROP TABLE IF EXISTS macpepdb.peptides;
CREATE TABLE IF NOT EXISTS macpepdb.peptides (
    mass bigint,
    sequence blob,
    PRIMARY KEY (mass, sequence)
);
