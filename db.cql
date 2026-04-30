CREATE KEYSPACE IF NOT EXISTS macpepdb WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

DROP TABLE IF EXISTS macpepdb.proteins;
CREATE TABLE IF NOT EXISTS macpepdb.proteins (
    id int primary key,
    accession ascii,
    sequence blob,
);
CREATE INDEX prot_acc_idx ON macpepdb.proteins (accession);

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

DROP TABLE IF EXISTS macpepdb.blobs;
CREATE TABLE IF NOT EXISTS macpepdb.blobs (
    key ascii,
    part smallint,
    data blob,
    PRIMARY KEY (key, part)
);

DROP TABLE IF EXISTS macpepdb.peptides;
CREATE TABLE IF NOT EXISTS macpepdb.peptides (
    partition smallint,
    mass bigint,
    sequence blob,
    PRIMARY KEY (partition, mass, sequence)
);
