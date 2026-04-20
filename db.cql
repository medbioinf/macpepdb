CREATE KEYSPACE IF NOT EXISTS macpepdb WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};

DROP TABLE IF EXISTS macpepdb.str_peptides;
CREATE TABLE IF NOT EXISTS macpepdb.str_peptides (
    mass bigint,
    sequence text,
    PRIMARY KEY (mass, sequence)
);

DROP TABLE IF EXISTS macpepdb.bytea_peptides;

CREATE TABLE IF NOT EXISTS macpepdb.bytea_peptides (
    mass bigint,
    sequence blob,
    PRIMARY KEY (mass, sequence)
);
