create table str_peptides (
    mass bigint not null,
    sequence varchar(50) not null,
    primary key (mass, sequence)
);

create table bit_peptides (
    mass bigint not null,
    sequence varbit(250) not null,
    primary key (mass, sequence)
);

create table bytea_peptides (
    mass bigint not null,
    sequence bytea not null,
    primary key (mass, sequence)
);


SELECT create_distributed_table('str_peptides', 'mass');
SELECT create_distributed_table('bit_peptides', 'mass');
SELECT create_distributed_table('bytea_peptides', 'mass');
