drop table if exists str_peptides cascade;
create table if not exists str_peptides (
    mass bigint not null,
    sequence varchar(50) not null,
    primary key (mass, sequence)
);
SELECT create_distributed_table('str_peptides', 'mass');

drop table if exists bit_peptides cascade;
create table if not exists bit_peptides (
    mass bigint not null,
    sequence varbit(250) storage plain not null,
    primary key (mass, sequence)
);
SELECT create_distributed_table('bit_peptides', 'mass');

drop table if exists bytea_peptides cascade;
create table if not exists bytea_peptides (
    mass bigint not null,
    sequence bytea storage plain not null,
    primary key (mass, sequence)

);
SELECT create_distributed_table('bytea_peptides', 'mass');
