pub const UP: [&str; 4] = [
    CREATE_KEYSPACE,
    CREATE_CONFIG_TABLE,
    CREATE_PROTEINS_TABLE,
    CREATE_PEPTIDES_TABLE,
];

pub const DROP_KEYSPACE: &str = "DROP KEYSPACE IF EXISTS macpep;";

const CREATE_KEYSPACE: &str = "CREATE KEYSPACE IF NOT EXISTS macpep
    WITH replication = { 
            'class': 'NetworkTopologyStrategy',
            'datacenter1': '1'
        }  AND durable_writes = true;";

const CREATE_CONFIG_TABLE: &str = "CREATE TABLE IF NOT EXISTS macpep.config (
        conf_key text PRIMARY KEY,
        value text
    );";

const CREATE_PROTEINS_TABLE: &str = "CREATE TABLE IF NOT EXISTS macpep.proteins (
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
    );";

const CREATE_PEPTIDES_TABLE: &str = "CREATE TABLE IF NOT EXISTS macpep.peptides (
        partition smallint,
        mass bigint,
        sequence text,
        missed_cleavages smallint,
        aa_counts list<smallint>,
        proteins set<text>,
        length smallint,
        is_swiss_prot boolean,
        is_trembl boolean,
        taxonomy_ids set<int>,
        unique_taxonomy_ids set<int>,
        proteome_ids set<text>,
        is_metadata_updated boolean,
        PRIMARY KEY (partition, mass, sequence)
    );";
