pub const UP: [&str; 4] = [
    CREATE_MIGRATIONS_TABLE,
    CREATE_CONFIG_TABLE,
    CREATE_PROTEINS_TABLE,
    CREATE_PEPTIDES_TABLE,
];

pub const DROP_KEYSPACE: &str = "DROP KEYSPACE IF EXISTS macpep_;";

pub const CREATE_KEYSPACE: &str = "CREATE KEYSPACE IF NOT EXISTS macpep_
                WITH REPLICATION = {'class': 'NetworkTopologyStrategy',
                'replication_factor': 1};";

const CREATE_MIGRATIONS_TABLE: &str = "CREATE TABLE macpep_.migrations ( pk TEXT, id INT, created TEXT, description TEXT, PRIMARY KEY(pk, id));";

const CREATE_CONFIG_TABLE: &str = "CREATE TABLE IF NOT EXISTS macpep_.config (
        conf_key text PRIMARY KEY,
        value text
    );";

const CREATE_PROTEINS_TABLE: &str = "CREATE TABLE IF NOT EXISTS macpep_.proteins (
        accession text PRIMARY KEY,
        secondary_accessions list<text>,
        entry_name text,
        name text,
        genes list<text>,
        taxonomy_id bigint,
        proteome_id text,
        is_reviewed boolean,
        sequence text,
        updated_at bigint
    );";

const CREATE_PEPTIDES_TABLE: &str = "CREATE TABLE IF NOT EXISTS macpep_.peptides (
        partition bigint,
        mass bigint,
        sequence text,
        missed_cleavages smallint,
        aa_counts list<smallint>,
        proteins set<text>,
        is_swiss_prot boolean,
        is_trembl boolean,
        taxonomy_ids set<bigint>,
        unique_taxonomy_ids set<bigint>,
        proteome_ids set<text>,
        is_metadata_updated boolean,
        PRIMARY KEY (partition, mass, sequence)
    );";
