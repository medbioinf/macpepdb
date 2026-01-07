pub const UP: [&str; 6] = [
    CREATE_MIGRATIONS_TABLE,
    CREATE_CONFIG_TABLE,
    CREATE_DOMAINS_TYPE,
    CREATE_PROTEINS_TABLE,
    CREATE_PEPTIDES_TABLE,
    CREATE_BLOBS_TABLE,
];

pub const DROP_KEYSPACE: &str = "DROP KEYSPACE IF EXISTS :KEYSPACE:;";

pub const CREATE_KEYSPACE: &str = "CREATE KEYSPACE IF NOT EXISTS :KEYSPACE:
                WITH REPLICATION = {'class': 'NetworkTopologyStrategy',
                'replication_factor': :REPLICATION_FACTOR:};";

const CREATE_MIGRATIONS_TABLE: &str = "CREATE TABLE :KEYSPACE:.migrations ( pk TEXT, id INT, created TEXT, description TEXT, PRIMARY KEY(pk, id));";

const CREATE_CONFIG_TABLE: &str = "CREATE TABLE IF NOT EXISTS :KEYSPACE:.config (
        conf_key text PRIMARY KEY,
        value text
    );";

const CREATE_DOMAINS_TYPE: &str =
    "CREATE TYPE IF NOT EXISTS :KEYSPACE:.Domain (name text, evidence text, start_index bigint, end_index bigint, protein text, start_index_protein bigint, end_index_protein bigint, peptide_offset bigint);";

const CREATE_PROTEINS_TABLE: &str = "CREATE TABLE IF NOT EXISTS :KEYSPACE:.proteins (
        accession text PRIMARY KEY,
        secondary_accessions list<text>,
        entry_name text,
        name text,
        genes list<text>,
        taxonomy_id bigint,
        proteome_id text,
        is_reviewed boolean,
        sequence text,
        updated_at bigint,
        domains frozen<set<Domain>>
    );";

const CREATE_PEPTIDES_TABLE: &str = "CREATE TABLE IF NOT EXISTS :KEYSPACE:.peptides (
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
        domains frozen<set<Domain>>,
        is_metadata_updated boolean,
        PRIMARY KEY (partition, mass, sequence)
    );";

const CREATE_BLOBS_TABLE: &str = "CREATE TABLE IF NOT EXISTS :KEYSPACE:.blobs (
        key text, 
        position bigint, 
        data blob,
        PRIMARY KEY (key, position)
    );";
