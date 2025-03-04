pub const DROP_KEYSPACE: &str = "DROP KEYSPACE IF EXISTS :KEYSPACE:;";

pub const CREATE_KEYSPACE: &str = "CREATE KEYSPACE IF NOT EXISTS :KEYSPACE:
                WITH REPLICATION = {'class': 'NetworkTopologyStrategy',
                'replication_factor': 1};";

const CREATE_MIGRATIONS_TABLE: &str = "CREATE TABLE :KEYSPACE:.migrations ( pk TEXT, id INT, created TEXT, description TEXT, PRIMARY KEY(pk, id));";

const CREATE_CONFIG_TABLE: &str = "CREATE TABLE IF NOT EXISTS :KEYSPACE:.config (
        conf_key text PRIMARY KEY,
        value text
    );";

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
        is_metadata_updated boolean,
        PRIMARY KEY (partition, mass, sequence)
    );";

const CREATE_BLOBS_TABLE: &str = "CREATE TABLE IF NOT EXISTS :KEYSPACE:.blobs (
        key text,
        position bigint,
        data blob,
        PRIMARY KEY (key, position)
    );";

#[allow(dead_code)]
/// Create domains type
/// This is only applied if the `domains` feature is enabled
///
const CREATE_DOMAINS_TYPE: &str = "CREATE TYPE IF NOT EXISTS :KEYSPACE:.Domain (
        name text,
        evidence text,
        start_index bigint,
        end_index bigint,
        protein text,
        start_index_protein bigint,
        end_index_protein bigint,
        peptide_offset bigint
    );";

#[allow(dead_code)]
/// Adds the domain type to the proteins table
/// This is only applied if the `domains` feature is enabled
///
const ADD_DOMAINS_TO_PROTEINS_TABLE: &str =
    "ALTER TABLE :KEYSPACE:.proteins ADD domains set<frozen<Domain>>;";

#[allow(dead_code)]
/// Adds the domain type to the peptides table
/// This is only applied if the `domains` feature is enabled
///
const ADD_DOMAINS_TO_PEPTIDES_TABLE: &str =
    "ALTER TABLE :KEYSPACE:.peptides ADD domains set<frozen<Domain>>;";

lazy_static! {
    /// List of all migration steps to be executed in order
    pub static ref UP: Vec<&'static str> = vec![
        CREATE_MIGRATIONS_TABLE,
        CREATE_CONFIG_TABLE,
        CREATE_PROTEINS_TABLE,
        CREATE_PEPTIDES_TABLE,
        CREATE_BLOBS_TABLE,
        #[cfg(feature = "domains")]
        CREATE_DOMAINS_TYPE,
        #[cfg(feature = "domains")]
        ADD_DOMAINS_TO_PROTEINS_TABLE,
        #[cfg(feature = "domains")]
        ADD_DOMAINS_TO_PEPTIDES_TABLE,
    ];
}
