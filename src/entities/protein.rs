// 3rd party imports
use scylla::frame::response::result::CqlValue;
use scylla::frame::response::result::Row as ScyllaRow;
use tokio_postgres::Row;

#[derive(Clone, Debug, PartialEq)]
/// Keeps all data from the original UniProt entry which are necessary for MaCPepDB
///
pub struct Protein {
    accession: String,
    secondary_accessions: Vec<String>,
    entry_name: String,
    name: String,
    genes: Vec<String>,
    taxonomy_id: i64,
    proteome_id: String,
    is_reviewed: bool,
    sequence: String,
    updated_at: i64,
}

impl Protein {
    /// Creates a new protein
    ///
    /// # Arguments
    /// * `accession` - The primary accession
    /// * `secondary_accessions` - The secondary accessions
    /// * `entry_name` - The entry name
    /// * `name` - The protein name
    /// * `genes` - The genes name
    /// * `taxonomy_id` - The taxonomy ID
    /// * `proteome_id` - The proteome ID
    /// * `is_reviewed` - True if the protein is reviewed (contained by SwissProt)
    /// * `sequence` - The amino acid sequence
    /// * `updated_at` - The last update date as unix timestamp
    ///
    pub fn new(
        accession: String,
        secondary_accessions: Vec<String>,
        entry_name: String,
        name: String,
        genes: Vec<String>,
        taxonomy_id: i64,
        proteome_id: String,
        is_reviewed: bool,
        sequence: String,
        updated_at: i64,
    ) -> Self {
        Self {
            accession,
            secondary_accessions,
            entry_name,
            name,
            genes,
            taxonomy_id,
            proteome_id,
            is_reviewed,
            sequence,
            updated_at,
        }
    }

    /// Returns the primary accession
    ///
    pub fn get_accession(&self) -> &String {
        &self.accession
    }

    /// Returns the secondary accessions
    ///
    pub fn get_secondary_accessions(&self) -> &Vec<String> {
        &self.secondary_accessions
    }

    /// Returns the entry name
    ///
    pub fn get_entry_name(&self) -> &String {
        &self.entry_name
    }

    /// Returns the protein name
    ///
    pub fn get_name(&self) -> &String {
        &self.name
    }

    /// Returns the gene
    ///
    pub fn get_genes(&self) -> &Vec<String> {
        &self.genes
    }

    /// Returns the taxonomy ID
    ///
    pub fn get_taxonomy_id(&self) -> &i64 {
        &self.taxonomy_id
    }

    /// Returns the proteome ID
    ///
    pub fn get_proteome_id(&self) -> &String {
        &self.proteome_id
    }

    /// Returns true if the protein is reviewed (contained by SwissProt)
    ///
    pub fn get_is_reviewed(&self) -> bool {
        self.is_reviewed
    }

    /// Returns the amino acid sequence
    ///
    pub fn get_sequence(&self) -> &String {
        &self.sequence
    }

    /// Returns the last update date as unix timestamp
    ///
    pub fn get_updated_at(&self) -> i64 {
        self.updated_at
    }

    /// Checks if data has changed which results in a metadata update for for proteins
    ///
    pub fn is_peptide_metadata_changed(stored_protein: &Self, updated_protein: &Self) -> bool {
        updated_protein.get_taxonomy_id() != stored_protein.get_taxonomy_id()
            || updated_protein.get_proteome_id() != stored_protein.get_proteome_id()
            || updated_protein.get_is_reviewed() != stored_protein.get_is_reviewed()
    }
}

impl From<Row> for Protein {
    fn from(row: Row) -> Self {
        Protein {
            accession: row.get("accession"),
            secondary_accessions: row.get("secondary_accessions"),
            entry_name: row.get("entry_name"),
            name: row.get("name"),
            genes: row.get("genes"),
            taxonomy_id: row.get("taxonomy_id"),
            proteome_id: row.get("proteome_id"),
            is_reviewed: row.get("is_reviewed"),
            sequence: row.get("sequence"),
            updated_at: row.get("updated_at"),
        }
    }
}

fn get_cql_value(columns: &Vec<Option<CqlValue>>, index: usize) -> CqlValue {
    columns.get(index).unwrap().to_owned().unwrap()
}

impl From<ScyllaRow> for Protein {
    fn from(row: ScyllaRow) -> Self {
        Protein {
            accession: get_cql_value(&row.columns, 0).into_string().unwrap(),
            secondary_accessions: get_cql_value(&row.columns, 1)
                .as_list()
                .unwrap()
                .into_iter()
                .map(|cql_val| cql_val.as_text().unwrap().to_owned())
                .collect(),
            entry_name: get_cql_value(&row.columns, 2).into_string().unwrap(),
            name: get_cql_value(&row.columns, 3).into_string().unwrap(),
            genes: get_cql_value(&row.columns, 4)
                .as_list()
                .unwrap()
                .into_iter()
                .map(|cql_val| cql_val.as_text().unwrap().to_owned())
                .collect(),
            taxonomy_id: get_cql_value(&row.columns, 5).as_bigint().unwrap(),
            proteome_id: get_cql_value(&row.columns, 6).into_string().unwrap(),
            is_reviewed: get_cql_value(&row.columns, 7).as_boolean().unwrap(),
            sequence: get_cql_value(&row.columns, 8).into_string().unwrap(),
            updated_at: get_cql_value(&row.columns, 9).as_bigint().unwrap(),
        }
    }
}
