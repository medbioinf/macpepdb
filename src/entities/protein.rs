// 3rd party import
use serde::Deserialize;

// internal imports
use crate::entities::peptide::Peptide;

#[derive(Clone, Debug, PartialEq, Deserialize)]
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
    peptides: Vec<Peptide>,
}

impl Protein {
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

    /// Returns the peptides
    ///
    pub fn get_peptides(&self) -> &Vec<Peptide> {
        &self.peptides
    }
}
