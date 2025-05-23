// std imports
use std::cmp::min;
use std::sync::Arc;

// 3rd party imports
use anyhow::{bail, Result};
use chrono::DateTime;
use dihardts_omicstools::proteomics::proteases::protease::Protease;
use fallible_iterator::FallibleIterator;
use futures::TryStreamExt;
use scylla::{DeserializeValue, SerializeValue};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};

// internal imports
use crate::database::scylla::client::Client;
use crate::tools::message_logger::ToLogMessage;
use crate::{database::scylla::peptide_table::PeptideTable, entities::domain::Domain};

use super::peptide::Peptide;

#[derive(Debug, Clone, PartialEq, DeserializeValue, SerializeValue, Serialize)]
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
    domains: Vec<Domain>,
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
    #[allow(clippy::too_many_arguments)]
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
        domains: Vec<Domain>,
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
            domains,
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

    pub fn get_domains(&self) -> &Vec<Domain> {
        &self.domains
    }

    pub fn get_all_accessions(&self) -> Vec<&String> {
        let mut accessions = vec![self.get_accession()];
        accessions.extend(self.get_secondary_accessions().as_slice());
        accessions
    }

    /// Checks if data has changed which results in a metadata update for for proteins
    ///
    pub fn is_peptide_metadata_changed(stored_protein: &Self, updated_protein: &Self) -> bool {
        updated_protein.get_taxonomy_id() != stored_protein.get_taxonomy_id()
            || updated_protein.get_proteome_id() != stored_protein.get_proteome_id()
            || updated_protein.get_is_reviewed() != stored_protein.get_is_reviewed()
    }

    /// Creates UniProt-txt-file entry of this protein
    /// See <https://web.expasy.org/docs/userman.html> for more information
    ///
    pub fn to_uniprot_txt_entry(&self) -> Result<String> {
        let mut entry = String::new();

        // ID
        entry.push_str(&format!(
            "ID   {}   {};   {}AA.\n",
            self.get_entry_name(),
            if self.get_is_reviewed() {
                "Reviewed"
            } else {
                "Unreviewed"
            },
            self.get_sequence().len()
        ));

        // Accessions
        let accessions = self.get_all_accessions();
        for start in (0..accessions.len()).step_by(9) {
            let end = min(start + 9, accessions.len());
            entry.push_str(&format!(
                "AC   {};\n",
                accessions[start..end]
                    .iter()
                    .map(|acc| acc.as_str())
                    .collect::<Vec<&str>>()
                    .join("; ")
            ));
        }

        // Date
        let date = match DateTime::from_timestamp(self.get_updated_at(), 0) {
            Some(date) => date.format("%d-%b-%Y"),
            None => {
                bail!(
                    "timestamp could not be converted to DateTime from timestamp {}",
                    self.get_updated_at(),
                )
            }
        };
        entry.push_str(&format!("DT   {}, unprocessable.\n", date));

        // Name
        entry.push_str(&format!("DE   RecName: Full={};\n", self.get_name()));

        // Proteome ID

        entry.push_str(&format!(
            "DR   Proteomes; {}; unprocessable.\n",
            self.get_proteome_id()
        ));

        // Genes
        let genes = self.get_genes();
        if !genes.is_empty() {
            entry.push_str(&format!("GN   Name={};\n", genes[0]));
        }
        if genes.len() > 1 {
            for start in (1..genes.len()).step_by(9) {
                let end = min(start + 9, genes.len());
                entry.push_str(&format!(
                    "GN   Synonyms={};\n",
                    genes[start..end].join("; ")
                ));
            }
        }

        // Taxonomy
        entry.push_str(&format!("OX   NCBI_TaxID={};\n", self.get_taxonomy_id()));

        // Sequence
        entry.push_str(&format!(
            "SQ   SEQUENCE   {} AA;;\n",
            self.get_sequence().len()
        ));

        // Chunk sequence into 10 amino acid blocks
        let seq_blocks = self
            .get_sequence()
            .as_bytes()
            .chunks(10)
            .map(|chunk| String::from_utf8(chunk.to_vec()))
            .collect::<Result<Vec<String>, _>>()?;

        // Write 6 blocks per line
        for start in (0..seq_blocks.len()).step_by(6) {
            let stop = min(start + 6, seq_blocks.len());
            entry.push_str(&format!("     {}\n", seq_blocks[start..stop].join(" ")));
        }

        entry.push_str("//");

        Ok(entry)
    }

    /// Creates a JSON value of this protein including it's peptides
    /// As the peptides are not stored in the database, the protein sequence needs to be digested
    /// using the given protease.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `partition_limits` - The mass partition limits
    /// * `protease` - The protease used to generate the peptides
    ///
    pub async fn to_json_with_peptides(
        &self,
        client: Arc<Client>,
        partition_limits: &[i64],
        protease: &dyn Protease,
        include_protein_accessions: bool,
    ) -> Result<JsonValue> {
        let mut peptides: Vec<Peptide> =
            PeptideTable::get_peptides_of_proteins(client, self, protease, partition_limits)
                .await?
                .try_collect()
                .await?;

        peptides.sort_by(|pep_x, pep_y| pep_x.get_mass().partial_cmp(&pep_y.get_mass()).unwrap());

        if !include_protein_accessions {
            peptides = peptides
                .into_iter()
                .map(|pep| pep.into_proteinless_peptide())
                .collect();
        }

        let mut protein_json: JsonValue = serde_json::to_value(self)?;
        protein_json["peptides"] = serde_json::to_value(peptides)?;
        Ok(protein_json)
    }

    /// Creates a JSON value of this protein including it's peptides sequences
    /// As the peptides are not stored in the database, the protein sequence needs to be digested
    /// using the given protease.
    ///
    /// # Arguments
    /// * `protease` - The protease used to generate the peptides
    ///
    pub fn to_json_with_peptide_sequences(&self, protease: &dyn Protease) -> Result<JsonValue> {
        let peptides: Vec<String> = protease
            .cleave(self.get_sequence())?
            .map(|pep| Ok(pep.get_sequence().to_owned()))
            .collect()?;

        let mut protein_json: JsonValue = serde_json::to_value(self)?;
        protein_json["peptides"] = serde_json::to_value(peptides)?;
        Ok(protein_json)
    }

    /// Creates a JSON value of this protein with an empty peptides attribute
    /// for faster loading
    ///
    /// # Arguments
    /// * `protease` - The protease used to generate the peptides
    ///
    pub fn to_json_without_peptides(&self) -> Result<JsonValue> {
        let mut protein_json: JsonValue = serde_json::to_value(self)?;
        protein_json["peptides"] = json!([]);
        Ok(protein_json)
    }
}

impl ToLogMessage for Protein {
    fn to_message(&self) -> Result<Vec<u8>> {
        Ok(self.to_uniprot_txt_entry()?.as_bytes().to_vec())
    }
}

#[cfg(test)]
mod test {
    // std imports
    use std::env;
    use std::fs::{remove_file, File};
    use std::io::Write;
    use std::path::Path;

    // 3rd party imports
    use fallible_iterator::FallibleIterator;

    // internal imports
    use crate::io::uniprot_text::reader::Reader;

    /// Check if the protein can be converted to a UniProt-txt-file entry and back
    ///
    #[test]
    fn test_protein_to_uniprot_txt_entry() {
        // Read test protein
        let mut reader = Reader::new(Path::new("test_files/leptin.txt"), 1024).unwrap();
        let protein = reader.next().unwrap().unwrap();

        // Create entry
        let entry = protein.to_uniprot_txt_entry().unwrap();

        // Wirte entry to temp file
        let temp_dir = env::temp_dir();
        let temp_file = temp_dir.join("test_protein_to_uniprot_txt_entry.txt");
        let mut file = File::create(&temp_file).unwrap();
        file.write_all(entry.as_bytes()).unwrap();
        drop(file);

        // read temp file
        let mut reader = Reader::new(&temp_file, 1024).unwrap();
        let reread_protein = reader.next().unwrap().unwrap();

        // Compare proteins
        assert_eq!(protein, reread_protein);

        remove_file(&temp_file).unwrap();
    }
}
