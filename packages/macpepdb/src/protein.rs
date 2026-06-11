use thiserror::Error;

use crate::sequence::{IsBitSequence, ProteinSequence as Sequence};

static NCBI_TAXONOMY_ID_ATTRIBUTE_NAME: &str = "NCBI_TaxID=";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Taxonomy ID capture `{0}` does not contain ID group")]
    EmptyTaxonomyIdCapture(String),
    #[error("Unable find `{NCBI_TAXONOMY_ID_ATTRIBUTE_NAME}` in OX line `{0}`")]
    MissingTaxonomyIdStart(String),
    #[error("Sequence error in protein: {0}")]
    Sequence(#[from] crate::sequence::Error),
    #[error("Unable to parse taxonomy ID as intege: {0}")]
    TaxonomyIdParsing(std::num::ParseIntError),
    #[error("Row decoding error in protein: {0}")]
    Row(#[from] tokio_postgres::Error),
}

#[derive(Clone, Debug)]
pub struct Protein {
    accession: String,
    id: Option<i32>,
    sequence: Sequence,
    taxonomy_id: i32,
}

impl Protein {
    pub fn new(accession: String, id: Option<i32>, sequence: Sequence, taxonomy_id: i32) -> Self {
        Self {
            accession,
            id,
            sequence,
            taxonomy_id,
        }
    }

    pub fn accession(&self) -> &str {
        &self.accession
    }

    pub fn sequence(&self) -> &Sequence {
        &self.sequence
    }

    pub fn id(&self) -> Option<i32> {
        self.id
    }

    pub fn taxonomy_id(&self) -> i32 {
        self.taxonomy_id
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + std::mem::size_of::<String>()
            + self.accession.len()
            + self.sequence.size()
            + std::mem::size_of::<i32>() // 4 for id and taxonomy_id
    }

    /// Builds a protein from a queried row with columns
    /// `id, accession, sequence, taxonomy_id`.
    pub fn from_row(row: &tokio_postgres::Row) -> Result<Self, Error> {
        Ok(Self {
            id: Some(row.try_get("id")?),
            accession: row.try_get("accession")?,
            sequence: row.try_get("sequence")?,
            taxonomy_id: row.try_get("taxonomy_id")?,
        })
    }
}

impl TryFrom<&uniprot_reader::entry::Entry> for Protein {
    type Error = Error;

    fn try_from(entry: &uniprot_reader::entry::Entry) -> Result<Self, Error> {
        let accession = entry
            .accession()
            .find(';')
            .map(|pos| entry.accession()[..pos].trim().to_string())
            .unwrap_or(entry.accession().to_string());

        Ok(Self {
            accession,
            id: None,
            sequence: Sequence::try_from(entry.sequence())?,
            taxonomy_id: taxonomy_id_from_organism_taxonomy_cross_reference(
                entry.organism_taxonomy_cross_reference(),
            )?,
        })
    }
}

impl TryFrom<(i32, &uniprot_reader::entry::Entry)> for Protein {
    type Error = Error;

    fn try_from((id, entry): (i32, &uniprot_reader::entry::Entry)) -> Result<Self, Error> {
        let mut protein = Self::try_from(entry)?;
        protein.id = Some(id);
        Ok(protein)
    }
}

fn taxonomy_id_from_organism_taxonomy_cross_reference(
    organism_taxonomy_cross_reference: &str,
) -> Result<i32, Error> {
    let start = organism_taxonomy_cross_reference
        .find(NCBI_TAXONOMY_ID_ATTRIBUTE_NAME)
        .ok_or(Error::MissingTaxonomyIdStart(
            organism_taxonomy_cross_reference.to_string(),
        ))?
        + NCBI_TAXONOMY_ID_ATTRIBUTE_NAME.len();

    // Taxonomy can end with semicolon or whitespace,
    // e.g `OX   NCBI_TaxID=83333 {ECO:0000312|Proteomes:UP000000625};`or `OX   NCBI_TaxID=83333;`
    // just read until no numeric follows
    organism_taxonomy_cross_reference[start..]
        .chars()
        .take_while(|c| c.is_numeric())
        .collect::<String>()
        .parse()
        .map_err(Error::TaxonomyIdParsing)
}
