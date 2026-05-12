use scylla::{DeserializeRow, SerializeRow};
use thiserror::Error;

use crate::sequence::ProteinSequence as Sequence;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Sequence error in protein: {0}")]
    Sequence(#[from] crate::sequence::Error),
}

#[derive(Debug, DeserializeRow, SerializeRow)]
pub struct Protein {
    accession: String,
    id: Option<i32>,
    sequence: Sequence,
}

impl Protein {
    pub fn new(accession: String, id: Option<i32>, sequence: Sequence) -> Self {
        Self {
            accession,
            id,
            sequence,
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
