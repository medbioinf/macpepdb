use std::{fs::File, io::BufReader, num::NonZeroUsize, path::PathBuf, sync::Arc};

use thiserror::Error;
use uniprot_reader::reader::Reader as ProteinReader;

pub static INSERTED_PROTEINS_METRIC: &str = "protein_table::build::inserted_proteins";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unable to open proteins file: {0}")]
    OpenFile(#[from] std::io::Error),
    #[error("Protein error in protein table: {0}")]
    Protein(Box<crate::protein::Error>),
    #[error("Protein reader error in protein table: {0}")]
    ProteinReader(#[from] uniprot_reader::reader::Error),
}
use crate::{client::Client, protein::Protein};

pub struct ProteinTable {
    client: Arc<Client>,
}

impl ProteinTable {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    pub async fn build(
        &self,
        protein_file_paths: impl Iterator<Item = &PathBuf>,
        insert_batch_size: NonZeroUsize,
    ) -> Result<usize, Error> {
        let mut protein_ctr: usize = 0;
        let inserted_proteins_metric = metrics::counter!(INSERTED_PROTEINS_METRIC);
        let mut buffer: Vec<Protein> = Vec::with_capacity(insert_batch_size.get());
        let mut protein_id: i32 = i32::MIN;

        for protein_file_path in protein_file_paths {
            let mut buf_reader = BufReader::new(File::open(protein_file_path)?);
            let entry_reader = ProteinReader::new(&mut buf_reader);

            for entry in entry_reader {
                let protein = Protein::try_from((protein_id, entry?.entry()))
                    .map_err(|e| Error::Protein(Box::new(e)))?;
                buffer.push(protein);
                if buffer.len() == insert_batch_size.get() {
                    protein_ctr += buffer.len();
                    inserted_proteins_metric.increment(buffer.len() as u64);
                    Protein::insert_batch(self.client.as_ref(), buffer.drain(..))
                        .await
                        .map_err(|e| Error::Protein(Box::new(e)))?
                }
                protein_id += 1;
            }
        }

        if !buffer.is_empty() {
            protein_ctr += buffer.len();
            inserted_proteins_metric.increment(buffer.len() as u64);
            Protein::insert_batch(self.client.as_ref(), buffer.drain(..))
                .await
                .map_err(|e| Error::Protein(Box::new(e)))?;
        }

        Ok(protein_ctr)
    }
}
