use std::{
    fs::File,
    io::{BufRead, BufReader},
    num::NonZeroUsize,
    path::PathBuf,
    sync::{Arc, LazyLock},
};

use flate2::read::GzDecoder;
use futures::future::join_all;
use scylla::{client::pager::TypedRowStream, errors::ExecutionError, serialize::row::SerializeRow};
use thiserror::Error;
use uniprot_reader::reader::Reader as ProteinReader;

static TABLE_NAME: &str = "proteins";

static INSERT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!("INSERT INTO {TABLE_NAME} (accession, id, sequence, taxonomy_id) VALUES (?, ?, ?, ?)")
});

static SELECT_STATEMENT: LazyLock<String> = LazyLock::new(|| format!("SELECT * FROM {TABLE_NAME}"));

pub static INSERTED_PROTEINS_METRIC: &str = "protein_table::build::inserted_proteins";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in protein: {0}")]
    Client(#[from] crate::client::Error),
    #[error("CQL execution error in protein: {0}")]
    CqlExecution(#[from] scylla::errors::ExecutionError),
    #[error("CQL paged execution error in protein: {0}")]
    CqlPagedExecution(#[from] scylla::errors::PagerExecutionError),
    #[error("CQL type check failed in protein: {0}")]
    CqlTypeCheck(#[from] scylla::errors::TypeCheckError),
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

    pub async fn insert(&self, protein: &Protein) -> Result<(), Error> {
        self.client
            .execute_unpaged(INSERT_STATEMENT.as_str(), protein)
            .await?;
        Ok(())
    }

    pub async fn insert_batch(&self, values: impl Iterator<Item = Protein>) -> Result<(), Error> {
        let insert_futures = values.map(|value| {
            self.client
                .execute_unpaged(INSERT_STATEMENT.as_str(), value)
        });

        join_all(insert_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, ExecutionError>>()?;

        Ok(())
    }

    pub async fn select(
        &self,
        select_addition: Option<&str>,
        values: impl SerializeRow,
    ) -> Result<TypedRowStream<Protein>, Error> {
        let statement = select_addition
            .map(|addition| format!("{} {}", SELECT_STATEMENT.as_str(), addition))
            .unwrap_or_else(|| SELECT_STATEMENT.as_str().to_string());

        Ok(self
            .client
            .execute_iter(statement, values)
            .await?
            .rows_stream::<Protein>()?)
    }

    pub async fn build(
        &self,
        protein_file_paths: impl Iterator<Item = &PathBuf>,
        concurrent_batch_size: NonZeroUsize,
    ) -> Result<(usize, usize), Error> {
        let mut protein_ctr: usize = 0;
        let mut proteins_size: usize = 0;
        let inserted_proteins_metric = metrics::counter!(INSERTED_PROTEINS_METRIC);
        let mut buffer: Vec<Protein> = Vec::with_capacity(concurrent_batch_size.get());
        let mut protein_id: i32 = i32::MIN;

        for protein_file_path in protein_file_paths {
            let protein_file = File::open(protein_file_path)?;

            let mut buf_reader: Box<dyn BufRead> =
                if protein_file_path.extension().and_then(|ext| ext.to_str()) == Some("gz") {
                    Box::new(BufReader::new(GzDecoder::new(protein_file)))
                } else {
                    Box::new(BufReader::new(protein_file))
                };

            let entry_reader = ProteinReader::new(&mut buf_reader);

            for entry in entry_reader {
                let protein = Protein::try_from((protein_id, entry?.entry()))
                    .map_err(|e| Error::Protein(Box::new(e)))?;
                proteins_size += protein.size();
                buffer.push(protein);
                if buffer.len() == concurrent_batch_size.get() {
                    protein_ctr += buffer.len();
                    inserted_proteins_metric.increment(buffer.len() as u64);
                    self.insert_batch(buffer.drain(..)).await?
                }
                protein_id += 1;
            }
        }

        if !buffer.is_empty() {
            protein_ctr += buffer.len();
            inserted_proteins_metric.increment(buffer.len() as u64);
            self.insert_batch(buffer.drain(..)).await?;
        }

        Ok((protein_ctr, proteins_size))
    }
}
