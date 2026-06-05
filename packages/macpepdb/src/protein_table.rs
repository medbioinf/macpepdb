use std::{
    num::NonZeroUsize,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicI32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_compression::tokio::bufread::GzipDecoder;
use crossbeam::queue::ArrayQueue;
use futures::{StreamExt, TryStreamExt, future::join_all};
use scylla::{client::pager::TypedRowStream, errors::ExecutionError, serialize::row::SerializeRow};
use thiserror::Error;
use tokio::io::{AsyncBufRead, BufReader};
use uniprot_reader::asynchronous::reader::AsyncReader as ProteinReader;

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
    #[error("CQL unable to fetch next row: {0}")]
    CqlNextRow(#[from] scylla::errors::NextRowError),
    #[error("CQL paged execution error in protein: {0}")]
    CqlPagedExecution(#[from] scylla::errors::PagerExecutionError),
    #[error("CQL type check failed in protein: {0}")]
    CqlTypeCheck(#[from] scylla::errors::TypeCheckError),
    #[error("Unable to open proteins file: {0}")]
    OpenFile(#[from] std::io::Error),
    #[error("Protein error in protein table: {0}")]
    Protein(Box<crate::protein::Error>),
    #[error("Protein reader error in protein table on file {0}: {1}")]
    ProteinReader(PathBuf, uniprot_reader::reader::Error),
    #[error("Unable to join insertion task: {0}")]
    Join(String),
}
use crate::{client::Client, protein::Protein};

type ProteinBuildQueue = Arc<ArrayQueue<Option<Vec<Protein>>>>;

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

    pub(crate) async fn count(&self) -> Result<usize, Error> {
        self.client
            .execute_iter(format!("SELECT (TINYINT) 1 FROM {TABLE_NAME}"), ())
            .await?
            .rows_stream::<(i8,)>()?
            .try_fold(0usize, |acc, _row| async move { Ok(acc + 1) })
            .await
            .map_err(Error::from)
    }

    pub async fn build(
        &self,
        protein_file_paths: impl Iterator<Item = &PathBuf>,
        concurrent_batch_size: NonZeroUsize,
        num_insertion_threads: NonZeroUsize,
    ) -> Result<(usize, usize), Error> {
        let queue: ProteinBuildQueue = Arc::new(ArrayQueue::new(num_insertion_threads.get() * 3));
        let protein_ctr = Arc::new(AtomicUsize::new(0));
        let proteins_size = Arc::new(AtomicUsize::new(0));
        let inserted_proteins_metric = Arc::new(metrics::counter!(INSERTED_PROTEINS_METRIC));
        let protein_id = Arc::new(AtomicI32::new(i32::MIN));

        // Spawn consumer (insertion) worker tasks
        let insertion_tasks = (0..num_insertion_threads.get())
            .map(|_| {
                let client = self.client.clone();
                let queue = queue.clone();
                let protein_ctr = protein_ctr.clone();
                let proteins_size = proteins_size.clone();
                let inserted_proteins_metric = inserted_proteins_metric.clone();

                tokio::spawn(async move {
                    let protein_table = ProteinTable::new(client);
                    loop {
                        // Try to pop from queue, spin if empty
                        let batch = match queue.pop() {
                            Some(batch) => batch,
                            None => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        };

                        // None sentinel signals shutdown
                        let batch = match batch {
                            Some(batch) => batch,
                            None => break,
                        };

                        let batch_len = batch.len();
                        let batch_size: usize = batch.iter().map(|p| p.size()).sum();

                        // Insert the batch
                        protein_table.insert_batch(batch.into_iter()).await?;

                        // Update shared counters
                        protein_ctr.fetch_add(batch_len, Ordering::SeqCst);
                        proteins_size.fetch_add(batch_size, Ordering::SeqCst);
                        inserted_proteins_metric.increment(batch_len as u64);
                    }
                    Ok::<(), Error>(())
                })
            })
            .collect::<Vec<_>>();

        // Producer: read files and push batches into queue
        let mut buffer: Vec<Protein> = Vec::with_capacity(concurrent_batch_size.get());

        for protein_file_path in protein_file_paths {
            let protein_file = tokio::fs::File::open(protein_file_path).await?;
            let buf_reader = BufReader::new(protein_file);
            let mut buf_reader: Pin<Box<dyn AsyncBufRead + Send>> =
                if protein_file_path.extension().and_then(|ext| ext.to_str()) == Some("gz") {
                    Box::pin(BufReader::new(GzipDecoder::new(buf_reader)))
                } else {
                    Box::pin(buf_reader)
                };

            let mut entry_reader = ProteinReader::new(&mut buf_reader);

            while let Some(entry) = entry_reader.next().await {
                let pid = protein_id.fetch_add(1, Ordering::SeqCst);
                let protein = Protein::try_from((
                    pid,
                    entry
                        .map_err(|err| Error::ProteinReader(protein_file_path.clone(), err))?
                        .entry(),
                ))
                .map_err(|e| Error::Protein(Box::new(e)))?;
                buffer.push(protein);

                if buffer.len() == concurrent_batch_size.get() {
                    // Push batch into queue, spin if full
                    loop {
                        match queue.push(Some(std::mem::take(&mut buffer))) {
                            Ok(()) => break,
                            Err(Some(batch)) => {
                                // Queue full, check if any worker finished (error)
                                if insertion_tasks.iter().any(|t| t.is_finished()) {
                                    return Err(Self::find_errored_task(insertion_tasks).await);
                                }
                                buffer = batch;
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                            Err(None) => {
                                // A None was already in the queue (shouldn't happen in normal flow)
                                if insertion_tasks.iter().any(|t| t.is_finished()) {
                                    return Err(Self::find_errored_task(insertion_tasks).await);
                                }
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        }
                    }
                }
            }
        }

        // Push remaining buffer
        if !buffer.is_empty() {
            loop {
                match queue.push(Some(std::mem::take(&mut buffer))) {
                    Ok(()) => break,
                    Err(Some(batch)) => {
                        if insertion_tasks.iter().any(|t| t.is_finished()) {
                            return Err(Self::find_errored_task(insertion_tasks).await);
                        }
                        buffer = batch;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(None) => {
                        if insertion_tasks.iter().any(|t| t.is_finished()) {
                            return Err(Self::find_errored_task(insertion_tasks).await);
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }

        // Send shutdown sentinel (None) to each worker
        for _ in 0..num_insertion_threads.get() {
            loop {
                if queue.push(None).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // Await all insertion tasks and collect errors
        for task in insertion_tasks {
            task.await.map_err(|e| Error::Join(e.to_string()))??;
        }

        Ok((
            protein_ctr.load(Ordering::SeqCst),
            proteins_size.load(Ordering::SeqCst),
        ))
    }

    async fn find_errored_task(tasks: Vec<tokio::task::JoinHandle<Result<(), Error>>>) -> Error {
        for task in tasks {
            if task.is_finished() {
                match task.await {
                    Ok(Ok(())) => continue,
                    Ok(Err(err)) => return err,
                    Err(e) => return Error::Join(e.to_string()),
                }
            }
        }
        Error::Join("No errored task found, but one finished early".to_string())
    }
}
