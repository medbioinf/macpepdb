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
use futures::{Stream, StreamExt};
use postgres_types::{ToSql, Type};
use thiserror::Error;
use tokio::io::{AsyncBufRead, BufReader};
use uniprot_reader::asynchronous::reader::AsyncReader as ProteinReader;

use crate::{client::Client, protein::Protein, stats_table::StatsTable};

static TABLE_NAME: &str = "proteins";

static INSERT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "INSERT INTO {TABLE_NAME} (id, accession, sequence, taxonomy_id) VALUES ($1, $2, $3, $4)"
    )
});

static COPY_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!("COPY {TABLE_NAME} (id, accession, sequence, taxonomy_id) FROM STDIN (FORMAT binary)")
});

/// Column types for the binary COPY into `proteins`, in column order.
static COPY_TYPES: LazyLock<[Type; 4]> =
    LazyLock::new(|| [Type::INT4, Type::TEXT, Type::BYTEA, Type::INT4]);

static SELECT_ALL_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT id, accession, sequence, taxonomy_id FROM {TABLE_NAME}"));

static SELECT_BY_IDS_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!("SELECT id, accession, sequence, taxonomy_id FROM {TABLE_NAME} WHERE id = ANY($1)")
});

static SELECT_ID_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT id FROM {TABLE_NAME}"));

pub static INSERTED_PROTEINS_METRIC: &str = "protein_table::build::inserted_proteins";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in protein: {0}")]
    Client(#[from] crate::client::Error),
    #[error(
        "Protein count not found. It should be stored in the `{}` table. Are you sure the database was build correctly?",
        StatsTable::table_name()
    )]
    CountNotFound,
    #[error("Row decoding error in protein: {0}")]
    Row(#[from] tokio_postgres::Error),
    #[error("Unable to open proteins file: {0}")]
    OpenFile(#[from] std::io::Error),
    #[error("Protein error in protein table: {0}")]
    Protein(Box<crate::protein::Error>),
    #[error("Protein reader error in protein table on file {0}: {1}")]
    ProteinReader(PathBuf, uniprot_reader::reader::Error),
    #[error("Stats table error in protein table: {0}")]
    StatsTable(Box<crate::stats_table::Error>),
    #[error("Unable to join insertion task: {0}")]
    Join(String),
}

into_thiserror_boxed!(crate::stats_table::Error, Error, StatsTable);
into_thiserror_boxed!(crate::protein::Error, Error, Protein);

type ProteinBuildQueue = Arc<ArrayQueue<Option<Vec<Protein>>>>;
type ProteinFilePathBuildQueue = Arc<ArrayQueue<Option<PathBuf>>>;

pub struct ProteinTable {
    client: Arc<Client>,
}

impl ProteinTable {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    pub async fn insert(&self, protein: &Protein) -> Result<(), Error> {
        let id = protein.id();
        let accession = protein.accession();
        let taxonomy_id = protein.taxonomy_id();
        self.client
            .execute(
                INSERT_STATEMENT.as_str(),
                &[&id, &accession, protein.sequence(), &taxonomy_id],
            )
            .await?;
        Ok(())
    }

    /// Bulk-loads a batch of proteins via a single binary COPY transaction.
    pub async fn insert_batch(&self, values: impl Iterator<Item = Protein>) -> Result<(), Error> {
        let values: Vec<Protein> = values.collect();
        if values.is_empty() {
            return Ok(());
        }

        self.client
            .run_congested(|| async {
                let mut copy = self
                    .client
                    .copy_in_binary(COPY_STATEMENT.as_str(), COPY_TYPES.as_slice())
                    .await?;
                for protein in &values {
                    let id = protein.id();
                    let accession = protein.accession();
                    let taxonomy_id = protein.taxonomy_id();
                    copy.write(&[&id, &accession, protein.sequence(), &taxonomy_id])
                        .await?;
                }
                copy.finish().await?;
                Ok::<(), crate::client::Error>(())
            })
            .await?;

        Ok(())
    }

    /// Streams every protein (`SELECT id, accession, sequence, taxonomy_id FROM proteins`).
    pub async fn select_all(
        &self,
    ) -> Result<impl Stream<Item = Result<Protein, Error>> + Send + use<>, Error> {
        let stream = self
            .client
            .query_stream(SELECT_ALL_STATEMENT.as_str(), Vec::new())
            .await?;
        Ok(stream.map(|row_res| {
            row_res
                .map_err(Error::Row)
                .and_then(|row| Protein::try_from(row).map_err(Error::from))
        }))
    }

    /// Streams the proteins with the given ids (`WHERE id = ANY($1)`).
    pub async fn select_by_ids(
        &self,
        ids: &[i32],
    ) -> Result<impl Stream<Item = Result<Protein, Error>> + Send + use<>, Error> {
        let params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(ids.to_vec())];
        let stream = self
            .client
            .query_stream(SELECT_BY_IDS_STATEMENT.as_str(), params)
            .await?;
        Ok(stream.map(|row_res| {
            row_res
                .map_err(Error::Row)
                .and_then(|row| Protein::try_from(row).map_err(Error::from))
        }))
    }

    pub async fn count(&self) -> Result<usize, Error> {
        StatsTable::new(self.client.clone())
            .select_protein_count()
            .await
            .map_err(Error::from)?
            .ok_or(Error::CountNotFound)
    }

    pub async fn select_ids(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<i32, Error>> + Send>>, Error> {
        let stream = self
            .client
            .query_stream(SELECT_ID_STATEMENT.as_str(), Vec::new())
            .await?;
        Ok(stream
            .map(|row_res| {
                let row = row_res.map_err(Error::from)?;
                row.try_get::<_, i32>(0).map_err(Error::from)
            })
            .boxed())
    }

    pub async fn build(
        &self,
        protein_file_paths: impl Iterator<Item = &PathBuf>,
        concurrent_batch_size: NonZeroUsize,
        num_insertion_threads: NonZeroUsize,
    ) -> Result<(usize, usize), Error> {
        let proteins_file_path_queue: ProteinFilePathBuildQueue =
            Arc::new(ArrayQueue::new(num_insertion_threads.get() * 3));
        let protein_queue: ProteinBuildQueue =
            Arc::new(ArrayQueue::new(num_insertion_threads.get() * 3));
        let protein_ctr = Arc::new(AtomicUsize::new(0));
        let proteins_size = Arc::new(AtomicUsize::new(0));
        let inserted_proteins_metric = Arc::new(metrics::counter!(INSERTED_PROTEINS_METRIC));
        let protein_id = Arc::new(AtomicI32::new(i32::MIN));

        let num_read_tasks = std::cmp::min(
            num_insertion_threads.get(),
            protein_file_paths
                .size_hint()
                .1
                .unwrap_or(num_insertion_threads.get()),
        );
        tracing::info!(
            "Spawning {num_read_tasks} read tasks and {} insertion tasks",
            num_insertion_threads.get()
        );
        let read_tasks = (0..num_read_tasks)
            .map(|_| {
                let proteins_file_path_queue = proteins_file_path_queue.clone();
                let protein_queue = protein_queue.clone();
                let protein_id = protein_id.clone();

                tokio::spawn(async move {
                    // Producer: read files and push batches into queue
                    let mut buffer: Vec<Protein> = Vec::with_capacity(concurrent_batch_size.get());

                    loop {
                        let protein_file_path = match proteins_file_path_queue.pop() {
                            Some(path) => path,
                            None => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        };

                        let protein_file_path = match protein_file_path {
                            Some(path) => path,
                            None => break,
                        };

                        tracing::info!("processing file: {:?}", protein_file_path);

                        let protein_file = tokio::fs::File::open(&protein_file_path).await?;
                        let buf_reader = BufReader::new(protein_file);
                        let mut buf_reader: Pin<Box<dyn AsyncBufRead + Send>> =
                            if protein_file_path.extension().and_then(|ext| ext.to_str())
                                == Some("gz")
                            {
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
                                    .map_err(|err| {
                                        Error::ProteinReader(protein_file_path.clone(), err)
                                    })?
                                    .entry(),
                            ))
                            .map_err(|e| Error::Protein(Box::new(e)))?;
                            buffer.push(protein);

                            if buffer.len() == concurrent_batch_size.get() {
                                // Push batch into queue, spin if full
                                loop {
                                    match protein_queue.push(Some(std::mem::take(&mut buffer))) {
                                        Ok(()) => break,
                                        Err(Some(batch)) => {
                                            buffer = batch;
                                            tokio::time::sleep(Duration::from_millis(50)).await;
                                        }
                                        Err(None) => {
                                            // A None was already in the queue (shouldn't happen in normal flow)
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
                            match protein_queue.push(Some(std::mem::take(&mut buffer))) {
                                Ok(()) => break,
                                Err(Some(batch)) => {
                                    buffer = batch;
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                }
                                Err(None) => {
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                }
                            }
                        }
                    }

                    Ok::<(), Error>(())
                })
            })
            .collect::<Vec<_>>();

        // Spawn consumer (insertion) worker tasks
        let insertion_tasks = (0..num_insertion_threads.get())
            .map(|_| {
                let client = self.client.clone();
                let protein_queue = protein_queue.clone();
                let protein_ctr = protein_ctr.clone();
                let proteins_size = proteins_size.clone();
                let inserted_proteins_metric = inserted_proteins_metric.clone();

                tokio::spawn(async move {
                    let protein_table = ProteinTable::new(client);
                    loop {
                        // Try to pop from protein_queue, spin if empty
                        let batch = match protein_queue.pop() {
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

        for protein_file_path in protein_file_paths {
            loop {
                match proteins_file_path_queue.push(Some(protein_file_path.clone())) {
                    Ok(()) => break,
                    Err(_) => {
                        // Queue full, check if any worker finished (error)
                        if read_tasks.iter().any(|t| t.is_finished()) {
                            return Err(Self::find_errored_task(read_tasks).await);
                        }
                        if insertion_tasks.iter().any(|t| t.is_finished()) {
                            return Err(Self::find_errored_task(insertion_tasks).await);
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }

        // Send shutdown sentinel (None) to each  read worker
        for _ in 0..num_insertion_threads.get() {
            loop {
                if proteins_file_path_queue.push(None).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // Await all read tasks and collect errors
        for task in read_tasks {
            task.await.map_err(|e| Error::Join(e.to_string()))??;
        }

        // Send shutdown sentinel (None) to each  read worker
        for _ in 0..num_insertion_threads.get() {
            loop {
                if protein_queue.push(None).is_ok() {
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
