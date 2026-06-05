use std::{
    collections::{HashMap, HashSet},
    iter::Peekable,
    num::NonZeroUsize,
    ops::AddAssign,
    sync::{Arc, LazyLock, atomic::AtomicI64},
    time::Duration,
};

use crossbeam::queue::ArrayQueue;
use fallible_iterator::FallibleIterator;
use futures::StreamExt;
use metrics::Counter;
use scylla::{
    client::pager::TypedRowStream,
    serialize::batch::BatchValuesFromIterator,
    statement::batch::{Batch, BatchType},
};
use thiserror::Error;

use crate::{
    client::Client,
    database_build::IsProteinAccess,
    mass_index::MassIndex,
    peptide::Peptide,
    protease::Protease,
    sequence::{CompactSequence, PeptideSequence},
};

pub const TABLE_NAME: &str = "peptides";

static INSERT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "INSERT INTO {TABLE_NAME} (partition, mass, sequence, protein_ids, unique_taxonomy_ids, non_unique_taxonomy_ids) VALUES (?, ?, ?, ?, ?, ?)"
    )
});

static SELECT_STATEMENT: LazyLock<String> = LazyLock::new(|| format!("SELECT * FROM {TABLE_NAME}"));

pub static PROGRESS_METRIC: &str = "peptides_table::build::progress";
pub static INSERTED_PEPTIDES_METRIC: &str = "peptides_table::build::inserted_peptides";
pub static QUEUE_METRIC: &str = "peptides_table::queue";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in peptide table: {0}")]
    Client(#[from] crate::client::Error),
    #[error("CQL execution error in peptide table: {0}")]
    CqlExecution(Box<scylla::errors::ExecutionError>),
    #[error("CQL paged execution error in peptide table: {0}")]
    CqlPagedExecution(Box<scylla::errors::PagerExecutionError>),
    #[error("CQL type check failed in peptide table: {0}")]
    CqlTypeCheck(#[from] scylla::errors::TypeCheckError),
    #[error("CQL next row error in peptide table: {0}")]
    CqlNextRow(#[from] scylla::errors::NextRowError),
    // #[error("Indexing stopped unexpectedly before finishing the protein processing ")]
    // EarlyIndexThreadStop,
    #[error("IO error in peptide table: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unable to join insertion task: {0}")]
    Join(String),
    #[error("Mass index error in peptide table: {0}")]
    MassIndex(Box<crate::mass_index::Error>),
    #[error("No errored thread found in peptide table, but one finished early.")]
    NoErroredThread,
    #[error("Protease error in peptide table: {0}")]
    Protease(#[from] crate::protease::Error),
    #[error("Protein access error in peptide table: {0}")]
    ProteinAccess(Box<crate::database_build::Error>),
    #[error("Peptide error in peptide table: {0}")]
    Peptide(#[from] crate::peptide::Error),
    #[error("Sequence error in peptide table: {0}")]
    Sequence(#[from] crate::sequence::Error),
    // #[error("Protein reader thread error: {0}")]
    // ProteinReaderThread(String),
    #[error("UnipotReader error in peptide table: {0}")]
    UnprotReader(#[from] uniprot_reader::reader::Error),
}

into_thiserror_boxed!(scylla::errors::ExecutionError, Error, CqlExecution);
into_thiserror_boxed!(
    scylla::errors::PagerExecutionError,
    Error,
    CqlPagedExecution
);
into_thiserror_boxed!(crate::mass_index::Error, Error, MassIndex);
into_thiserror_boxed!(crate::database_build::Error, Error, ProteinAccess);

type ConcurrentlyBuildQueue = Arc<ArrayQueue<Option<(i64, HashSet<i32>)>>>;

struct NextPartitionGuard {
    next_partition: AtomicI64,
}

impl NextPartitionGuard {
    fn new() -> Self {
        Self {
            next_partition: AtomicI64::new(0),
        }
    }

    fn next_partition(&self) -> i64 {
        self.next_partition
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

pub struct PeptideTable {
    client: Arc<Client>,
}

impl PeptideTable {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    pub async fn insert(&self, peptide: &Peptide) -> Result<(), Error> {
        self.client
            .run_congested(|| {
                self.client
                    .execute_unpaged(INSERT_STATEMENT.as_str(), peptide)
            })
            .await?;
        Ok(())
    }

    async fn insert_batch(
        &self,
        peptides: Peekable<impl Iterator<Item = Peptide>>,
        batch_size_limit: NonZeroUsize,
        inserted_peptides_metric: Arc<Counter>,
    ) -> Result<(), Error> {
        let mut peptide_buffer_cql_size = 0;
        let mut peptide_buffer: Vec<Peptide> = Vec::new();

        let mut peptides = peptides.into_iter().peekable();

        while let Some(peptide) = peptides.next() {
            peptide_buffer_cql_size += peptide.cql_size();
            peptide_buffer.push(peptide);

            if let Some(next_peptide) = peptides.peek()
                && peptide_buffer.len() < 100
                && peptide_buffer_cql_size + next_peptide.cql_size() < batch_size_limit.get() * 1000
            {
                continue;
            }

            let peptide_buffer_len = peptide_buffer.len();
            let mut batch_statement = Batch::new(BatchType::Unlogged);
            (0..peptide_buffer_len).for_each(|_| {
                batch_statement.append_statement(INSERT_STATEMENT.as_str());
            });
            if let Some(consistency) = self.client.write_consistency_level() {
                batch_statement.set_consistency(consistency);
            }
            self.client
                .run_congested(|| {
                    self.client.batch(
                        &batch_statement,
                        BatchValuesFromIterator::new(peptide_buffer.iter()),
                    )
                })
                .await?;

            peptide_buffer_cql_size = 0;
            peptide_buffer.clear();
            inserted_peptides_metric.increment(peptide_buffer_len as u64);
        }

        Ok(())
    }

    pub async fn select(
        &self,
        select_addition: Option<&str>,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<TypedRowStream<Peptide>, Error> {
        let statement = select_addition
            .map(|addition| format!("{} {}", SELECT_STATEMENT.as_str(), addition))
            .unwrap_or_else(|| SELECT_STATEMENT.as_str().to_string());

        Ok(self
            .client
            .execute_iter(statement, values)
            .await?
            .rows_stream::<Peptide>()?)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn build_concurrently(
        &self,
        protein_access: Arc<Box<dyn IsProteinAccess>>,
        skip_protein_associations: bool,
        skip_taxonomies: bool,
        protease: Arc<Protease>,
        batch_size_limit: NonZeroUsize,
        num_threads: NonZeroUsize,
        mass_index: MassIndex,
    ) -> Result<HashMap<i64, Vec<i64>>, Error> {
        let queue: ConcurrentlyBuildQueue = Arc::new(ArrayQueue::new(num_threads.get() * 3));
        let progress_metric = Arc::new(metrics::gauge!(PROGRESS_METRIC));
        let inserted_peptides_metric = Arc::new(metrics::counter!(INSERTED_PEPTIDES_METRIC));
        let queue_metric = metrics::gauge!(QUEUE_METRIC);
        let next_partition_guard = Arc::new(NextPartitionGuard::new());

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let protein_access = protein_access.clone();
                let protease = protease.clone();
                let queue = queue.clone();
                let client = self.client.clone();
                let progress_metric = progress_metric.clone();
                let inserted_peptides_metric = inserted_peptides_metric.clone();
                let next_partition_guard = next_partition_guard.clone();

                tokio::spawn(async move {
                    let peptide_table = PeptideTable::new(client.clone());
                    let mut mass_partition_map: HashMap<i64, Vec<i64>> = HashMap::new();
                    let mut peptide_buffer: Vec<Peptide> = Vec::new();
                    let mut partition_cql_size: usize = 0;
                    let mut batch_cql_size: usize = 0;
                    let mut partition = next_partition_guard.next_partition();

                    // Track which masses have peptides in the current buffer
                    let mut buffer_masses: HashSet<i64> = HashSet::new();

                    loop {
                        let (mass, protein_ids) = match queue.pop() {
                            Some(Some(entry)) => entry,
                            Some(None) => {
                                if !peptide_buffer.is_empty() {
                                    for &m in &buffer_masses {
                                        mass_partition_map.entry(m).or_default().push(partition);
                                    }
                                    tracing::debug!(
                                        "Inserting {} peptides into partition {partition}; partition size {}/{}",
                                        peptide_buffer.len(),
                                        partition_cql_size as f32 / 1000.0 / 1000.0,
                                        crate::cql::MAX_PARTITION_SIZE as f32 / 1000.0 / 1000.0,
                                    );
                                    peptide_table
                                        .insert_batch(
                                            peptide_buffer.drain(..).peekable(),
                                            batch_size_limit,
                                            inserted_peptides_metric.clone(),
                                        )
                                        .await?;
                                }
                                break;
                            }
                            None => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        };

                        // Flush buffer if it's large enough before processing new mass
                        if batch_cql_size >= batch_size_limit.get() * 1000 {
                            for &m in &buffer_masses {
                                mass_partition_map.entry(m).or_default().push(partition);
                            }
                            tracing::debug!(
                                "Inserting {} peptides into partition {partition}; partition size {}/{}",
                                peptide_buffer.len(),
                                partition_cql_size as f32 / 1000.0 / 1000.0,
                                crate::cql::MAX_PARTITION_SIZE as f32 / 1000.0 / 1000.0,
                            );
                            peptide_table
                                .insert_batch(
                                    peptide_buffer.drain(..).peekable(),
                                    batch_size_limit,
                                    inserted_peptides_metric.clone(),
                                )
                                .await?;
                            batch_cql_size = 0;
                            buffer_masses.clear();
                        }

                        let protein_ids = Vec::from_iter(protein_ids);

                        let protein_ids_len = protein_ids.len();

                        let mut proteins = protein_access.by_ids(&protein_ids).await?;

                        let protein_ids = if skip_protein_associations {
                            Vec::new()
                        } else {
                            protein_ids.clone()
                        };

                        // Using the more compact form of the sequence to keep the peptide in memory as small as possible, mass is not important now.
                        let mut peptide_sequences: HashMap<CompactSequence, HashMap<i32, usize>> =
                            HashMap::with_capacity(2 * protein_ids_len);

                        while let Some(protein) = proteins.next().await.transpose()? {
                            #[allow(clippy::mutable_key_type)]
                            protease
                                .cleave(protein.sequence().as_ref(), Some(mass..=mass))
                                .for_each(|peptide| {
                                    peptide_sequences
                                        .entry(CompactSequence::try_from(peptide.into_sequence())?)
                                        .or_default()
                                        .entry(protein.taxonomy_id())
                                        .or_insert(0)
                                        .add_assign(1);
                                    Ok(())
                                })?;
                        }

                        let peptides: Vec<Peptide> = peptide_sequences
                            .into_iter()
                            .map(|(seq, taxonomies)| {
                                let unique_taxonomy_ids = taxonomies
                                    .iter()
                                    .filter(|(_, count)| **count == 1 && !skip_taxonomies)
                                    .map(|(taxonomy_id, _)| *taxonomy_id)
                                    .collect::<Vec<_>>();

                                let non_unique_taxonomy_ids = taxonomies
                                    .into_iter()
                                    .filter(|(_, count)| *count > 1 && !skip_taxonomies)
                                    .map(|(taxonomy_id, _)| taxonomy_id)
                                    .collect::<Vec<_>>();

                                Ok::<_, Error>(Peptide::new(
                                    PeptideSequence::try_from(seq).map_err(Error::Sequence)?,
                                    protein_ids.clone(),
                                    unique_taxonomy_ids,
                                    non_unique_taxonomy_ids,
                                ))
                            })
                            .collect::<Result<_, _>>()?;

                        for mut peptide in peptides {
                            // If adding this peptide would exceed partition size, flush and start new partition
                            if !peptide_buffer.is_empty()
                                && partition_cql_size + peptide.cql_size()
                                    >= crate::cql::MAX_PARTITION_SIZE
                            {
                                for &m in &buffer_masses {
                                    mass_partition_map.entry(m).or_default().push(partition);
                                }
                                tracing::debug!(
                                    "Partition full: {} peptides into partition {partition}; cql size {}/{}",
                                    peptide_buffer.len(),
                                    partition_cql_size as f32 / 1000.0 / 1000.0,
                                    crate::cql::MAX_PARTITION_SIZE as f32 / 1000.0 / 1000.0,
                                );
                                peptide_table
                                    .insert_batch(
                                        peptide_buffer.drain(..).peekable(),
                                        batch_size_limit,
                                        inserted_peptides_metric.clone(),
                                    )
                                    .await?;
                                batch_cql_size = 0;
                                partition_cql_size = 0;
                                buffer_masses.clear();
                                partition = next_partition_guard.next_partition();
                            }

                            let cql_size = peptide.cql_size();
                            peptide.set_partition(partition);
                            peptide_buffer.push(peptide);
                            partition_cql_size += cql_size;
                            batch_cql_size += cql_size;
                            buffer_masses.insert(mass);
                        }

                        progress_metric.increment(1);
                    }
                    Ok::<_, Error>(mass_partition_map)
                })
            })
            .collect::<Vec<_>>();

        for mass_index_entry in mass_index.into_iter() {
            let mut mass_index_entry = Some(mass_index_entry);
            loop {
                mass_index_entry = match queue.push(mass_index_entry) {
                    Ok(()) => break,
                    Err(entry) => {
                        // check if all threads still running
                        if digest_and_insertion_threads
                            .iter()
                            .any(|thread| thread.is_finished())
                        {
                            // find errored_thread and return error
                            return Err(
                                Self::find_errored_thread(digest_and_insertion_threads).await
                            );
                        }
                        entry
                    }
                };
            }
            queue_metric.set(queue.len() as f64);
        }

        // Send none to signal stop
        for _ in 0..num_threads.get() {
            loop {
                if queue.push(None).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        let mut final_mass_to_partitions_map: HashMap<i64, Vec<i64>> = HashMap::new();

        for thread in digest_and_insertion_threads {
            match thread.await.map_err(|err| Error::Join(err.to_string()))? {
                Ok(map) => final_mass_to_partitions_map.extend(map),
                Err(err) => return Err(err),
            }
        }

        Ok(final_mass_to_partitions_map)
    }

    #[allow(clippy::type_complexity)]
    async fn find_errored_thread(
        threads: Vec<tokio::task::JoinHandle<Result<HashMap<i64, Vec<i64>>, Error>>>,
    ) -> Error {
        for thread in threads {
            if thread.is_finished() {
                match thread.await {
                    Ok(Ok(_)) => continue,
                    Ok(Err(err)) => return err,
                    Err(_err) => continue,
                }
            }
        }

        Error::NoErroredThread
    }
}
