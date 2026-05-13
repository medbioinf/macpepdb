use std::{
    collections::HashSet,
    num::NonZeroUsize,
    sync::{Arc, LazyLock},
    time::Duration,
};

use crossbeam::queue::ArrayQueue;
use fallible_iterator::FallibleIterator;
use futures::StreamExt;
use scylla::{
    client::pager::TypedRowStream,
    statement::batch::{Batch, BatchType},
};
use thiserror::Error;

use crate::{
    client::Client,
    configuration::Configuration,
    mass_index::MassIndex,
    peptide::{IsPeptide, Peptide},
    protein_table::ProteinTable,
    sequence::{CompactSequence, PeptideSequence},
};

pub const TABLE_NAME: &str = "peptides";

static INSERT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!("INSERT INTO {TABLE_NAME} (partition, mass, sequence) VALUES (?, ?, ?)")
});

static SELECT_STATEMENT: LazyLock<String> = LazyLock::new(|| format!("SELECT * FROM {TABLE_NAME}"));

pub static INSERTED_PEPTIDES_METRIC: &str = "peptides_table::build::inserted_peptides";
pub static QUEUE_METRIC: &str = "peptides_table::queue";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in peptide table: {0}")]
    Client(#[from] crate::client::Error),
    #[error("CQL execution error in peptide table: {0}")]
    CqlExecution(#[from] scylla::errors::ExecutionError),
    #[error("CQL paged execution error in peptide table: {0}")]
    CqlPagedExecution(#[from] scylla::errors::PagerExecutionError),
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
    MassIndex(#[from] crate::mass_index::Error),
    #[error("No errored thread found in peptide table, but one finished early.")]
    NoErroredThread,
    #[error("Protease error in peptide table: {0}")]
    Protease(#[from] crate::protease::Error),
    #[error("Protein table error in peptide table: {0}")]
    ProteinTable(#[from] crate::protein_table::Error),
    #[error("Peptide error in peptide table: {0}")]
    Peptide(#[from] crate::peptide::Error),
    #[error("Sequence error in peptide table: {0}")]
    Sequence(#[from] crate::sequence::Error),
    // #[error("Protein reader thread error: {0}")]
    // ProteinReaderThread(String),
    #[error("UnipotReader error in peptide table: {0}")]
    UnprotReader(#[from] uniprot_reader::reader::Error),
}

type ConcurrentlyBuildQueue = Arc<ArrayQueue<Option<(i64, HashSet<i32>)>>>;

pub struct PeptideTable {
    client: Arc<Client>,
}

impl PeptideTable {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    pub async fn insert(&self, peptide: &Peptide) -> Result<(), Error> {
        self.client
            .execute_unpaged(INSERT_STATEMENT.as_str(), peptide)
            .await
            .map_err(Error::CqlExecution)?;
        Ok(())
    }

    pub async fn insert_batch(&self, values: Vec<Peptide>) -> Result<(), Error> {
        let batch_type = if values
            .iter()
            .all(|value| value.partition() == values[0].partition())
        {
            BatchType::Unlogged
        } else {
            tracing::warn!(
                "Peptide batch insert includes multipe partitions. This should be avoided."
            );
            BatchType::Logged
        };

        let mut batch_statement = Batch::new(batch_type);
        (0..values.len()).for_each(|_| {
            batch_statement.append_statement(INSERT_STATEMENT.as_str());
        });
        if let Some(consistency) = self.client.write_consistency_level() {
            batch_statement.set_consistency(consistency);
        }

        self.client
            .batch(&batch_statement, values)
            .await
            .map_err(Error::CqlExecution)?;

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
            .await
            .map_err(Error::CqlPagedExecution)?
            .rows_stream::<Peptide>()?)
    }

    pub async fn build_concurrently(
        &self,
        configuration: Arc<Configuration>,
        insert_batch_size: NonZeroUsize,
        num_threads: NonZeroUsize,
        mass_index: MassIndex,
    ) -> Result<(), Error> {
        let queue: ConcurrentlyBuildQueue = Arc::new(ArrayQueue::new(num_threads.get() * 3));
        let inserted_peptides_metric = Arc::new(metrics::counter!(INSERTED_PEPTIDES_METRIC));
        let queue_metric = metrics::gauge!(QUEUE_METRIC);

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let configuration = configuration.clone();
                let queue = queue.clone();
                let client = self.client.clone();
                let inserted_peptides_metric = inserted_peptides_metric.clone();

                tokio::spawn(async move {
                    let peptide_table = PeptideTable::new(client.clone());
                    let protein_table = ProteinTable::new(client);

                    loop {
                        let (mass, protein_ids) = match queue.pop() {
                            Some(Some(entry)) => entry,
                            Some(None) => break,
                            None => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        };

                        let protein_ids_len = protein_ids.len();

                        let mut proteins = protein_table
                            .select(Some("WHERE id IN ?"), (Vec::from_iter(protein_ids),))
                            .await?;

                        // Using the more compact form of the sequence to keep the peptide in memory as small as possible, mass is not important now.
                        let mut peptide_sequences: HashSet<CompactSequence> =
                            HashSet::with_capacity(2 * protein_ids_len);

                        while let Some(protein) = proteins.next().await.transpose()? {
                            #[allow(clippy::mutable_key_type)]
                            configuration
                                .protease()
                                .cleave(protein.sequence().as_ref())
                                .filter(|peptide| Ok(peptide.mass() == mass))
                                .for_each(|peptide| {
                                    peptide_sequences.insert(CompactSequence::try_from(
                                        peptide.into_sequence(),
                                    )?);
                                    Ok(())
                                })?;
                        }

                        let mut peptide_sequence_stream = futures::stream::iter(peptide_sequences)
                            .chunks(insert_batch_size.get());

                        while let Some(chunk) = peptide_sequence_stream.next().await {
                            let peptides = chunk
                                .into_iter()
                                .map(|seq| {
                                    Peptide::new_with_partition(
                                        PeptideSequence::try_from(seq)?,
                                        configuration.mass_partitioning(),
                                    )
                                })
                                .collect::<Result<Vec<_>, crate::peptide::Error>>()?;

                            let peptides_len = peptides.len();
                            peptide_table.insert_batch(peptides).await?;
                            inserted_peptides_metric.increment(peptides_len as u64);
                        }
                    }
                    Ok::<_, Error>(())
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

        for thread in digest_and_insertion_threads {
            thread.await.map_err(|err| Error::Join(err.to_string()))??;
        }

        Ok(())
    }

    async fn find_errored_thread(
        threads: Vec<tokio::task::JoinHandle<Result<(), Error>>>,
    ) -> Error {
        for thread in threads {
            if thread.is_finished() {
                match thread.await {
                    Ok(Ok(())) => continue,
                    Ok(Err(err)) => return err,
                    Err(_err) => continue,
                }
            }
        }

        Error::NoErroredThread
    }
}
