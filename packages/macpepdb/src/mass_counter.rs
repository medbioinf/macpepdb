use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    num::NonZeroUsize,
    sync::{Arc, LazyLock, atomic::AtomicUsize},
    time::Duration,
};

use crossbeam::queue::ArrayQueue;
use fallible_iterator::FallibleIterator;
use futures::{Stream, StreamExt, future::join_all};
use scylla::{
    DeserializeRow, SerializeRow, client::pager::TypedRowStream, errors::ExecutionError,
    serialize::row::SerializeRow,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    client::Client, mass_index::Entry as MassIndexEntry, protease::Protease, protein::Protein,
    sequence::ByteSequence,
};

static TABLE_NAME: &str = "mass_counts";

static INSERT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("INSERT INTO {TABLE_NAME} (mass, count) VALUES (?, ?)"));

static SELECT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT mass, count FROM {TABLE_NAME}"));

static SELECT_MASS_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT mass FROM {TABLE_NAME}"));

pub static PROGESS_METRIC: &str = "mass_counter::progress";
pub static PEPTIDES_METRIC: &str = "mass_counter::peptides";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in mass index: {0}")]
    Client(#[from] crate::client::Error),
    #[error("CQL execution error in mass index: {0}")]
    CqlExecution(#[from] scylla::errors::ExecutionError),
    #[error("CQL paged execution error in mass index: {0}")]
    CqlPagedExecution(#[from] scylla::errors::PagerExecutionError),
    #[error("CQL type check failed in mass index: {0}")]
    CqlTypeCheck(#[from] scylla::errors::TypeCheckError),
    #[error("CQL next row error in mass index: {0}")]
    CqlNextRow(#[from] scylla::errors::NextRowError),
    #[error("IO error in mass index: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unable to join insertion task: {0}")]
    Join(String),
    #[error("No errored thread found in mass index, but one finished early.")]
    NoErroredThread,
    #[error("Mass index error in mass index: {0}")]
    MassIndex(#[from] crate::mass_index::Error),
    #[error("Protease error in mass index: {0}")]
    Protease(#[from] crate::protease::Error),
    #[error("Protein error in mass index: {0}")]
    Protein(#[from] crate::protein::Error),
    #[error("UnipotReader error in mass index: {0}")]
    UnprotReader(#[from] uniprot_reader::reader::Error),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, DeserializeRow, SerializeRow)]
pub struct Entry {
    mass: i64,
    count: i64,
}

impl Entry {
    pub fn new(mass: i64, count: i64) -> Self {
        Self { mass, count }
    }

    pub fn mass(&self) -> i64 {
        self.mass
    }

    pub fn count(&self) -> i64 {
        self.count
    }

    pub async fn insert(&self, client: &Client) -> Result<(), Error> {
        client
            .execute_unpaged(INSERT_STATEMENT.as_str(), &self)
            .await?;

        Ok(())
    }

    pub async fn insert_batch(
        client: &Client,
        values: impl Iterator<Item = Self>,
    ) -> Result<(), Error> {
        let insertion_futures =
            values.map(|value| client.execute_unpaged(INSERT_STATEMENT.as_str(), value));

        join_all(insertion_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, ExecutionError>>()?;

        Ok(())
    }

    pub async fn select(
        client: &Client,
        select_addition: Option<&str>,
        values: impl SerializeRow,
    ) -> Result<TypedRowStream<Self>, Error> {
        let statement = select_addition
            .map(|addition| format!("{} {}", SELECT_STATEMENT.as_str(), addition))
            .unwrap_or_else(|| SELECT_STATEMENT.as_str().to_string());

        Ok(client
            .execute_iter(statement, values)
            .await?
            .rows_stream::<Self>()?)
    }
}

pub struct MassCounter {
    client: Arc<Client>,
}

impl MassCounter {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    pub async fn count(
        &self,
        protease: &Protease,
        insert_batch_size: NonZeroUsize,
    ) -> Result<usize, Error> {
        let mut mass_index_entries = MassIndexEntry::select(self.client.as_ref(), None, ()).await?;
        let mut peptide_ctr: usize = 0;

        let progress_metric = metrics::counter!(PROGESS_METRIC);
        let peptides_metric = metrics::counter!(PEPTIDES_METRIC);

        let mut buffer: VecDeque<Entry> = VecDeque::with_capacity(insert_batch_size.get());

        while let Some(mass_index_entry) = mass_index_entries.next().await.transpose()? {
            let mut proteins = Protein::select(
                self.client.as_ref(),
                Some("WHERE accession IN ?"),
                (mass_index_entry.proteins(),),
            )
            .await?;

            // Using the more compact form of the sequence to keep the peptide in memory, mass is not important now.
            let mut peptide_sequences: HashSet<ByteSequence> =
                HashSet::with_capacity(2 * mass_index_entry.proteins().len());

            while let Some(protein) = proteins.next().await.transpose()? {
                #[allow(clippy::mutable_key_type)]
                protease
                    .cleave(protein.sequence().to_string().as_str(), true)
                    .map_err(Error::Protease)?
                    .for_each(|peptide| {
                        peptide_sequences.insert(ByteSequence::try_from(peptide.into_sequence())?);
                        Ok(())
                    })?;
            }

            peptide_ctr += peptide_sequences.len();
            peptides_metric.increment(peptide_sequences.len() as u64);

            buffer.push_back(Entry {
                mass: mass_index_entry.mass(),
                count: peptide_sequences.len() as i64,
            });

            if buffer.len() >= insert_batch_size.get() {
                Entry::insert_batch(self.client.as_ref(), buffer.drain(..)).await?;
            }

            progress_metric.increment(1);
        }

        if !buffer.is_empty() {
            Entry::insert_batch(self.client.as_ref(), buffer.drain(..)).await?;
        }

        Ok(peptide_ctr)
    }

    pub async fn count_concurrently(
        &self,
        protease: &Protease,
        insert_batch_size: NonZeroUsize,
        num_threads: NonZeroUsize,
    ) -> Result<usize, Error> {
        let mut mass_index_entries = MassIndexEntry::select(self.client.as_ref(), None, ()).await?;
        let queue: Arc<ArrayQueue<Option<MassIndexEntry>>> =
            Arc::new(ArrayQueue::new(num_threads.get() * 3));
        let protease = Arc::new(protease.clone());
        let peptide_ctr = Arc::new(AtomicUsize::new(0));
        let progress_metric = Arc::new(metrics::counter!(PROGESS_METRIC));
        let peptides_metric = Arc::new(metrics::counter!(PEPTIDES_METRIC));

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let protease = protease.clone();
                let queue = queue.clone();
                let client = self.client.clone();
                let protease = protease.clone();
                let peptide_ctr = peptide_ctr.clone();
                let peptides_metric = peptides_metric.clone();

                tokio::spawn(async move {
                    let mut buffer: VecDeque<Entry> =
                        VecDeque::with_capacity(insert_batch_size.get());

                    loop {
                        let mass_index_entry = match queue.pop() {
                            Some(Some(entry)) => entry,
                            Some(None) => break,
                            None => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        };

                        let mut proteins = Protein::select(
                            client.as_ref(),
                            Some("WHERE accession IN ?"),
                            (mass_index_entry.proteins(),),
                        )
                        .await?;

                        // Using the more compact form of the sequence to keep the peptide in memory as small as possible, mass is not important now.
                        let mut peptide_sequences: HashSet<ByteSequence> =
                            HashSet::with_capacity(2 * mass_index_entry.proteins().len());

                        while let Some(protein) = proteins.next().await.transpose()? {
                            #[allow(clippy::mutable_key_type)]
                            protease
                                .cleave(protein.sequence().to_string().as_str(), true)
                                .map_err(Error::Protease)?
                                .for_each(|peptide| {
                                    peptide_sequences
                                        .insert(ByteSequence::try_from(peptide.into_sequence())?);
                                    Ok(())
                                })?;
                        }

                        peptide_ctr.fetch_add(
                            peptide_sequences.len(),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        peptides_metric.increment(peptide_sequences.len() as u64);

                        buffer.push_back(Entry {
                            mass: mass_index_entry.mass(),
                            count: peptide_sequences.len() as i64,
                        });

                        if buffer.len() >= insert_batch_size.get() {
                            Entry::insert_batch(client.as_ref(), buffer.drain(..)).await?;
                        }
                    }

                    if !buffer.is_empty() {
                        Entry::insert_batch(client.as_ref(), buffer.drain(..)).await?;
                    }

                    Ok::<_, Error>(())
                })
            })
            .collect::<Vec<_>>();

        while let Some(mass_index_entry) = mass_index_entries.next().await.transpose()? {
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
            progress_metric.increment(1);
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

        Ok(peptide_ctr.load(std::sync::atomic::Ordering::Relaxed))
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

    pub async fn len(&self) -> Result<usize, Error> {
        // Using count(*) in CQL-based databases is inefficient and will most likely result in timeouts
        // Streaming the masses and count them is safer.
        Ok(self.masses().await?.count().await)
    }

    pub async fn is_empty(&self) -> Result<bool, Error> {
        // See len
        Ok(self.len().await? == 0)
    }

    pub async fn masses(&self) -> Result<impl Stream<Item = Result<i64, Error>>, Error> {
        Ok(self
            .client
            .execute_iter(SELECT_MASS_STATEMENT.as_str(), ())
            .await?
            .rows_stream::<(i64,)>()?
            .map(|row| Ok(row?.0)))
    }

    pub async fn entries(&self) -> Result<impl Stream<Item = Result<Entry, Error>>, Error> {
        Ok(Entry::select(self.client.as_ref(), None, ())
            .await?
            .map(|entry| entry.map_err(Error::from)))
    }

    pub async fn get(&self, mass: i64) -> Result<Option<Entry>, Error> {
        let mut stream = Entry::select(
            self.client.as_ref(),
            Some("WHERE mass = ? LIMIT 1"),
            (mass,),
        )
        .await?;

        while let Some(entry) = stream.next().await.transpose()? {
            if entry.mass() == mass {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {}
