use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    num::NonZeroUsize,
    sync::{Arc, LazyLock},
    time::Duration,
};

use crossbeam::queue::ArrayQueue;
use fallible_iterator::FallibleIterator;
use futures::{Stream, StreamExt, TryStreamExt, future::join_all};
use scylla::{
    DeserializeRow, SerializeRow, client::pager::TypedRowStream, errors::ExecutionError,
    serialize::row::SerializeRow,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{client::Client, protease::Protease, protein::Protein};

static TABLE_NAME: &str = "mass_index";

static UPSERT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("UPDATE {TABLE_NAME} SET proteins = proteins + ? WHERE mass = ?"));

static SELECT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT mass, proteins FROM {TABLE_NAME}"));

static SELECT_MASS_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT mass FROM {TABLE_NAME}"));

pub static PROGRESS_METRIC: &str = "mass_index::progress";

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
    // #[error("Indexing stopped unexpectedly before finishing the protein processing ")]
    // EarlyIndexThreadStop,
    #[error("IO error in mass index: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unable to join insertion task: {0}")]
    Join(String),
    #[error("No errored thread found in mass index, but one finished early.")]
    NoErroredThread,
    #[error("Protease error in mass index: {0}")]
    Protease(#[from] crate::protease::Error),
    #[error("Protein error in mass index: {0}")]
    Protein(#[from] crate::protein::Error),
    // #[error("Protein reader thread error: {0}")]
    // ProteinReaderThread(String),
    #[error("UnipotReader error in mass index: {0}")]
    UnprotReader(#[from] uniprot_reader::reader::Error),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, DeserializeRow, SerializeRow)]
pub struct Entry {
    mass: i64,
    proteins: HashSet<String>,
}

impl Entry {
    pub fn mass(&self) -> i64 {
        self.mass
    }

    pub fn proteins(&self) -> &HashSet<String> {
        &self.proteins
    }

    pub async fn upsert(&self, client: &Client) -> Result<(), Error> {
        let stmt = client
            .get_prepared_statement(UPSERT_STATEMENT.as_str())
            .await?;

        client.execute_unpaged(&stmt, &self).await?;

        Ok(())
    }

    pub async fn upsert_batch(
        client: &Client,
        values: impl Iterator<Item = Self>,
    ) -> Result<(), Error> {
        let stmt = client
            .get_prepared_statement(UPSERT_STATEMENT.as_str())
            .await?;

        let upsert_futures = values.map(|value| client.execute_unpaged(&stmt, value));

        join_all(upsert_futures)
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
            .query_iter(statement.as_str(), values)
            .await?
            .rows_stream::<Self>()?)
    }
}

pub struct MassIndex {
    client: Arc<Client>,
}

impl MassIndex {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    pub async fn build(
        &self,
        protease: &Protease,
        insert_batch_size: NonZeroUsize,
    ) -> Result<usize, Error> {
        let mut proteins = Protein::select(self.client.as_ref(), None, ()).await?;
        let progress_metric = metrics::counter!(PROGRESS_METRIC);

        let mut buffer: HashMap<i64, HashSet<String>> =
            HashMap::with_capacity(insert_batch_size.get());

        while let Some(protein) = proteins.next().await.transpose()? {
            #[allow(clippy::mutable_key_type)]
            let peptides = protease
                .cleave(protein.sequence().to_string().as_str(), true)
                .map_err(Error::Protease)?
                .collect::<HashSet<_>>()
                .map_err(Error::Protease)?;

            let masses = peptides
                .iter()
                .map(|peptide| peptide.mass())
                .collect::<HashSet<_>>();

            for mass in masses {
                buffer
                    .entry(mass)
                    .or_default()
                    .insert(protein.accession().to_string());

                if buffer.len() >= insert_batch_size.get() {
                    Entry::upsert_batch(
                        self.client.as_ref(),
                        buffer
                            .drain()
                            .map(|(mass, proteins)| Entry { mass, proteins }),
                    )
                    .await?;
                }
            }
            progress_metric.increment(1);
        }

        if !buffer.is_empty() {
            Entry::upsert_batch(
                self.client.as_ref(),
                buffer
                    .drain()
                    .map(|(mass, proteins)| Entry { mass, proteins }),
            )
            .await?;
        }

        self.masses()
            .await?
            .try_fold(0, |ctr, _mass| async move { Ok(ctr + 1) })
            .await
    }

    pub async fn build_concurrently(
        &self,
        protease: &Protease,
        insert_batch_size: NonZeroUsize,
        num_threads: NonZeroUsize,
    ) -> Result<usize, Error> {
        let mut proteins = Protein::select(self.client.as_ref(), None, ()).await?;
        let queue: Arc<ArrayQueue<Option<Protein>>> =
            Arc::new(ArrayQueue::new(num_threads.get() * 3));
        let protease = Arc::new(protease.clone());
        let progress_metric = Arc::new(metrics::counter!(PROGRESS_METRIC));

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let protease = protease.clone();
                let queue = queue.clone();
                let client = self.client.clone();
                let protease = protease.clone();
                let progress_metric = progress_metric.clone();

                tokio::spawn(async move {
                    let mut buffer: HashMap<i64, HashSet<String>> =
                        HashMap::with_capacity(insert_batch_size.get());

                    loop {
                        let protein = match queue.pop() {
                            Some(Some(protein)) => protein,
                            Some(None) => break,
                            None => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        };

                        #[allow(clippy::mutable_key_type)]
                        let peptides = protease
                            .cleave(protein.sequence().to_string().as_str(), true)
                            .map_err(Error::Protease)?
                            .collect::<HashSet<_>>()
                            .map_err(Error::Protease)?;

                        let masses = peptides
                            .iter()
                            .map(|peptide| peptide.mass())
                            .collect::<HashSet<_>>();

                        for mass in masses {
                            buffer
                                .entry(mass)
                                .or_default()
                                .insert(protein.accession().to_string());

                            if buffer.len() >= insert_batch_size.get() {
                                Entry::upsert_batch(
                                    client.as_ref(),
                                    buffer
                                        .drain()
                                        .map(|(mass, proteins)| Entry { mass, proteins }),
                                )
                                .await?;
                            }
                        }
                        progress_metric.increment(1);
                    }

                    if !buffer.is_empty() {
                        Entry::upsert_batch(
                            client.as_ref(),
                            buffer
                                .drain()
                                .map(|(mass, proteins)| Entry { mass, proteins }),
                        )
                        .await?;
                    }

                    Ok::<_, Error>(())
                })
            })
            .collect::<Vec<_>>();

        while let Some(protein) = proteins.next().await.transpose()? {
            let mut protein = Some(protein);
            loop {
                protein = match queue.push(protein) {
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

        self.masses()
            .await?
            .try_fold(0, |ctr, _mass| async move { Ok(ctr + 1) })
            .await
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
            .query_iter(SELECT_MASS_STATEMENT.as_str(), ())
            .await?
            .rows_stream::<(i64,)>()?
            .map(|row| Ok(row?.0)))
    }

    pub async fn entries(&self) -> Result<TypedRowStream<Entry>, Error> {
        Entry::select(self.client.as_ref(), None, ()).await
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
