use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    num::NonZeroUsize,
    sync::LazyLock,
};

use fallible_iterator::FallibleIterator;
use futures::{Stream, StreamExt, future::join_all};
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

pub struct MassIndex<'a> {
    client: &'a Client,
}

impl<'a> MassIndex<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn build(
        &self,
        protease: &Protease,
        insert_batch_size: NonZeroUsize,
    ) -> Result<(), Error> {
        let mut proteins = Protein::select(self.client, None, ()).await?;

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
                        self.client,
                        buffer
                            .drain()
                            .map(|(mass, proteins)| Entry { mass, proteins }),
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    // pub async fn build_multithreaded(
    //     &mut self,
    //     protease: &Protease,
    //     num_threads: usize,
    // ) -> Result<(), Error> {
    //     let num_threads = std::cmp::min(num_threads, self.files.len());

    //     let (sender, receiver) = std::sync::mpsc::channel::<(i64, Entry)>();

    //     let protein_reader_threads = (0..num_threads)
    //         .map(|_| {
    //             let file_path_queue = Arc::clone(&file_path_queue);
    //             let sender = sender.clone();
    //             let protease = protease.clone();

    //             std::thread::spawn(move || {
    //                 while let Some((path_idx, path)) = file_path_queue.lock().unwrap().pop_front() {
    //                     let mut buf_reader = BufReader::new(File::open(path).unwrap());
    //                     let reader = uniprot_reader::reader::Reader::new(&mut buf_reader);

    //                     for item in reader {
    //                         let item = item.unwrap();
    //                         #[allow(clippy::mutable_key_type)]
    //                         let peptides = protease
    //                             .cleave(item.entry().sequence(), true)
    //                             .unwrap()
    //                             .collect::<HashSet<_>>()
    //                             .unwrap();

    //                         for peptide in peptides {
    //                             let mass = peptide.mass();
    //                             let entry = Entry {
    //                                 file_idx: path_idx,
    //                                 offset: item.offset().clone(),
    //                             };

    //                             match sender.send((mass, entry)) {
    //                                 Ok(_) => (),
    //                                 Err(_) => return Err(Error::EarlyIndexThreadStop),
    //                             }
    //                         }
    //                     }
    //                 }
    //                 Ok(())
    //             })
    //         })
    //         .collect::<Vec<_>>();

    //     // drop original sender
    //     drop(sender);

    //     while let Ok((mass, entry)) = receiver.recv() {
    //         self.insert(mass, entry)
    //             .await
    //             .map_err(|err| Error::InsertEntry(err.to_string()))?;
    //     }

    //     for thread in protein_reader_threads.into_iter() {
    //         thread
    //             .join()
    //             .map_err(|err| Error::ProteinReaderThread(format!("{err:?}")))??;
    //     }

    //     Ok(())
    // }

    pub async fn len(&self) -> Result<usize, Error> {
        // Using count(*) in CQL-based databases is inefficient and will most likely result in timeouts
        // Streaming the masses and count them is safer.
        Ok(self.masses().await?.count().await)
    }

    pub async fn is_empty(&self) -> Result<bool, Error> {
        // See len
        Ok(self.len().await? == 0)
    }

    pub async fn masses(&'a self) -> Result<impl Stream<Item = Result<i64, Error>>, Error> {
        Ok(self
            .client
            .query_iter(SELECT_MASS_STATEMENT.as_str(), ())
            .await?
            .rows_stream::<(i64,)>()?
            .map(|row| Ok(row?.0)))
    }

    pub async fn entries(&'a self) -> Result<TypedRowStream<Entry>, Error> {
        Entry::select(self.client, None, ()).await
    }

    pub async fn get(&'a self, mass: i64) -> Result<Option<Entry>, Error> {
        let mut stream =
            Entry::select(self.client, Some("WHERE mass = ? LIMIT 1"), (mass,)).await?;

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
