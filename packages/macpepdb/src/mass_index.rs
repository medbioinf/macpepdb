use std::{
    collections::HashSet, fmt::Debug, num::NonZeroUsize, ops::Deref, sync::Arc, time::Duration,
};

use crossbeam::queue::ArrayQueue;
use dashmap::{DashMap, iter::OwningIter};
use fallible_iterator::FallibleIterator;
use futures::StreamExt;
use thiserror::Error;

use crate::{
    database_build::IsProteinAccess, peptide::IsPeptide, protease::Protease, protein::Protein,
};

pub static PROGRESS_METRIC: &str = "mass_index::progress";

#[derive(Debug, Error)]
pub enum Error {
    #[error("CQL next row error in mass index: {0}")]
    CqlNextRow(#[from] scylla::errors::NextRowError),
    // #[error("Indexing stopped unexpectedly before finishing the protein processing ")]
    // EarlyIndexThreadStop,
    #[error("IO error in mass index: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unable to join insertion task: {0}")]
    Join(String),
    #[error("Protein ID missing")]
    MissingProteinId,
    #[error("No errored thread found in mass index, but one finished early.")]
    NoErroredThread,
    #[error("Protease error in mass index: {0}")]
    Protease(#[from] crate::protease::Error),
    #[error("Protein access error in mass index: {0}")]
    ProteinAccess(#[from] crate::database_build::Error),
    #[error("Unable to unwrap index from Arc")]
    IndexUnwrap,
    // #[error("Protein reader thread error: {0}")]
    // ProteinReaderThread(String),
    #[error("UnipotReader error in mass index: {0}")]
    UnprotReader(#[from] uniprot_reader::reader::Error),
}

pub struct MassIndex(DashMap<i64, HashSet<i32>>);

impl MassIndex {
    pub async fn build_concurrently(
        protein_access: Arc<Box<dyn IsProteinAccess>>,
        protease: &Protease,
        num_threads: NonZeroUsize,
    ) -> Result<Self, Error> {
        let mut proteins = protein_access.all().await?;
        let queue: Arc<ArrayQueue<Option<Arc<Protein>>>> =
            Arc::new(ArrayQueue::new(num_threads.get() * 3));
        let protease = Arc::new(protease.clone());
        let progress_metric = Arc::new(metrics::counter!(PROGRESS_METRIC));
        let index: Arc<DashMap<i64, HashSet<i32>>> = Arc::new(DashMap::new());

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let protease = protease.clone();
                let queue = queue.clone();
                let protease = protease.clone();
                let progress_metric = progress_metric.clone();
                let index = index.clone();

                tokio::spawn(async move {
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
                            .cleave(protein.sequence().as_ref())
                            .collect::<HashSet<_>>()
                            .map_err(Error::Protease)?;

                        let masses = peptides
                            .iter()
                            .map(|peptide| peptide.mass())
                            .collect::<HashSet<_>>();

                        for mass in masses {
                            index
                                .entry(mass)
                                .or_default()
                                .insert(protein.id().ok_or(Error::MissingProteinId)?);
                        }
                        progress_metric.increment(1);
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

        Ok(Self(
            Arc::try_unwrap(index).map_err(|_| Error::IndexUnwrap)?,
        ))
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

    async fn inner_size(index: &DashMap<i64, HashSet<i32>>) -> usize {
        std::mem::size_of::<DashMap<i64, HashSet<i32>>>()
            + index.capacity()
                * (std::mem::size_of::<usize>() + std::mem::size_of::<HashSet<i32>>())
            + index.iter().fold(0_usize, |acc, entry| {
                acc + entry.capacity() * std::mem::size_of::<i32>()
            })
    }

    pub async fn size(&self) -> usize {
        Self::inner_size(&self.0).await + std::mem::size_of::<Self>()
    }
}

impl Deref for MassIndex {
    type Target = DashMap<i64, HashSet<i32>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<MassIndex> for DashMap<i64, HashSet<i32>> {
    fn from(index: MassIndex) -> Self {
        index.0
    }
}

impl IntoIterator for MassIndex {
    type Item = (i64, HashSet<i32>);
    type IntoIter = OwningIter<i64, HashSet<i32>>;

    fn into_iter(self) -> Self::IntoIter {
        DashMap::<i64, HashSet<i32>>::from(self).into_iter()
    }
}

#[cfg(test)]
mod tests {}
