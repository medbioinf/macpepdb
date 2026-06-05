use std::{
    collections::{HashMap, HashSet, hash_map::IntoIter},
    fmt::Debug,
    num::NonZeroUsize,
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use crossbeam::queue::ArrayQueue;
use fallible_iterator::FallibleIterator;
use futures::StreamExt;
use thiserror::Error;

use crate::{
    database_build::IsProteinAccess, peptide::IsPeptide, protease::Protease, protein::Protein,
};

pub static PROGRESS_METRIC: &str = "mass_index::progress";

pub static LOCAL_INDEX_MAX_KEYS: usize = 1000;

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

pub struct MassIndex(HashMap<i64, HashSet<i32>>);

impl MassIndex {
    pub async fn build_concurrently(
        protein_access: Arc<Box<dyn IsProteinAccess>>,
        protease: &Protease,
        num_threads: NonZeroUsize,
    ) -> Result<Self, Error> {
        let queue: Arc<ArrayQueue<Option<Arc<Protein>>>> =
            Arc::new(ArrayQueue::new(num_threads.get() * 3));
        let protease = Arc::new(protease.clone());
        let progress_metric = Arc::new(metrics::counter!(PROGRESS_METRIC));

        let protein_count = protein_access.count().await?;
        let protein_amount_for_estimation = std::cmp::max(protein_count / 100 * 10, 1);

        let mut masses = HashSet::<i64>::with_capacity(protein_amount_for_estimation * 10);
        let mut proteins = protein_access
            .all()
            .await?
            .take(protein_amount_for_estimation)
            .map(|res| res.map_err(Error::from));

        while let Some(protein) = proteins.next().await {
            let protein = protein?;
            let peptides = protease
                .cleave(protein.sequence().as_ref(), None)
                .collect::<Vec<_>>()
                .map_err(Error::Protease)?;
            masses.extend(peptides.into_iter().map(|peptide| peptide.mass()));
        }

        let masses_estimation = masses.len() * 10;
        tracing::info!("Estimate ~{masses_estimation} masses");

        drop(masses);

        let index = Arc::new(parking_lot::Mutex::new(
            HashMap::<i64, HashSet<i32>>::with_capacity(masses_estimation),
        ));

        let mut proteins = protein_access.all().await?;

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let protease = protease.clone();
                let queue = queue.clone();
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
                        let protein_id = protein.id().ok_or(Error::MissingProteinId)?;

                        #[allow(clippy::mutable_key_type)]
                        let masses = protease
                            .cleave_masses_only(protein.sequence().as_ref())
                            .collect::<HashSet<_>>()?;

                        let mut index_lock = index.lock();
                        for mass in masses {
                            index_lock
                                .entry(mass)
                                .or_insert_with(|| HashSet::with_capacity(10))
                                .insert(protein_id);
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

        let index = Arc::try_unwrap(index)
            .map_err(|_| Error::IndexUnwrap)?
            .into_inner();

        Ok(MassIndex(index))
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

    async fn inner_size(index: &HashMap<i64, HashSet<i32>>) -> usize {
        std::mem::size_of::<HashMap<i64, HashSet<i32>>>()
            + index.capacity() * (std::mem::size_of::<i64>() + std::mem::size_of::<HashSet<i32>>())
            + index.iter().fold(0_usize, |acc, entry| {
                acc + entry.1.capacity() * std::mem::size_of::<i32>()
            })
    }

    pub async fn size(&self) -> usize {
        Self::inner_size(&self.0).await + std::mem::size_of::<Self>()
    }
}

impl Deref for MassIndex {
    type Target = HashMap<i64, HashSet<i32>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<MassIndex> for HashMap<i64, HashSet<i32>> {
    fn from(index: MassIndex) -> Self {
        index.0
    }
}

impl IntoIterator for MassIndex {
    type Item = (i64, HashSet<i32>);
    type IntoIter = IntoIter<i64, HashSet<i32>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {}
