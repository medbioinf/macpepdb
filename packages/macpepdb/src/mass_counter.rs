use std::{
    collections::HashSet, fmt::Debug, num::NonZeroUsize, ops::Deref, sync::Arc, time::Duration,
};

use crossbeam::queue::ArrayQueue;
use dashmap::{DashMap, iter::OwningIter};
use fallible_iterator::FallibleIterator;
use futures::StreamExt;
use thiserror::Error;

use crate::{
    client::Client, mass_index::MassIndex, protease::Protease, protein::Protein,
    sequence::ByteSequence,
};

pub static PROGESS_METRIC: &str = "mass_counter::progress";
pub static PEPTIDES_METRIC: &str = "mass_counter::peptides";
pub static SIZE_METRIC: &str = "mass_counter::size";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unable to unwrap counter from Arc")]
    CounterUnwrap,
    #[error("CQL next row error in mass index: {0}")]
    CqlNextRow(#[from] scylla::errors::NextRowError),
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
    #[error("UnipotReader error in mass index: {0}")]
    UnprotReader(#[from] uniprot_reader::reader::Error),
}

type ConcurrentBuildQueue = Arc<ArrayQueue<Option<(i64, Vec<i32>)>>>;

pub struct MassCounter(DashMap<i64, usize>);

impl MassCounter {
    pub async fn count(
        client: &Client,
        protease: &Protease,
        mass_index: &MassIndex,
    ) -> Result<Self, Error> {
        let progress_metric = metrics::counter!(PROGESS_METRIC);
        let peptides_metric = metrics::counter!(PEPTIDES_METRIC);
        let counter: DashMap<i64, usize> = DashMap::new();

        for entry in mass_index.iter() {
            let mut proteins = Protein::select(
                client,
                Some("WHERE id IN ?"),
                (Vec::from_iter(entry.value().iter().cloned()),),
            )
            .await?;

            // Using the more compact form of the sequence to keep the peptide in memory, mass is not important now.
            let mut peptide_sequences: HashSet<ByteSequence> =
                HashSet::with_capacity(2 * entry.value().len());

            while let Some(protein) = proteins.next().await.transpose()? {
                #[allow(clippy::mutable_key_type)]
                protease
                    .cleave(protein.sequence().to_string().as_str(), true)
                    .map_err(Error::Protease)?
                    .filter(|peptide| Ok(peptide.mass() == *entry.key()))
                    .for_each(|peptide| {
                        peptide_sequences.insert(ByteSequence::try_from(peptide.into_sequence())?);
                        Ok(())
                    })?;
            }

            peptides_metric.increment(peptide_sequences.len() as u64);

            counter.insert(*entry.key(), peptide_sequences.len());

            progress_metric.increment(1);
        }

        Ok(Self(counter))
    }

    pub async fn count_concurrently(
        client: Arc<Client>,
        protease: &Protease,
        mass_index: &MassIndex,
        num_threads: NonZeroUsize,
    ) -> Result<Self, Error> {
        let queue: ConcurrentBuildQueue = Arc::new(ArrayQueue::new(num_threads.get() * 3));

        let protease = Arc::new(protease.clone());
        let progress_metric = Arc::new(metrics::counter!(PROGESS_METRIC));
        let size_metric = Arc::new(metrics::counter!(SIZE_METRIC));
        let peptides_metric = Arc::new(metrics::counter!(PEPTIDES_METRIC));

        let counter: Arc<DashMap<i64, usize>> = Arc::new(DashMap::new());

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let protease = protease.clone();
                let queue = queue.clone();
                let client = client.clone();
                let protease = protease.clone();
                let progress_metric = progress_metric.clone();
                let peptides_metric = peptides_metric.clone();
                let size_metric = size_metric.clone();
                let counter = counter.clone();

                size_metric.increment(std::mem::size_of::<Self>() as u64);

                tokio::spawn(async move {
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

                        let mut proteins =
                            Protein::select(client.as_ref(), Some("WHERE id IN ?"), (protein_ids,))
                                .await?;

                        // Using the more compact form of the sequence to keep the peptide in memory as small as possible, mass is not important now.
                        let mut peptide_sequences: HashSet<ByteSequence> =
                            HashSet::with_capacity(2 * protein_ids_len);

                        while let Some(protein) = proteins.next().await.transpose()? {
                            #[allow(clippy::mutable_key_type)]
                            protease
                                .cleave(protein.sequence().to_string().as_str(), true)
                                .map_err(Error::Protease)?
                                .filter(|peptide| Ok(peptide.mass() == mass))
                                .for_each(|peptide| {
                                    peptide_sequences
                                        .insert(ByteSequence::try_from(peptide.into_sequence())?);
                                    Ok(())
                                })?;
                        }
                        peptides_metric.increment(peptide_sequences.len() as u64);

                        counter.insert(mass, peptide_sequences.len());

                        size_metric.increment(
                            (std::mem::size_of::<i64>() + std::mem::size_of::<usize>()) as u64,
                        );
                        progress_metric.increment(1);
                    }

                    Ok::<_, Error>(())
                })
            })
            .collect::<Vec<_>>();

        for entry in mass_index.iter() {
            let mut entry = Some((*entry.key(), Vec::from_iter(entry.value().iter().cloned())));
            loop {
                entry = match queue.push(entry) {
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
            Arc::try_unwrap(counter).map_err(|_| Error::CounterUnwrap)?,
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

    pub fn peptides_len(&self) -> usize {
        self.0.iter().fold(0, |acc, entry| acc + entry.value())
    }

    async fn inner_size(index: &DashMap<i64, usize>) -> usize {
        std::mem::size_of::<DashMap<i64, usize>>()
            + index.capacity() * (std::mem::size_of::<i64>() + std::mem::size_of::<usize>())
    }

    pub async fn size(&self) -> usize {
        Self::inner_size(&self.0).await + std::mem::size_of::<Self>()
    }
}

impl Deref for MassCounter {
    type Target = DashMap<i64, usize>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<MassCounter> for DashMap<i64, usize> {
    fn from(counter: MassCounter) -> Self {
        counter.0
    }
}

impl IntoIterator for MassCounter {
    type Item = (i64, usize);
    type IntoIter = OwningIter<i64, usize>;

    fn into_iter(self) -> Self::IntoIter {
        DashMap::<i64, usize>::from(self).into_iter()
    }
}

#[cfg(test)]
mod tests {}
