use std::{
    collections::HashSet,
    num::{NonZeroI64, NonZeroUsize},
    ops::Index,
    sync::{Arc, LazyLock},
    time::Duration,
};

use crossbeam::queue::ArrayQueue;
use fallible_iterator::FallibleIterator;
use futures::StreamExt;

use rayon::prelude::ParallelSliceMut;
use thiserror::Error;

use crate::{
    amino_acid::TRYPTOPHAN,
    client::Client,
    database_build::IsProteinAccess,
    protease::Protease,
    protein::Protein,
    sequence::{IsBitSequence, PeptideSequence},
    stats_table::StatsTable,
};

pub static PROGRESS_METRIC: &str = "mass_index::progress";

static LOCAL_MASS_LIMIT: usize = 10_000;

static THEORETICAL_MAX_MASS: LazyLock<i64> =
    LazyLock::new(|| TRYPTOPHAN.mono_mass() * PeptideSequence::MAX_LENGTH.get() as i64);

#[derive(Debug, Error)]
pub enum Error {
    #[error(
        "Unable to unwrap masses from Arc. At this point only one reference should be exists. Maybe a thread did not stop?"
    )]
    FinalMassesUnwrap,
    #[error("IO error in mass index: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "Unable to unwrap index ptr from Arc. At this point only one reference should be exists. Maybe a thread did not stop?"
    )]
    IndexPtrUnwrap,
    #[error("Unable to join insertion task: {0}")]
    Join(String),
    #[error(
        "Unable to unwrap masses from Arc. At this point only one reference should be exists. Maybe a thread did not stop?"
    )]
    MassesUnwrap,
    #[error("Protein ID missing")]
    MissingProteinId,
    #[error("No errored thread found in mass index, but one finished early.")]
    NoErroredThread,
    #[error("Protease error in mass index: {0}")]
    Protease(#[from] crate::protease::Error),
    #[error("Protein access error in mass index: {0}")]
    ProteinAccess(#[from] crate::database_build::Error),
    #[error(
        "Unable to unwrap protein IDs from Arc. At this point only one reference should be exists. Maybe a thread did not stop?"
    )]
    ProteinIdsUnwrap,
    // #[error("Protein reader thread error: {0}")]
    // ProteinReaderThread(String),
    #[error("StatsTable error in mass index: {0}")]
    StatsTale(#[from] crate::stats_table::Error),
    #[error("UnipotReader error in mass index: {0}")]
    UnprotReader(#[from] uniprot_reader::reader::Error),
}

pub struct PartialMassIndex {
    mass_interval: (i64, i64),
    masses: Vec<i64>,
    indptr: Vec<u64>,
    protein_ids: Vec<i32>,
}

impl PartialMassIndex {
    pub fn size(&self) -> usize {
        std::mem::size_of::<(i64, i64)>()
            + std::mem::size_of::<Self>()
            + self.masses.capacity() * std::mem::size_of::<i64>()
            + self.indptr.capacity() * std::mem::size_of::<u64>()
            + self.protein_ids.capacity() * std::mem::size_of::<i32>()
    }
}

pub struct IntoIter {
    masses: Vec<i64>,
    indptr: Vec<u64>,
    protein_ids: Vec<i32>,
}

impl Iterator for IntoIter {
    type Item = (i64, Vec<i32>);

    fn next(&mut self) -> Option<Self::Item> {
        let mass = self.masses.pop()?;
        self.indptr.pop(); // drop the trailing end pointer for this row
        let start = *self.indptr.last().unwrap() as usize;
        // start..protein_ids.len() is always the tail when going back-to-front
        let ids = self.protein_ids.split_off(start);
        Some((mass, ids))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.masses.len(), Some(self.masses.len()))
    }
}

impl ExactSizeIterator for IntoIter {}

impl IntoIterator for PartialMassIndex {
    type Item = (i64, Vec<i32>);
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            masses: self.masses,
            indptr: self.indptr,
            protein_ids: self.protein_ids,
        }
    }
}

impl Index<i64> for PartialMassIndex {
    type Output = [i32];

    fn index(&self, mass: i64) -> &Self::Output {
        if let Ok(idx) = self.masses.binary_search(&mass) {
            &self.protein_ids[self.indptr[idx] as usize..self.indptr[idx + 1] as usize]
        } else {
            &[]
        }
    }
}

pub struct MassIndex {
    parts: Vec<PartialMassIndex>,
}

impl MassIndex {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            parts: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, part: PartialMassIndex) {
        self.parts.push(part);
    }

    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    pub fn len(&self) -> usize {
        self.parts.iter().map(|part| part.masses.len()).sum()
    }

    pub fn num_protein_associations(&self) -> usize {
        self.parts.iter().map(|part| part.protein_ids.len()).sum()
    }

    pub fn masses(&self) -> impl Iterator<Item = &i64> {
        self.parts.iter().flat_map(|part| part.masses.as_slice())
    }

    pub async fn build(
        client: Arc<Client>,
        protein_access: Arc<Box<dyn IsProteinAccess>>,
        protease: Arc<Protease>,
        num_threads: NonZeroUsize,
        mass_interval_width: Option<NonZeroI64>,
    ) -> Result<Self, Error> {
        let intervals = if let Some(interval) = mass_interval_width {
            (0..=*THEORETICAL_MAX_MASS / interval.get())
                .map(|i| {
                    let start = i * interval.get();
                    let end = (i + 1) * interval.get();
                    (start, end)
                })
                .collect()
        } else {
            vec![(0, *THEORETICAL_MAX_MASS)]
        };

        let mut mass_index = Self::with_capacity(intervals.len());

        for interval in intervals.into_iter() {
            let partial_mass_index = Self::partial_build(
                protein_access.clone(),
                protease.clone(),
                num_threads,
                interval,
            )
            .await?;
            mass_index.push(partial_mass_index);
        }

        StatsTable::new(client.clone())
            .upsert_mass_count(mass_index.len())
            .await?;

        Ok(mass_index)
    }

    async fn partial_build(
        protein_access: Arc<Box<dyn IsProteinAccess>>,
        protease: Arc<Protease>,
        num_threads: NonZeroUsize,
        mass_interval: (i64, i64),
    ) -> Result<PartialMassIndex, Error> {
        let interval = Arc::new(mass_interval);

        // Intermediate: unsorted flat (mass_idx, protein_id) pairs from all threads.
        // Sorted + deduped after threads finish, then converted to CSR in one pass.
        let pairs: Arc<parking_lot::Mutex<Vec<(i64, i32)>>> = Arc::new(parking_lot::Mutex::new(
            Vec::with_capacity(protein_access.count().await? * 10),
        ));

        let queue: Arc<ArrayQueue<Option<Arc<Protein>>>> =
            Arc::new(ArrayQueue::new(num_threads.get() * 3));
        let progress_metric = Arc::new(metrics::counter!(PROGRESS_METRIC));
        progress_metric.absolute(0);

        let mut proteins = protein_access.all().await?;

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let protease = protease.clone();
                let queue = queue.clone();
                let progress_metric = progress_metric.clone();
                let mass_interval = interval.clone();
                let pairs = pairs.clone();

                tokio::spawn(async move {
                    let mut local_pairs: HashSet<(i64, i32)> =
                        HashSet::with_capacity(LOCAL_MASS_LIMIT);
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
                        protease
                            .cleave_masses_only(protein.sequence().as_ref())
                            .filter(|mass| Ok(mass_interval.0 <= *mass && *mass < mass_interval.1))
                            .for_each(|mass| {
                                local_pairs.insert((mass, protein_id));
                                Ok(())
                            })?;

                        if local_pairs.len() >= LOCAL_MASS_LIMIT {
                            pairs.lock().extend(local_pairs.drain());
                        }

                        progress_metric.increment(1);
                    }

                    if !local_pairs.is_empty() {
                        pairs.lock().extend(local_pairs);
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

        let mut pairs = Arc::try_unwrap(pairs)
            .map_err(|_| Error::ProteinIdsUnwrap)?
            .into_inner();

        // Sort by (mass_idx, protein_id) and deduplicate so each pair is unique.
        pairs.par_sort_unstable();
        pairs.dedup();

        // Build CSR from sorted pairs in one pass.
        // indptr[i+1] counts entries for row i; prefix-summed into offsets after.
        let mut masses = Vec::new();
        let mut indptr = vec![0u64];
        let mut protein_ids = Vec::with_capacity(pairs.len());

        for (mass, protein_id) in &pairs {
            if masses.last() != Some(mass) {
                masses.push(*mass);
                indptr.push(*indptr.last().unwrap());
            }
            *indptr.last_mut().unwrap() += 1;
            protein_ids.push(*protein_id);
        }

        Ok(PartialMassIndex {
            mass_interval,
            masses,
            indptr,
            protein_ids,
        })
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

    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.parts.iter().map(|part| part.size()).sum::<usize>()
    }
}

impl Index<i64> for MassIndex {
    type Output = [i32];

    fn index(&self, mass: i64) -> &Self::Output {
        for part in &self.parts {
            if part.mass_interval.0 <= mass && mass < part.mass_interval.1 {
                return &part[mass];
            }
        }
        &[]
    }
}

impl IntoIterator for MassIndex {
    type Item = PartialMassIndex;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.parts.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        num::NonZeroUsize,
        sync::Arc,
    };

    use fallible_iterator::FallibleIterator;
    use futures::StreamExt;
    use uniprot_reader::asynchronous::reader::AsyncReader;

    use crate::{
        database_build::{InMemoryProteinAccess, IsProteinAccess},
        protease::Protease,
        protein::Protein,
    };

    use super::*;

    #[tokio::test]
    async fn test_mass_index() {
        let proteins_file = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("some_human_proteins.uniprot.txt");

        let protease = Protease::by_name(
            "trypsin",
            Some(NonZeroUsize::new(6).unwrap()),
            Some(NonZeroUsize::new(50).unwrap()),
            Some(2),
            false,
        )
        .unwrap();

        let mut buf_reader =
            tokio::io::BufReader::new(tokio::fs::File::open(proteins_file).await.unwrap());
        let reader = AsyncReader::new(&mut buf_reader);

        let proteins = reader
            .enumerate()
            .map(|(protein_id, entry)| {
                Protein::try_from((protein_id as i32, entry.unwrap().entry())).unwrap()
            })
            .collect::<Vec<_>>()
            .await;

        let mut manual_index: HashMap<i64, HashSet<i32>> = HashMap::new();
        proteins.iter().for_each(|protein| {
            let protein_id = protein.id().unwrap();
            protease
                .cleave_masses_only(protein.sequence().as_ref())
                .for_each(|mass| {
                    manual_index.entry(mass).or_default().insert(protein_id);

                    Ok(())
                })
                .unwrap();
        });

        let protein_access: Arc<Box<dyn IsProteinAccess>> = Arc::new(Box::new(
            InMemoryProteinAccess::with_proteins(proteins.into_iter()),
        ));

        let mass_index = MassIndex::partial_build(
            protein_access,
            Arc::new(protease),
            NonZeroUsize::new(4).unwrap(),
            (0, *THEORETICAL_MAX_MASS),
        )
        .await
        .unwrap();

        for (mass, protein_ids) in mass_index.into_iter() {
            let expected_protein_ids = manual_index.get(&mass).unwrap();
            assert_eq!(
                expected_protein_ids.len(),
                protein_ids.len(),
                "Mass {}: expected {} protein IDs, got {}",
                mass,
                expected_protein_ids.len(),
                protein_ids.len()
            );
            for protein_id in protein_ids {
                assert!(
                    expected_protein_ids.contains(&protein_id),
                    "Mass {}: unexpected protein ID {}",
                    mass,
                    protein_id
                );
            }
        }
    }
}
