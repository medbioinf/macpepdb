use std::{
    collections::HashSet,
    hash::Hash,
    num::{NonZeroI64, NonZeroUsize},
    ops::Index,
    sync::{Arc, LazyLock},
    time::Duration,
};

use crossbeam::queue::ArrayQueue;
use fallible_iterator::FallibleIterator;
use futures::StreamExt;

use metrics::counter;
use rayon::prelude::ParallelSliceMut;
use thiserror::Error;

use crate::{
    amino_acid::TRYPTOPHAN,
    client::Client,
    database_build::IsProteinAccess,
    mass::to_float as mass_to_float,
    protease::Protease,
    protein::Protein,
    sequence::{IsBitSequence, PeptideSequence},
    stats_table::StatsTable,
};

pub static PARTIAL_PROGRESS_METRIC: &str = "mass_index::partial_progress::processed_proteins";
pub static TOTAL_PROGRESS_METRIC: &str = "mass_index::total_progress::processed_proteins";

static LOCAL_MASS_LIMIT: usize = 8_333_333; // 100 MB for i64 + i32

static THEORETICAL_MAX_MASS: LazyLock<i64> =
    LazyLock::new(|| TRYPTOPHAN.mono_mass() * PeptideSequence::MAX_LENGTH.get() as i64);

/// A `(mass, protein_id)` pair packed into 12 bytes (no alignment padding).
///
/// A plain `(i64, i32)` is 16 bytes — the `i64` forces 8-byte alignment, wasting 4 bytes
/// per pair. With billions of pairs that 33% is real memory. The mass is split into two
/// `u32` halves so every field is `i32`-sized and the struct keeps a 4-byte alignment.
/// That alignment matters: the CSR build reinterprets the pair buffer in place as a
/// `Vec<i32>` (see `partial_build`), which is only sound when the allocation's layout
/// matches `[i32]`. The const asserts below pin that invariant.
#[derive(Clone, Copy)]
#[repr(C)]
struct MassPidPair {
    mass_lo: u32,
    mass_hi: u32,
    pid: i32,
}

const _: () = {
    assert!(std::mem::size_of::<MassPidPair>() == 3 * std::mem::size_of::<i32>());
    assert!(std::mem::align_of::<MassPidPair>() == std::mem::align_of::<i32>());
};

impl MassPidPair {
    #[inline]
    fn new(mass: i64, pid: i32) -> Self {
        let mass = mass as u64;
        Self {
            mass_lo: mass as u32,
            mass_hi: (mass >> 32) as u32,
            pid,
        }
    }

    #[inline]
    fn mass(&self) -> i64 {
        (((self.mass_hi as u64) << 32) | self.mass_lo as u64) as i64
    }

    #[inline]
    fn pid(&self) -> i32 {
        self.pid
    }
}

impl Eq for MassPidPair {}

impl Hash for MassPidPair {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.mass().hash(state);
        self.pid().hash(state);
    }
}

impl PartialEq for MassPidPair {
    fn eq(&self, other: &Self) -> bool {
        self.mass() == other.mass() && self.pid() == other.pid()
    }
}

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

type PairQueue = Arc<ArrayQueue<Option<HashSet<MassPidPair>>>>;

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

    pub fn number_of_intervals(mass_interval_width: Option<NonZeroI64>) -> usize {
        if let Some(width) = mass_interval_width {
            (*THEORETICAL_MAX_MASS / width.get()) as usize
        } else {
            1
        }
    }

    pub async fn build(
        client: Arc<Client>,
        protein_access: Arc<Box<dyn IsProteinAccess>>,
        protease: Arc<Protease>,
        num_threads: NonZeroUsize,
        mass_interval_width: Option<NonZeroI64>,
    ) -> Result<Self, Error> {
        let intervals = if let Some(width) = mass_interval_width {
            (0..=*THEORETICAL_MAX_MASS / width.get())
                .map(|i| {
                    let start = i * width.get();
                    let end = (i + 1) * width.get();
                    (start, end)
                })
                .collect()
        } else {
            vec![(0, *THEORETICAL_MAX_MASS)]
        };

        tracing::info!(
            "Mass index will be built with {} intervals",
            intervals.len()
        );
        let progress_metric = counter!(TOTAL_PROGRESS_METRIC);

        let mut mass_index = Self::with_capacity(intervals.len());

        for interval in intervals.into_iter() {
            tracing::info!(
                "Building mass index for interval [{}, {})",
                mass_to_float(interval.0),
                mass_to_float(interval.1)
            );
            let partial_mass_index = Self::partial_build(
                protein_access.clone(),
                protease.clone(),
                num_threads,
                interval,
            )
            .await?;
            mass_index.push(partial_mass_index);
            progress_metric.increment(1);
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
        let protein_count = protein_access.count().await?;

        let now = std::time::Instant::now();

        let protein_queue: Arc<ArrayQueue<Option<Arc<Protein>>>> =
            Arc::new(ArrayQueue::new(num_threads.get() * 3));
        let pair_queue: PairQueue = Arc::new(ArrayQueue::new(num_threads.get()));
        let partial_progress_metric = Arc::new(metrics::counter!(PARTIAL_PROGRESS_METRIC));
        partial_progress_metric.absolute(0);

        let collector_task = {
            let pair_queue = pair_queue.clone();
            tokio::spawn(async move {
                // Intermediate: unsorted flat (mass, protein_id) pairs from all threads, packed
                // into 12 bytes each. Sorted + deduped after threads finish, then converted to CSR
                // in one pass that reuses this buffer's allocation for `protein_ids`.
                let mut pairs: Vec<MassPidPair> = Vec::with_capacity(protein_count);

                loop {
                    let batch = match pair_queue.pop() {
                        Some(Some(batch)) => batch,
                        Some(None) => break,
                        None => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };

                    pairs.extend(batch);
                }

                Ok::<_, Error>(pairs)
            })
        };

        let mut proteins = protein_access.all().await?;

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let protease = protease.clone();
                let protein_queue = protein_queue.clone();
                let partial_progress_metric = partial_progress_metric.clone();
                let mass_interval = interval.clone();
                let pair_queue = pair_queue.clone();

                tokio::spawn(async move {
                    let mut local_pairs: HashSet<MassPidPair> =
                        HashSet::with_capacity(LOCAL_MASS_LIMIT);

                    loop {
                        let protein = match protein_queue.pop() {
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
                                local_pairs.insert(MassPidPair::new(mass, protein_id));
                                Ok(())
                            })?;

                        if local_pairs.len() >= LOCAL_MASS_LIMIT {
                            let mut batch = Some(std::mem::replace(
                                &mut local_pairs,
                                HashSet::with_capacity(LOCAL_MASS_LIMIT),
                            ));

                            loop {
                                batch = match pair_queue.push(batch) {
                                    Ok(()) => break,
                                    Err(errored_batched) => {
                                        tokio::time::sleep(Duration::from_millis(50)).await;
                                        errored_batched
                                    }
                                };
                            }
                        }

                        partial_progress_metric.increment(1);
                    }

                    let mut batch = Some(local_pairs);
                    loop {
                        batch = match pair_queue.push(batch) {
                            Ok(()) => break,
                            Err(errored_batched) => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                errored_batched
                            }
                        };
                    }

                    Ok::<_, Error>(())
                })
            })
            .collect::<Vec<_>>();

        while let Some(protein) = proteins.next().await.transpose()? {
            let mut protein = Some(protein);
            loop {
                protein = match protein_queue.push(protein) {
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
                if protein_queue.push(None).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        for thread in digest_and_insertion_threads {
            thread.await.map_err(|err| Error::Join(err.to_string()))??;
        }

        loop {
            match pair_queue.push(None) {
                Ok(()) => break,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }

        let mut pairs = collector_task
            .await
            .map_err(|err| Error::Join(err.to_string()))??;

        tracing::info!(
            "Build for interval [{}, {}) produced {} pairs in {:}s",
            mass_to_float(mass_interval.0),
            mass_to_float(mass_interval.1),
            pairs.len(),
            now.elapsed().as_secs_f32()
        );

        // Sort by (mass_idx, protein_id) and deduplicate so each pair is unique.
        let now = std::time::Instant::now();
        pairs.par_sort_unstable_by(|a, b| {
            a.mass().cmp(&b.mass()).then_with(|| a.pid().cmp(&b.pid()))
        });
        pairs.dedup_by(|a, b| a.mass() == b.mass() && a.pid() == b.pid());
        tracing::info!(
            "Sorting and deduplication for interval [{}, {}) produced {} unique pairs in {:}s",
            mass_to_float(mass_interval.0),
            mass_to_float(mass_interval.1),
            pairs.len(),
            now.elapsed().as_secs_f32()
        );

        // Build CSR from the sorted pairs in a single pass, reusing the pair buffer's own
        // allocation for `protein_ids` so we never hold both buffers at once.
        //
        // Each `MassPidPair` is exactly three `i32` slots and the id we keep is one `i32`.
        // Reading pair `i` touches slots `3*i..3*i+3` and writes its id to slot `i`. Since
        // `i <= 3*i`, the write cursor always trails the read cursor and never clobbers a
        // pair that has not been read yet.
        let len = pairs.len();
        let capacity = pairs.capacity();
        let mut masses: Vec<i64> = Vec::new();
        let mut indptr: Vec<u64> = vec![0u64];

        // All access goes through this single raw pointer (no reborrow of `pairs`) so the
        // aliasing between the reads and the in-place writes stays well-defined.
        let slots = pairs.as_mut_ptr() as *mut i32;
        for i in 0..len {
            // SAFETY: `3*i + 2 < 3*len <= 3*capacity` i32 slots, all in bounds and 4-byte aligned.
            let (mass, pid) = unsafe {
                let lo = slots.add(3 * i).read() as u32;
                let hi = slots.add(3 * i + 1).read() as u32;
                let pid = slots.add(3 * i + 2).read();
                ((((hi as u64) << 32) | lo as u64) as i64, pid)
            };
            if masses.last() != Some(&mass) {
                masses.push(mass);
                indptr.push(*indptr.last().unwrap());
            }
            *indptr.last_mut().unwrap() += 1;
            // SAFETY: `i <= 3*i`, so slot `i` was already read on this (or an earlier) pass.
            unsafe { slots.add(i).write(pid) };
        }

        // Hand the (now id-only) allocation to a `Vec<i32>`. The buffer holds `3*capacity`
        // i32 slots; the const asserts on `MassPidPair` guarantee it has the same size and
        // alignment as `[i32; 3]`, so this matches the layout `Vec<i32>` will free.
        std::mem::forget(pairs);
        let protein_ids = unsafe { Vec::from_raw_parts(slots, len, capacity * 3) };

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
