use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::sleep,
    time::Duration,
    vec,
};

use dihardts_cstools::bloom_filter::BloomFilter;
use dihardts_omicstools::proteomics::proteases::{
    functions::get_by_name as get_protease_by_name, protease::Protease,
};
use fallible_iterator::FallibleIterator;
use parking_lot::RwLock;
use sysinfo::{System, SystemExt};
use tracing::{debug, info};

use crate::{
    chemistry::amino_acid::INTERNAL_TRYPTOPHAN,
    entities::{peptide::Peptide, protein::Protein},
    io::uniprot_text::reader::Reader,
    tools::{
        errors::peptide_mass_counter_error::PeptideMassCounterError,
        omicstools::{convert_to_internal_peptide, remove_unknown_from_digest},
        progress_monitor::ProgressMonitor,
        queue_monitor::QueueMonitor,
    },
};

lazy_static! {
    static ref MAX_MASS: i64 = INTERNAL_TRYPTOPHAN.get_mono_mass_int() * 60;
    static ref PROTEIN_QUEUE_WRITE_SLEEP_TIME: Duration = Duration::from_millis(100);
    static ref PROTEIN_QUEUE_READ_SLEEP_TIME: Duration = Duration::from_secs(2);
}

struct MassCountThread {
    thread: Option<std::thread::JoinHandle<Result<(), PeptideMassCounterError>>>,
    stop_flag: Arc<AtomicBool>,
}

impl MassCountThread {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tid: usize,
        protein_queue_arc: Arc<Mutex<Vec<Protein>>>,
        protease: Box<dyn Protease>,
        remove_peptides_containing_unknown: bool,
        bloom_filter: Arc<BloomFilter<AtomicU8>>,
        mass_counters: Arc<RwLock<HashMap<i64, AtomicU64>>>,
        processed_proteins: Arc<AtomicUsize>,
        stop_flag: Option<Arc<AtomicBool>>,
    ) -> Self {
        let stop_flag = match stop_flag {
            Some(stop_flag) => stop_flag,
            None => Arc::new(AtomicBool::new(false)),
        };
        let thread_stop = stop_flag.clone();
        let thread = Some(std::thread::spawn(move || {
            Self::work(
                tid,
                protein_queue_arc,
                protease,
                remove_peptides_containing_unknown,
                bloom_filter,
                mass_counters,
                processed_proteins,
                thread_stop,
            )
        }));

        Self { thread, stop_flag }
    }

    fn join(&mut self) -> Result<(), PeptideMassCounterError> {
        if self.thread.is_none() {
            return Ok(());
        }
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(PeptideMassCounterError::ProteinCountThreadPanicError)??;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn work(
        tid: usize,
        protein_queue_arc: Arc<Mutex<Vec<Protein>>>,
        protease: Box<dyn Protease>,
        remove_peptides_containing_unknown: bool,
        bloom_filter: Arc<BloomFilter<AtomicU8>>,
        mass_counters: Arc<RwLock<HashMap<i64, AtomicU64>>>,
        processed_proteins: Arc<AtomicUsize>,
        stop_flag: Arc<AtomicBool>,
    ) -> Result<(), PeptideMassCounterError> {
        let mut wait_for_queue = false;
        loop {
            // Wait for protein queue to fill up
            if wait_for_queue {
                sleep(*PROTEIN_QUEUE_READ_SLEEP_TIME);
                wait_for_queue = false;
            }
            // Try to get protein from queue
            // if queue is empty wait for queue to fill up
            // if also stop flag is set, break
            let protein = match protein_queue_arc.lock().unwrap().pop() {
                Some(protein) => protein,
                None => {
                    if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    } else {
                        wait_for_queue = true;
                        continue;
                    }
                }
            };

            // Digest protein, keep only sequences
            let peptides = convert_to_internal_peptide(
                match remove_peptides_containing_unknown {
                    true => Box::new(remove_unknown_from_digest(
                        protease
                            .cleave(protein.get_sequence())
                            .map_err(PeptideMassCounterError::ProteaseCleavageError)?,
                    )),
                    false => Box::new(
                        protease
                            .cleave(protein.get_sequence())
                            .map_err(PeptideMassCounterError::ProteaseCleavageError)?,
                    ),
                },
                &[i64::MAX],
                &protein,
            )
            .collect::<HashSet<Peptide>>()
            .map_err(PeptideMassCounterError::PeptideConversionError)?;

            for peptide in peptides.into_iter() {
                if !bloom_filter.add_aliased(peptide.get_sequence().as_bytes())? {
                    if let Some(counter) = mass_counters.read().get(&peptide.get_mass()) {
                        counter.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    mass_counters
                        .write()
                        .entry(peptide.get_mass())
                        .or_insert_with(|| AtomicU64::new(0))
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            processed_proteins.fetch_add(1, Ordering::Relaxed);
        }
        debug!("Thread finished {}", tid);
        Ok(())
    }
}

/// Counts the number of peptides per mass in the given protein files,
///
pub struct PeptideMassCounter;

impl PeptideMassCounter {
    /// Counts the number of peptides per mass in the given protein files.
    /// Returns a map of mass to peptide count.
    ///
    /// # Arguments
    /// * `protein_file_paths` - Paths to the protein files
    /// * `protease` - protease used for digestion
    /// * `remove_peptides_containing_unknown` - If true removes peptides containing unknown amino acids
    /// * `false_positive_probability` - False positive probability of the bloom filter
    /// * `bloom_filter_length` - Length of the bloom filter in bytes
    /// * `num_threads` - Number of threads to use
    /// * `initial_num_partitions` - Initial number of partitions for counting
    ///
    pub async fn count(
        protein_file_paths: &[PathBuf],
        protease: &dyn Protease,
        remove_peptides_containing_unknown: bool,
        false_positive_probability: f64,
        bloom_filter_length: u64,
        num_threads: usize,
        initial_num_partitions: usize,
    ) -> Result<Vec<(i64, u64)>, PeptideMassCounterError> {
        // Count number of proteins in files
        info!("Counting proteins ...");

        if System::new_all().available_memory() < bloom_filter_length {
            return Err(PeptideMassCounterError::InsufficientMemory(
                bloom_filter_length as usize,
                System::new_all().available_memory() as usize,
            ));
        }

        let protein_ctr = Arc::new(AtomicUsize::new(0));

        let protein_file_path_queue = Arc::new(Mutex::new(protein_file_paths.to_owned()));
        let processed_files = Arc::new(AtomicUsize::new(0));
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut progress_view = ProgressMonitor::new(
            "protein counting",
            vec![processed_files.clone()],
            vec![Some(protein_file_paths.len() as u64)],
            vec!["files".to_string()],
            None,
        )
        .map_err(PeptideMassCounterError::ProgressMonitorError)?;

        let thread_handles: Vec<std::thread::JoinHandle<Result<(), PeptideMassCounterError>>> = (0
            ..num_threads)
            .map(|_| {
                let thread_protein_file_path_queue = protein_file_path_queue.clone();
                let thread_processed_files = processed_files.clone();
                let thread_protein_ctr = protein_ctr.clone();
                std::thread::spawn(move || {
                    loop {
                        let path = {
                            let mut path_queue = match thread_protein_file_path_queue.lock() {
                                Ok(path_queue) => path_queue,
                                Err(_) => {
                                    return Err(PeptideMassCounterError::ProteinQueuePoisoned);
                                }
                            };
                            match path_queue.pop() {
                                Some(path) => path,
                                None => break,
                            }
                        };
                        info!("... {}", path.display());
                        let mut reader = Reader::new(&path, 1024)
                            .map_err(PeptideMassCounterError::ProteinReaderError)?;
                        let ctr = reader
                            .count_proteins()
                            .map_err(PeptideMassCounterError::ProteinCountError)?;
                        thread_protein_ctr.fetch_add(ctr, Ordering::Relaxed);
                        thread_processed_files.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();

        // wait for counting to finish
        for thread_handle in thread_handles.into_iter() {
            thread_handle
                .join()
                .map_err(PeptideMassCounterError::ProteinCountThreadPanicError)??;
        }

        // Stop progress bar
        stop_flag.store(true, Ordering::Relaxed);
        progress_view
            .stop()
            .await
            .map_err(PeptideMassCounterError::ProgressMonitorError)?;

        let protein_ctr = protein_ctr.load(Ordering::SeqCst);

        info!("... {} proteins in total", protein_ctr);

        // Calculate the max mass and width of each partition
        let max_mass = INTERNAL_TRYPTOPHAN.get_mono_mass_int()
            * protease.get_max_length().unwrap_or(60) as i64;
        let mass_step = max_mass / initial_num_partitions as i64;
        debug!("max mass: {}", max_mass);
        debug!("mass step: {}", mass_step);

        // Create partition_limits
        let mut partition_limits: Vec<i64> =
            (mass_step..=max_mass).step_by(mass_step as usize).collect();
        // Add last partition limit if it is not already max_mass
        if partition_limits.last().unwrap() != &max_mass {
            partition_limits.push(max_mass);
        }

        let mass_counters: Arc<RwLock<HashMap<i64, AtomicU64>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let bloom_filter = Arc::new(
            BloomFilter::<AtomicU8>::build()
                .with_false_positive_probability(false_positive_probability)
                .with_length(bloom_filter_length * 8) // bits
                .map_err(PeptideMassCounterError::BloomFilterError)?,
        );

        info!("{bloom_filter}");

        // Create the the protein queue
        let protein_queue_size = num_threads * 300;
        let protein_queue_arc: Arc<Mutex<Vec<Protein>>> =
            Arc::new(Mutex::new(Vec::with_capacity(protein_queue_size)));

        // Stop flags for threads
        let progress_stop_flag = Arc::new(AtomicBool::new(false));
        stop_flag.store(false, Ordering::Relaxed);

        let processed_proteins = Arc::new(AtomicUsize::new(0));

        // Create progress bar
        let mut progress_view = ProgressMonitor::new(
            "",
            vec![processed_proteins.clone()],
            vec![Some(protein_ctr as u64)],
            vec!["proteins".to_string()],
            None,
        )
        .map_err(PeptideMassCounterError::ProgressMonitorError)?;

        let mut queue_monitor = QueueMonitor::new(
            "",
            vec![protein_queue_arc.clone()],
            vec![protein_queue_size as u64],
            vec!["protein queue".to_string()],
            None,
        )
        .map_err(PeptideMassCounterError::QueueMonitorError)?;

        // Start threads
        let mass_count_threads = (0..num_threads)
            .map(|tid| {
                Ok(MassCountThread::new(
                    tid,
                    protein_queue_arc.clone(),
                    get_protease_by_name(
                        protease.get_name(),
                        protease.get_min_length(),
                        protease.get_max_length(),
                        protease.get_max_missed_cleavages(),
                    )
                    .map_err(PeptideMassCounterError::ProteaseCreationError)?,
                    remove_peptides_containing_unknown,
                    bloom_filter.clone(),
                    mass_counters.clone(),
                    processed_proteins.clone(),
                    Some(stop_flag.clone()),
                ))
            })
            .collect::<Result<Vec<_>, PeptideMassCounterError>>()?;

        for protein_file_path in protein_file_paths.iter() {
            info!("Reading proteins from {}", protein_file_path.display());
            let mut reader = Reader::new(protein_file_path, 4096)
                .map_err(PeptideMassCounterError::ProteinReaderError)?;
            let mut wait_for_queue = false;
            while let Some(protein) = reader
                .next()
                .map_err(PeptideMassCounterError::NextProteinReadeError)?
            {
                loop {
                    if wait_for_queue {
                        // Wait before pushing the protein into queue
                        sleep(*PROTEIN_QUEUE_WRITE_SLEEP_TIME);
                        wait_for_queue = false;
                    }
                    // Acquire lock on protein queue
                    let mut protein_queue = match protein_queue_arc.lock() {
                        Ok(protein_queue) => protein_queue,
                        Err(_) => {
                            return Err(PeptideMassCounterError::ProteinQueuePoisoned);
                        }
                    };
                    // If protein queue is already full, set wait and try again
                    if protein_queue.len() >= protein_queue_size {
                        wait_for_queue = true;
                        continue;
                    }
                    protein_queue.push(protein);
                    break;
                }
            }
        }
        debug!("All proteins read, waiting for threads to finish");

        // Set stop flag to true
        stop_flag.store(true, Ordering::Relaxed);

        // Wait for threads to finish
        for mut thread in mass_count_threads.into_iter() {
            thread.join()?;
        }

        // Stop progress bar
        progress_stop_flag.store(true, Ordering::Relaxed);
        progress_view
            .stop()
            .await
            .map_err(PeptideMassCounterError::ProgressMonitorError)?;
        queue_monitor
            .stop()
            .await
            .map_err(PeptideMassCounterError::QueueMonitorError)?;

        debug!("Accumulate results");
        let mut partitions_counters: Vec<(i64, u64)> = Arc::try_unwrap(mass_counters)
            .unwrap()
            .into_inner()
            .into_iter()
            .map(|counter| (counter.0, counter.1.load(Ordering::SeqCst)))
            .collect();

        partitions_counters.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(partitions_counters)
    }
}

#[cfg(test)]
mod test {
    // std imports
    use std::path::{Path, PathBuf};

    // 3rd party imports
    use dihardts_omicstools::proteomics::proteases::functions::get_by_name as get_protease_by_name;

    // internal imports
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_counting() {
        let protease = get_protease_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();

        let mass_counts = PeptideMassCounter::count(
            &[PathBuf::from("test_files/mouse.txt")],
            protease.as_ref(),
            true,
            0.0000001,
            1024 * 1024 * 1024 * 3, // 3 GiB
            1,
            1,
        )
        .await
        .unwrap();

        let tsv = std::fs::read_to_string(Path::new("test_files/mouse_masses.tsv")).unwrap();
        let expected_mass_counts: Vec<(i64, u64)> = tsv
            .lines()
            .skip(1)
            .map(|line| line.trim())
            .filter(|line| !line.is_empty())
            .map(|line| {
                let mut parts = line.split("\t");
                let mass: i64 = parts.next().unwrap().parse().unwrap();
                let count: u64 = parts.next().unwrap().parse().unwrap();
                (mass, count)
            })
            .collect();

        assert_eq!(
            mass_counts.len(),
            expected_mass_counts.len(),
            "Mass count lengths do not match (got: {}, expected: {})",
            mass_counts.len(),
            expected_mass_counts.len(),
        );

        for (idx, (mass_count, expected_mass_count)) in mass_counts
            .iter()
            .zip(expected_mass_counts.iter())
            .enumerate()
        {
            assert_eq!(
                mass_count.0, expected_mass_count.0,
                "Masses do not match at index {} (expected: {}, got: {})",
                idx, expected_mass_count.0, mass_count.0
            );
            assert_eq!(
                mass_count.1, expected_mass_count.1,
                "Counts do not match at index {} (expected: {}, got: {})",
                idx, expected_mass_count.1, mass_count.1
            );
        }

        assert_eq!(mass_counts, expected_mass_counts);
    }
}
