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

use anyhow::{bail, Result};
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
    thread: Option<std::thread::JoinHandle<Result<()>>>,
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

    fn join(&mut self) -> Result<()> {
        if self.thread.is_none() {
            return Ok(());
        }
        self.stop_flag.store(true, Ordering::Relaxed);
        match self.thread.take().unwrap().join() {
            Ok(_) => Ok(()),
            Err(err) => bail!(format!("Error in count thread: {:?}", err)),
        }
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
    ) -> Result<()> {
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
                        protease.cleave(protein.get_sequence())?,
                    )),
                    false => Box::new(protease.cleave(protein.get_sequence())?),
                },
                &[i64::MAX],
                &protein,
            )
            .collect::<HashSet<Peptide>>()?;

            for peptide in peptides.into_iter() {
                if !bloom_filter.add_aliased(peptide.get_sequence().as_bytes())? {
                    if let Some(counter) = mass_counters.read().get(&peptide.get_mass()) {
                        counter.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                }

                mass_counters
                    .write()
                    .entry(peptide.get_mass())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(1, Ordering::Relaxed);
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
    ) -> Result<Vec<(i64, u64)>> {
        // Count number of proteins in files
        info!("Counting proteins ...");

        if System::new_all().available_memory() < bloom_filter_length {
            bail!("Not enough available memory to create bloom filters. Available memory: {} bytes, required memory: {} bytes", System::new_all().available_memory(), bloom_filter_length * initial_num_partitions as u64);
        }

        let mut protein_ctr: usize = 0;

        let protein_file_path_queue = Arc::new(Mutex::new(protein_file_paths.to_owned()));
        let processed_files = Arc::new(AtomicUsize::new(0));
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut progress_view = ProgressMonitor::new(
            "protein counting",
            vec![processed_files.clone()],
            vec![Some(protein_file_paths.len() as u64)],
            vec!["files".to_string()],
            None,
        )?;

        let thread_handles: Vec<std::thread::JoinHandle<Result<usize>>> = (0..num_threads)
            .map(|_| {
                let thread_protein_file_path_queue = protein_file_path_queue.clone();
                let thread_processed_files = processed_files.clone();
                std::thread::spawn(move || {
                    let mut protein_ctr: usize = 0;
                    loop {
                        let path = {
                            let mut path_queue = match thread_protein_file_path_queue.lock() {
                                Ok(path_queue) => path_queue,
                                Err(err) => {
                                    bail!(format!("Could not lock protein path queue: {}", err))
                                }
                            };
                            match path_queue.pop() {
                                Some(path) => path,
                                None => break,
                            }
                        };
                        info!("... {}", path.display());
                        let mut reader = Reader::new(&path, 1024)?;
                        protein_ctr += reader.count_proteins()?;
                        thread_processed_files.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(protein_ctr)
                })
            })
            .collect::<Vec<_>>();

        // wait for counting to finish
        for thread_handle in thread_handles.into_iter() {
            protein_ctr += match thread_handle.join() {
                Ok(protein_ctr) => protein_ctr?,
                Err(err) => bail!(format!("Error in protein counting thread: {:?}", err)),
            };
        }

        // Stop progress bar
        stop_flag.store(true, Ordering::Relaxed);
        progress_view.stop().await?;

        info!("... {} proteins in total", protein_ctr);

        // Calculate the max mass and width of each partition
        let max_mass = INTERNAL_TRYPTOPHAN.get_mono_mass_int() * 60;
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
                .map_err(anyhow::Error::from)?,
        );

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
        )?;

        let mut queue_monitor = QueueMonitor::new(
            "",
            vec![protein_queue_arc.clone()],
            vec![protein_queue_size as u64],
            vec!["protein queue".to_string()],
            None,
        )?;

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
                    )?,
                    remove_peptides_containing_unknown,
                    bloom_filter.clone(),
                    mass_counters.clone(),
                    processed_proteins.clone(),
                    Some(stop_flag.clone()),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        for protein_file_path in protein_file_paths.iter() {
            info!("Reading proteins from {}", protein_file_path.display());
            let mut reader = Reader::new(protein_file_path, 4096)?;
            let mut wait_for_queue = false;
            while let Some(protein) = reader.next()? {
                loop {
                    if wait_for_queue {
                        // Wait before pushing the protein into queue
                        sleep(*PROTEIN_QUEUE_WRITE_SLEEP_TIME);
                        wait_for_queue = false;
                    }
                    // Acquire lock on protein queue
                    let mut protein_queue = match protein_queue_arc.lock() {
                        Ok(protein_queue) => protein_queue,
                        Err(err) => bail!(format!("Could not lock protein queue: {}", err)),
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
        progress_view.stop().await?;
        queue_monitor.stop().await?;

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
