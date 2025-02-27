use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::sleep,
    time::Duration,
};

use anyhow::{bail, Result};
use dihardts_cstools::bloom_filter::BloomFilter;
use dihardts_omicstools::proteomics::proteases::{
    functions::get_by_name as get_protease_by_name, protease::Protease,
};
use fallible_iterator::FallibleIterator;
use sysinfo::{System, SystemExt};
use tracing::{debug, info, trace};

use crate::{
    chemistry::amino_acid::{calc_sequence_mass_int, INTERNAL_TRYPTOPHAN},
    entities::protein::Protein,
    io::uniprot_text::reader::Reader,
    tools::{
        omicstools::remove_unknown_from_digest, peptide_partitioner::get_mass_partition,
        progress_monitor::ProgressMonitor, queue_monitor::QueueMonitor,
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
        bloom_filter_arc: Arc<Vec<Mutex<BloomFilter>>>,
        partition_limits: Arc<Vec<i64>>,
        partitions_counters: Arc<Vec<Mutex<HashMap<i64, u64>>>>,
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
                bloom_filter_arc,
                partition_limits,
                partitions_counters,
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
        bloom_filter_arc: Arc<Vec<Mutex<BloomFilter>>>,
        partition_limits: Arc<Vec<i64>>,
        partitions_counters: Arc<Vec<Mutex<HashMap<i64, u64>>>>,
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
            trace!("Thread {} got protein {}", tid, protein.get_accession());

            // Digest protein, keep only sequences
            let peptides: Vec<String> = match remove_peptides_containing_unknown {
                true => remove_unknown_from_digest(protease.cleave(protein.get_sequence())?)
                    .map(|pep| Ok(pep.get_sequence().to_string()))
                    .collect()?,
                false => protease
                    .cleave(protein.get_sequence())?
                    .map(|pep| Ok(pep.get_sequence().to_string()))
                    .collect()?,
            };

            // Calc mass per seqeunce and sort peptides into partitions
            let mut peptides_sorted_by_partition = vec![Vec::new(); partition_limits.len()];

            for sequence in peptides.into_iter() {
                let mass = calc_sequence_mass_int(sequence.as_str())?;
                let partition = get_mass_partition(&partition_limits, mass)?;
                peptides_sorted_by_partition[partition].push((mass, sequence));
            }

            // Add peptides to bloom filter and count them
            for (partition_id, peptides) in peptides_sorted_by_partition.into_iter().enumerate() {
                if peptides.is_empty() {
                    continue;
                }
                let mut bloom_filter = bloom_filter_arc[partition_id].lock().unwrap();
                let mut partition = partitions_counters[partition_id].lock().unwrap();
                for (mass, sequence) in peptides.into_iter() {
                    if bloom_filter.contains(&sequence)? {
                        continue;
                    }
                    bloom_filter.add(&sequence)?;
                    *partition.entry(mass).or_insert(0) += 1;
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
    /// * `usable_memory_fraction` - Fraction of the available memory to use
    /// * `num_threads` - Number of threads to use
    /// * `initial_num_partitions` - Initial number of partitions for counting
    ///
    pub async fn count(
        protein_file_paths: &[PathBuf],
        protease: &dyn Protease,
        remove_peptides_containing_unknown: bool,
        false_positive_probability: f64,
        usable_memory_fraction: f64,
        num_threads: usize,
        initial_num_partitions: usize,
    ) -> Result<Vec<(i64, u64)>> {
        // Count number of proteins in files
        info!("Counting proteins ...");

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

        // Create a counter for each partition
        let partitions_counters: Vec<Mutex<HashMap<i64, u64>>> = partition_limits
            .iter()
            .map(|_| Mutex::new(HashMap::new()))
            .collect();

        // Create the bloom filter for counting peptides. Use the allowed fraction of available memory
        let usable_ram = System::new_all().available_memory() as f64 * usable_memory_fraction;
        let usable_ram_per_partition = (usable_ram / partition_limits.len() as f64).floor() as u64;
        let bloom_filters = (0..partition_limits.len())
            .map(|_| {
                BloomFilter::new_by_size_and_fp_prob(
                    usable_ram_per_partition * 8,
                    false_positive_probability,
                )
            })
            .collect::<Result<Vec<BloomFilter>>>()?;

        // Put everything in Arcs
        let partition_limits = Arc::new(partition_limits);
        let partitions_counters = Arc::new(partitions_counters);
        let bloom_filters: Arc<Vec<Mutex<BloomFilter>>> =
            Arc::new(bloom_filters.into_iter().map(Mutex::new).collect());

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
                    bloom_filters.clone(),
                    partition_limits.clone(),
                    partitions_counters.clone(),
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
        let mut partitions_counters: Vec<(i64, u64)> = Arc::try_unwrap(partitions_counters)
            .unwrap()
            .into_iter()
            .flat_map(|counter| counter.into_inner().unwrap().into_iter())
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
            0.02,
            0.3,
            20,
            40,
        )
        .await
        .unwrap();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .from_path(Path::new("test_files/mouse_masses.tsv"))
            .unwrap();

        let expected_mass_counts = reader
            .deserialize()
            .map(|line| Ok(line?))
            .collect::<Result<Vec<(i64, u64)>>>()
            .unwrap();

        assert_eq!(mass_counts, expected_mass_counts);
    }
}
