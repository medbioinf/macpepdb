use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::sleep,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use dihardts_cstools::bloom_filter::BloomFilter;
use dihardts_omicstools::proteomics::proteases::{
    functions::get_by_name as get_protease_by_name, protease::Protease,
};
use fallible_iterator::FallibleIterator;
use futures::future::join_all;
use indicatif::ProgressStyle;
use sysinfo::{System, SystemExt};
use tokio::task::JoinHandle;
use tracing::{debug, info, info_span, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::{
    chemistry::amino_acid::{calc_sequence_mass_int, INTERNAL_TRYPTOPHAN},
    entities::protein::Protein,
    io::uniprot_text::reader::Reader,
    tools::{omicstools::remove_unknown_from_digest, peptide_partitioner::get_mass_partition},
};

lazy_static! {
    static ref MAX_MASS: i64 = INTERNAL_TRYPTOPHAN.get_mono_mass_int() * 60;
    static ref PROTEIN_QUEUE_WRITE_SLEEP_TIME: Duration = Duration::from_millis(100);
    static ref PROTEIN_QUEUE_READ_SLEEP_TIME: Duration = Duration::from_secs(2);
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
    ///
    pub async fn count<'a>(
        protein_file_paths: &'a Vec<PathBuf>,
        protease: &dyn Protease,
        remove_peptides_containing_unknown: bool,
        false_positive_probability: f64,
        usable_memory_fraction: f64,
        num_threads: usize,
    ) -> Result<Vec<(i64, u64)>> {
        // Count number of proteins in files
        debug!("Counting proteins ...");
        let mut protein_ctr: usize = 0;
        for path in protein_file_paths.iter() {
            debug!("... {}", path.display());
            protein_ctr += Reader::new(path, 1024)?.count_proteins()?;
        }
        debug!("... {} proteins in total", protein_ctr);

        // Calculate the max mass and width of each partition
        let max_mass = INTERNAL_TRYPTOPHAN.get_mono_mass_int() * 60;
        let mass_step = max_mass / (num_threads * 4) as i64;
        debug!("max mass: {}", max_mass);
        debug!("mass step: {}", mass_step);

        // Create partition_limits
        let mut partition_limits: Vec<i64> = (mass_step..=max_mass)
            .step_by(mass_step as usize)
            .into_iter()
            .map(|i| i)
            .collect();
        // Add last partition limit if it is not already max_mass
        if partition_limits.last().unwrap() != &max_mass {
            partition_limits.push(max_mass);
        }

        // Create a counter for each partition
        let partitions_counters: Vec<Arc<Mutex<HashMap<i64, u64>>>> = partition_limits
            .iter()
            .map(|_| Arc::new(Mutex::new(HashMap::new())))
            .collect();

        // Create the bloom filter for counting peptides. Use the allowed fraction of available memory
        let usable_ram =
            (System::new_all().available_memory() as f64 * usable_memory_fraction) as u64;
        let bloom_filter =
            BloomFilter::new_by_size_and_fp_prob(usable_ram * 8, false_positive_probability)?;

        // Put everything in Arcs
        let partition_limits = Arc::new(partition_limits);
        let partitions_counters = Arc::new(partitions_counters);
        let bloom_filter_arc = Arc::new(Mutex::new(bloom_filter));

        // Create the the protein queue
        let protein_queue_size = num_threads * 300;
        let protein_queue_arc: Arc<Mutex<Vec<Protein>>> =
            Arc::new(Mutex::new(Vec::with_capacity(protein_queue_size)));

        // Stop flag for signaling threads to stop once the protein queue is emptied
        let stop_flag = Arc::new(AtomicBool::new(false));

        // Start threads
        let mut ctr_thread_handlers: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            // Make copies of shared resources to move into thread
            let thread_protein_queue_arc = protein_queue_arc.clone();
            let thread_stop_flag = stop_flag.clone();
            let thread_protease = get_protease_by_name(
                protease.get_name(),
                protease.get_min_length(),
                protease.get_max_length(),
                protease.get_max_missed_cleavages(),
            )?;
            let thread_bloom_filter_arc = bloom_filter_arc.clone();
            let remove_peptides_containing_unknown = remove_peptides_containing_unknown.clone();
            let thread_partition_limits = partition_limits.clone();
            let thread_partitions_counters = partitions_counters.clone();
            let ctr_thread_handler = tokio::spawn(async move {
                Self::count_thread(
                    thread_protein_queue_arc,
                    thread_stop_flag,
                    thread_protease,
                    remove_peptides_containing_unknown,
                    thread_bloom_filter_arc,
                    thread_partition_limits,
                    thread_partitions_counters,
                )
                .await
            });
            ctr_thread_handlers.push(ctr_thread_handler);
        }

        // Create progress bars
        let header_span = info_span!("counting masses");
        header_span.pb_set_style(&ProgressStyle::default_bar());
        header_span.pb_set_length(protein_ctr as u64);
        let header_span_enter = header_span.enter();

        let mut last_wait_instant: Option<Instant> = None;

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
                        if last_wait_instant.is_some_and(|x| (Instant::now() - x).as_secs() > 60) {
                            debug!("Producer sleeping since 1 minute");
                        }
                        last_wait_instant = Some(Instant::now());
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
                    last_wait_instant = None;
                    protein_queue.push(protein);
                    // Increase progress bar
                    Span::current().pb_inc(1);
                    break;
                }
            }
        }
        debug!("All proteins read, waiting for threads to finish");

        // Drop the progress bar
        std::mem::drop(header_span_enter);
        std::mem::drop(header_span);

        // Set stop flag to true
        stop_flag.store(true, Ordering::Relaxed);

        debug!("Waiting for threads to finish");
        join_all(ctr_thread_handlers).await;

        let mut partitions_counters: Vec<(i64, u64)> = Arc::try_unwrap(partitions_counters)
            .unwrap()
            .into_iter()
            .flat_map(|counter| {
                Arc::try_unwrap(counter)
                    .unwrap()
                    .into_inner()
                    .unwrap()
                    .into_iter()
            })
            .collect();

        partitions_counters.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(partitions_counters)
    }

    async fn count_thread(
        protein_queue_arc: Arc<Mutex<Vec<Protein>>>,
        stop_flag: Arc<AtomicBool>,
        protease: Box<dyn Protease>,
        remove_peptides_containing_unknown: bool,
        bloom_filter_arc: Arc<Mutex<BloomFilter>>,
        partition_limits: Arc<Vec<i64>>,
        partitions_counters: Arc<Vec<Arc<Mutex<HashMap<i64, u64>>>>>,
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
            let peptides: Vec<String> = match remove_peptides_containing_unknown {
                true => remove_unknown_from_digest(protease.cleave(&protein.get_sequence())?)
                    .map(|pep| Ok(pep.get_sequence().to_string()))
                    .collect()?,
                false => protease
                    .cleave(&protein.get_sequence())?
                    .map(|pep| Ok(pep.get_sequence().to_string()))
                    .collect()?,
            };

            // Sort peptides by mass
            let mut peptides_sorted_by_partitions: Vec<Vec<String>> =
                vec![Vec::new(); partition_limits.len()];
            // Acquire lock on bloom filter to check peptide existence
            let mut bloom_filter = bloom_filter_arc.lock().unwrap();
            // Iterate over peptides, if they do not already exists, add them to the bloom filter and partition
            for sequence in peptides.into_iter() {
                if bloom_filter.contains(sequence.as_str())? {
                    continue;
                }
                bloom_filter.add(sequence.as_str())?;
                let mass = calc_sequence_mass_int(sequence.as_str())?;
                let partition = get_mass_partition(&partition_limits, mass)?;
                peptides_sorted_by_partitions[partition].push(sequence);
            }

            for (partition, peptides) in peptides_sorted_by_partitions.into_iter().enumerate() {
                if peptides.is_empty() {
                    continue;
                }
                let mut partition_ctr_map = partitions_counters[partition].lock().unwrap();
                for sequence in peptides.into_iter() {
                    let mass = calc_sequence_mass_int(sequence.as_str())?;
                    *partition_ctr_map.entry(mass).or_insert(0) += 1;
                }
            }
        }
        Ok(())
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

    #[tokio::test]
    async fn test_counting() {
        let protease = get_protease_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();

        let mass_counts = PeptideMassCounter::count(
            &vec![PathBuf::from("test_files/mouse.txt")],
            protease.as_ref(),
            true,
            0.02,
            0.3,
            20,
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