// std imports
use std::mem::drop;
use std::path::PathBuf;

// 3rd party imports
use anyhow::{bail, Result};
use dihardts_cstools::bloom_filter::BloomFilter;
use fallible_iterator::FallibleIterator;
use indicatif::ProgressStyle;
use sysinfo::{System, SystemExt};
use tracing::{debug, info, info_span, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

// internal imports
use crate::biology::digestion_enzyme::enzyme::Enzyme;
use crate::chemistry::amino_acid::{calc_sequence_mass, TRYPTOPHAN, UNKNOWN};
use crate::io::uniprot_text::reader::Reader;
// use crate::tools::bloom_filter::BloomFilter;
use crate::tools::display::bytes_to_human_readable;

lazy_static! {
    static ref MAX_MASS: i64 = TRYPTOPHAN.get_mono_mass() * 60;
}

pub fn get_mass_partition(partition_limits: &Vec<i64>, mass: i64) -> Result<usize> {
    for (idx, limit) in partition_limits.iter().enumerate() {
        if mass < *limit {
            return Ok(idx);
        }
    }
    bail!("Mass is too large to be partitioned");
}

pub struct PeptidePartitioner<'a> {
    protein_file_paths: &'a Vec<PathBuf>,
    enzyme: &'a dyn Enzyme,
    remove_peptides_containing_unknown: bool,
    false_positive_probability: f64,
    allowed_memory_usage: f64,
}

impl<'a> PeptidePartitioner<'a> {
    /// Creates a new PeptidePartitioner
    ///
    /// # Arguments
    /// * `protein_file_paths` - Paths to the protein files
    /// * `enzyme` - Enzyme used for digestion
    /// * `remove_peptides_containing_unknown` - If true removes peptides containing unknown amino acids
    /// * `false_positive_probability` - False positive probability of the bloom filter
    pub fn new(
        protein_file_paths: &'a Vec<PathBuf>,
        enzyme: &'a dyn Enzyme,
        remove_peptides_containing_unknown: bool,
        false_positive_probability: f64,
        allowed_memory_usage: f64,
    ) -> Result<Self> {
        Ok(Self {
            protein_file_paths,
            enzyme,
            remove_peptides_containing_unknown,
            false_positive_probability,
            allowed_memory_usage,
        })
    }

    /// Equalizes the partition contents by moving peptides from one partition to another
    /// and adjusting the partition limits.
    /// Returns error if the resulting partition limits are not monotonically increasing.
    ///
    /// # Arguments
    /// * `partition_contents` - Number of peptides in each partition
    /// * `partition_limits` - Mass limits of each partition
    ///
    fn equalized_partition_contents(
        &self,
        partition_contents: &mut Vec<u64>,
        partition_limits: &mut Vec<i64>,
    ) -> Result<Vec<(u64, i64)>> {
        // Calculate the min and max peptides per partition
        let peptide_count: u64 = partition_contents.iter().sum();
        let peptides_per_partition = peptide_count / partition_contents.len() as u64;

        debug!("equalizing partitions");
        debug!("peptide count: {}", peptide_count);
        debug!("peptides per partition: {}", peptides_per_partition);

        let header_span = info_span!("equalizing_partitions");
        header_span.pb_set_style(&ProgressStyle::default_bar());
        header_span.pb_set_length(partition_contents.len() as u64);
        let header_span_enter = header_span.enter();

        let mut equalized_partition_contents: Vec<(u64, i64)> =
            (0..partition_contents.len()).map(|_| (0, 0)).collect();

        let mut last_limit: i64 = -1;
        for (content, limit) in equalized_partition_contents.iter_mut() {
            let mut capacity = peptides_per_partition - *content;
            *limit = last_limit + 1;
            while capacity > 0 {
                if capacity >= partition_contents[0] {
                    *content += partition_contents[0];
                    *limit = partition_limits[0];
                    partition_contents.remove(0);
                    partition_limits.remove(0);
                } else {
                    let mut moveable_fraction = capacity as f64 / partition_contents[0] as f64;
                    let mut movable_peptides =
                        (moveable_fraction * partition_contents[0] as f64).ceil() as u64;
                    // Dealing with rounding. If movable fraction is too small and movable peptides is rounded down,
                    // movables peptides might be 0 while there is still free capacity. Resulting in an infinite loop.
                    // If it is rounded up it might come to an capacity overflow.
                    if movable_peptides == 0 || movable_peptides > capacity {
                        movable_peptides = capacity;
                        moveable_fraction = movable_peptides as f64 / partition_contents[0] as f64;
                    }
                    let mass_diff = partition_limits[0] - *limit;
                    let moveable_mass = (moveable_fraction * mass_diff as f64) as i64;
                    *content += movable_peptides;
                    partition_contents[0] -= movable_peptides;
                    *limit += moveable_mass;
                }
                last_limit = *limit;
                capacity = peptides_per_partition - *content;
            }
            Span::current().pb_inc(1);
        }

        std::mem::drop(header_span_enter);
        std::mem::drop(header_span);

        // Equalizing will set the last limit to the last partition which contains peptides.
        // So let set the last limit to the max mass.
        let last_idx = equalized_partition_contents.len() - 1;
        equalized_partition_contents[last_idx].1 = *MAX_MASS;

        // Sanity check. Limits should be monotonically increasing
        for idx in 1..equalized_partition_contents.len() {
            if equalized_partition_contents[idx].1 <= equalized_partition_contents[idx - 1].1 {
                bail!(format!(
                    "Partition limits decreasing between {} ({}) and {} ({})",
                    idx - 1,
                    equalized_partition_contents[idx - 1].1,
                    idx,
                    equalized_partition_contents[idx].1
                ));
            }
        }

        Ok(equalized_partition_contents)
    }

    /// Creates an even distribution of peptides across partitions.
    /// It tries to fit everything in memory, by
    /// creating a bloom filter for each partition using the available memory.
    ///
    /// # Arguments
    /// * `num_partitions` - Number of partitions to create
    /// * `peptide_protein_ratio_opt` - Peptide to protein ration, if None the available memory distributed multiple bloom filters
    ///
    pub fn partition(
        &self,
        num_partitions: u64,
        peptide_protein_ratio: Option<f64>,
    ) -> Result<Vec<i64>> {
        info!("Generating partition limits");

        let mut protein_ctr: usize = 0;

        debug!("Counting proteins");
        for path in self.protein_file_paths.iter() {
            debug!("... {}", path.display());
            protein_ctr += Reader::new(path, 1024)?.count_proteins()?;
        }

        // Calculate the average partition windows (60 is the max number of amino acids per peptide ,
        // so 60 times tryptophan is the heaviest peptide possible)
        let average_partition_windows = TRYPTOPHAN.get_mono_mass() * 60 / num_partitions as i64;

        let mut partition_contents: Vec<u64> = Vec::with_capacity(num_partitions as usize);
        let mut partition_limits: Vec<i64> = Vec::with_capacity(num_partitions as usize);
        for i in 0..num_partitions as i64 {
            partition_contents.push(0);
            partition_limits.push((i + 1) * average_partition_windows);
        }

        debug!("Allowed memory usage {}", &self.allowed_memory_usage);
        debug!(
            "System available memory {}",
            System::new_all().available_memory()
        );
        let usable_ram = 900000000;
        // let usable_ram =
        //     (System::new_all().available_memory() as f64 * self.allowed_memory_usage) as u64;
        debug!("Using {} RAM for bloom_filters", &usable_ram);

        let mut bloom_filters: Vec<BloomFilter> = match peptide_protein_ratio {
            Some(peptide_protein_ratio) => {
                let peptides_per_partition =
                    (peptide_protein_ratio * protein_ctr as f64) as u64 / num_partitions;
                let size_per_bloom_filter =
                    BloomFilter::calc_size(peptides_per_partition, self.false_positive_probability);
                let max_size_per_bloom_filter = usable_ram / num_partitions;
                if size_per_bloom_filter > max_size_per_bloom_filter {
                    bail!(format!(
                        "Bloom filter size of {} is larger than the maximum possible size of {}",
                        bytes_to_human_readable(size_per_bloom_filter),
                        bytes_to_human_readable(max_size_per_bloom_filter)
                    ));
                }
                debug!(
                        "create bloom filter expecting ~ {} peptides per partition...\n... with {} x {}", 
                        peptides_per_partition,
                        num_partitions,
                        bytes_to_human_readable(size_per_bloom_filter)
                    );
                (0..num_partitions)
                    .into_iter()
                    .map(|_| {
                        BloomFilter::new_by_item_count_and_fp_prob(
                            peptides_per_partition,
                            self.false_positive_probability,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            None => {
                let bloom_filter_size = usable_ram / num_partitions;
                debug!(
                    "create bloom filter using available memory {}...\n... with {} x {}",
                    bytes_to_human_readable(usable_ram),
                    num_partitions,
                    bytes_to_human_readable(bloom_filter_size)
                );
                (0..num_partitions)
                    .into_iter()
                    .map(|_| {
                        BloomFilter::new_by_size_and_fp_prob(
                            bloom_filter_size * 8,
                            self.false_positive_probability,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
        };

        debug!("creating unequaled partitions...");

        let header_span = info_span!("creating_partitions");
        header_span.pb_set_style(&ProgressStyle::default_bar());
        header_span.pb_set_length(protein_ctr as u64);
        let header_span_enter = header_span.enter();

        // Iterate over the proteins and digest them
        for protein_file_path in self.protein_file_paths {
            debug!("... {}", protein_file_path.display());
            let mut reader = Reader::new(protein_file_path, 1024)?;
            while let Some(protein) = reader.next()? {
                let mut peptide_sequences = self.enzyme.digest(protein.get_sequence());
                if self.remove_peptides_containing_unknown {
                    peptide_sequences
                        .retain(|sequence, _| !sequence.contains(*UNKNOWN.get_one_letter_code()));
                }
                for (sequence, _) in peptide_sequences.into_iter() {
                    let mass = calc_sequence_mass(sequence.as_str())?;
                    let partition = get_mass_partition(&partition_limits, mass)?;
                    if !bloom_filters[partition].contains(sequence.as_str())? {
                        bloom_filters[partition].add(sequence.as_str())?;
                        partition_contents[partition] += 1;
                    }
                }
                Span::current().pb_inc(1);
            }
        }

        std::mem::drop(header_span_enter);
        std::mem::drop(header_span);

        // drop bloom filter to free up memory
        debug!("Dropping bloom_filters");
        drop(bloom_filters);

        let equalized_partition_contents =
            self.equalized_partition_contents(&mut partition_contents, &mut partition_limits)?;
        Ok(equalized_partition_contents
            .iter()
            .map(|(_, lim)| *lim)
            .collect())
    }
}

#[cfg(test)]
mod test {
    // std imports
    use std::collections::HashSet;
    use std::path::PathBuf;

    // 3rd party imports
    use tracing_test::traced_test;

    // internal imports
    use super::*;
    use crate::biology::digestion_enzyme::trypsin::Trypsin;
    use crate::io::uniprot_text::reader::Reader;

    const NUM_PARTITIONS: u64 = 100;

    #[traced_test]
    #[test]
    fn test_partitioning() {
        let protein_file_paths = PathBuf::from("test_files/mouse.txt");
        let trypsin = Trypsin::new(2, 6, 50);
        let mut reader = Reader::new(&protein_file_paths, 4096).unwrap();
        let mut peptides: HashSet<String> = HashSet::new();

        while let Some(protein) = reader.next().unwrap() {
            let peptide_sequences = trypsin.digest(protein.get_sequence());
            for (sequence, _) in peptide_sequences {
                peptides.insert(sequence);
            }
        }

        let protein_file_paths = vec![protein_file_paths.clone()];
        let partitioner =
            PeptidePartitioner::new(&protein_file_paths, &trypsin, false, 0.0002, 0.8).unwrap();
        let partition_limits = partitioner.partition(NUM_PARTITIONS, None).unwrap(); // this will fail if the partitioning is not correct

        assert_eq!(partition_limits.len(), NUM_PARTITIONS as usize);
        assert_eq!(partition_limits[partition_limits.len() - 1], *MAX_MASS);
    }
}
