// 3rd party imports
use anyhow::{bail, Result};
use indicatif::ProgressStyle;
use tracing::{debug, info_span, warn, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

// Internal imports
use crate::chemistry::amino_acid::INTERNAL_TRYPTOPHAN;

pub const PARTITION_TOLERANCE: f64 = 0.01;

lazy_static! {
    static ref MAX_MASS: i64 = INTERNAL_TRYPTOPHAN.get_mono_mass_int() * 60;
}

/// Returns the partition for the given mass
///
/// # Arguments
/// * `partition_limits` - A vector of partition limits (cannot be empty)
/// * `mass` - The mass to get the partition for (must be larger or equal to 0 and small than the last partition limit)
///
pub fn get_mass_partition(partition_limits: &Vec<i64>, mass: i64) -> Result<usize> {
    // Check for errors in the given arguments
    if partition_limits.is_empty() {
        bail!("Partition limits are empty");
    }
    if mass < 0 {
        bail!("Mass cannot be negative");
    }
    if mass > *partition_limits.last().unwrap() {
        bail!("Mass is too large to be partitioned");
    }

    // Binary search for the partition
    let mut start: usize = 0;
    let mut end: usize = partition_limits.len() - 1;
    while start <= end {
        let position = (start + end) / 2;
        // Return the position if the mass is in the partition or the first partition
        if position == 0
            || mass > partition_limits[position - 1] && mass <= partition_limits[position]
        {
            return Ok(position);
        } else if mass < partition_limits[position] {
            end = position - 1;
        } else {
            start = position + 1;
        }
    }
    bail!("No partition for given mass was found");
}

pub struct PeptidePartitioner;

impl<'a> PeptidePartitioner {
    /// Creates a new peptide partitioning based on the peptide mass counts
    ///
    /// # Arguments
    /// * `mass_counts` - A vector of tuples containing the mass and the number of peptides
    /// * `num_partitions` - The number of partitions to create
    /// * `partition_tolerance` - The balancing tolerance for each part to prevent overflow is entirely put into last partition (0.0 - 1.0, default: 0.01)
    ///
    pub fn create_partition_limits(
        mass_counts: &Vec<(i64, u64)>,
        num_partitions: u64,
        partition_tolerance: Option<f64>,
    ) -> Result<Vec<i64>> {
        // Set partition tolerance. Throw errors if tolerance is not in the correct range
        let partition_tolerance = match partition_tolerance {
            Some(tolerance) => {
                if tolerance < 0.0 || tolerance > 1.0 {
                    bail!("Partition tolerance must be between 0.0 and 1.0");
                }

                tolerance
            }
            None => PARTITION_TOLERANCE,
        };
        let mut partition_limits: Vec<i64> = vec![0; num_partitions as usize];
        let peptide_count: u64 = mass_counts.iter().map(|(_, count)| count).sum();

        let mut peptides_per_partition =
            (peptide_count as f64 / num_partitions as f64).ceil() as u64;
        peptides_per_partition +=
            (peptides_per_partition as f64 * partition_tolerance).ceil() as u64;

        debug!("Peptide count: {}", peptide_count);
        debug!("Peptides per partition: {}", peptides_per_partition);

        // Create progress bars
        let header_span = info_span!("counting masses");
        header_span.pb_set_style(&ProgressStyle::default_bar());
        header_span.pb_set_length(mass_counts.len() as u64);
        let header_span_enter = header_span.enter();

        let mut partition_idx: usize = 0;
        let mut partition_content: u64 = 0;
        for (mass, peptide_count) in mass_counts {
            loop {
                if partition_content + *peptide_count <= peptides_per_partition
                    || partition_idx == partition_limits.len() - 1
                {
                    partition_content += *peptide_count;
                    partition_limits[partition_idx] = *mass;
                    break;
                } else {
                    partition_idx += 1;
                    partition_content = 0;
                }
            }
            Span::current().pb_inc(1);
        }

        let zeroes = partition_limits.iter().filter(|limit| **limit == 0).count();

        if zeroes > 0 {
            warn!(
                "There are {} empty partitions. This can happen if the number of partition and the partition tolerance is too large for the number of peptides. The empty partitions are removed which results in less partitions. This has no effect on the results.",
                zeroes
            );
            partition_limits.retain(|limit| *limit != 0);
        }

        match partition_limits.last_mut() {
            Some(limit) => *limit = *MAX_MASS,
            None => bail!("Partition limits are empty"),
        }

        // Drop the progress bar
        std::mem::drop(header_span_enter);
        std::mem::drop(header_span);

        Ok(partition_limits)
    }
}

#[cfg(test)]
mod test {
    // std imports
    use std::path::PathBuf;

    // 3rd party imports
    use dihardts_omicstools::proteomics::proteases::functions::get_by_name as get_protease_by_name;
    use tracing_test::traced_test;

    // internal imports
    use super::*;
    use crate::tools::peptide_mass_counter::PeptideMassCounter;

    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    async fn test_partitioning() {
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

        let partition_limits =
            PeptidePartitioner::create_partition_limits(&mass_counts, 10, None).unwrap();

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .from_path("test_files/mouse_partitioning.tsv")
            .unwrap();

        let expected_partition_limits: Vec<i64> =
            reader.deserialize().map(|line| line.unwrap()).collect();

        assert_eq!(partition_limits, expected_partition_limits);
    }

    #[test]
    fn test_get_partition() {
        let partition_limits: Vec<i64> = vec![10, 20, 30, 40, 50, 60, 70, 80];
        // Edge case 0
        assert_eq!(get_mass_partition(&partition_limits, 0).unwrap(), 0);
        // Check all partitions
        for (partition, limit) in partition_limits[0..partition_limits.len() - 1]
            .iter()
            .enumerate()
        {
            // In partition
            assert_eq!(
                get_mass_partition(&partition_limits, *limit - 1).unwrap(),
                partition
            );
            // Exact upper limit
            assert_eq!(
                get_mass_partition(&partition_limits, *limit).unwrap(),
                partition
            );
            // Above upper limit
            assert_eq!(
                get_mass_partition(&partition_limits, *limit).unwrap(),
                partition
            );
        }
        // Check upper partition
        let partition = partition_limits.len() - 1;
        let limit = partition_limits[partition];
        // In partition
        assert_eq!(
            get_mass_partition(&partition_limits, limit - 1).unwrap(),
            partition
        );
        // Exact upper limit
        assert_eq!(
            get_mass_partition(&partition_limits, limit).unwrap(),
            partition
        );
        // Above upper limit should throw error as mass i above the last partition limit
        assert!(get_mass_partition(&partition_limits, limit + 1).is_err());

        // Expect error for negative mass
        assert!(get_mass_partition(&partition_limits, -1).is_err());
        // Expect error for empty partition limits
        assert!(get_mass_partition(&vec![], 10).is_err());
    }
}
