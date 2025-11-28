// std imports
use std::cmp::max;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::sleep;
use std::time::Duration;

// 3rd party imports
use anyhow::{bail, Result};
use crossbeam_queue::ArrayQueue;
use dihardts_omicstools::biology::io::taxonomy_reader::TaxonomyReader;
use dihardts_omicstools::proteomics::proteases::functions::get_by_name as get_protease_by_name;
use dihardts_omicstools::proteomics::proteases::protease::Protease;
use fallible_iterator::FallibleIterator;
use futures::future::join_all;
use futures::{pin_mut, StreamExt, TryStreamExt};
use metrics::{counter, describe_counter, describe_gauge, gauge, Unit};
use tokio::fs::create_dir_all;
use tokio::sync::mpsc::{channel, Sender};
use tokio::{pin, spawn};
use tracing::{debug, error, info, info_span, trace, warn};
use tracing_indicatif::span_ext::IndicatifSpanExt;
use tracing_indicatif::style::ProgressStyle;

// internal imports
use crate::database::generic_client::GenericClient;
use crate::database::scylla::client::Client;
use crate::database::scylla::configuration_table::ConfigurationIncompleteError;
use crate::database::scylla::migrations::run_migrations;
use crate::database::scylla::{
    configuration_table::ConfigurationTable, peptide_table::PeptideTable,
    protein_table::ProteinTable,
};
use crate::tools::message_logger::MessageLogger;
use crate::tools::metrics_monitor::{MetricsMonitor, MonitorableMetric, MonitorableMetricType};
// use crate::tools::metrics_logger::MetricsLogger;
use crate::tools::omicstools::{convert_to_internal_peptide, remove_unknown_from_digest};
use crate::tools::peptide_mass_counter::PeptideMassCounter;
use scylla::value::CqlValue;

use crate::entities::{configuration::Configuration, peptide::Peptide, protein::Protein};
use crate::io::uniprot_text::reader::Reader;
use crate::tools::peptide_partitioner::PeptidePartitioner;

use super::taxonomy_tree_table::TaxonomyTreeTable;

lazy_static! {
    static ref PROTEIN_QUEUE_WRITE_SLEEP_TIME: Duration = Duration::from_millis(100);
    static ref PROTEIN_QUEUE_READ_SLEEP_TIME: Duration = Duration::from_secs(2);
}

const MAX_INSERT_TRIES: u64 = 5;

/// Name for unprocessable proteins log file
///
const UNPROCESSABLE_PROTEINS_LOG_FILE_NAME: &str = "unprocessable_proteins.txt";

/// Counter name for processed proteins
///
pub const PROCESSED_PROTEINS_COUNTER_NAME: &str = "macpepdb_build_digestion_processed_proteins";

/// Counter name for processed peptides
///
pub const PROCESSED_PEPTIDES_COUNTER_NAME: &str = "macpepdb_build_digestion_processed_peptides";

/// Counter name for errors
///
pub const ERRORS_COUNTER_NAME: &str = "macpepdb_build_digestion_errors";

/// Counter name for unrecoverable errors
///
pub const UNRECOVERABLE_ERRORS_COUNTER_NAME: &str = "macpepdb_build_digestion_unrecoverable_errors";

/// Counter name for protein queue size
///
pub const PROTEIN_QUEUE_SIZE_COUNTER_NAME: &str = "macpepdb_build_digestion_protein_queue_size";

pub const METADATA_PROCESSED_PEPTIDES_COUNTER_NAME: &str =
    "macpepdb_build_metadata_processed_peptides";
pub const METADATA_ERRORS_COUNTER_NAME: &str = "macpepdb_build_metadata_errors";

pub const METADATA_UNRESOLVABLE_ERRORS_COUNTER_NAME: &str =
    "macpepdb_build_metadata_unresolvable_errors";

/// Number of proteins per thread in queue
///
pub const PROTEIN_QUEUE_MULTIPLICATOR: usize = 10;

/// Struct which maintains the database content.
/// * Inserts and updates proteins from given files
/// * Maintains associations between proteins and peptides
/// * Keeps metadata up to date
/// * ...
pub struct DatabaseBuild {
    database_url: String,
}

impl DatabaseBuild {
    /// Reads the saved configuration from the database or sets a new configuration if no configuration is saved.
    /// If no initial configuration is given and no configuration is saved in the database an error is thrown.
    /// If the initial configuration is used and has no partition limits, the partition limits are calculated.
    ///
    /// # Arguments
    /// * `client` - The postgres client
    /// * `protein_file_paths` - The paths to the protein files
    /// * `num_partitions` - The number of partitions
    /// * `allowed_ram_fraction` - The allowed fraction of available memory for the bloom filter during counting
    /// * `partitioner_false_positive_probability` - The false positive probability of the partitioner
    /// * `initial_configuration_opt` - The initial configuration
    ///
    pub async fn get_or_set_configuration(
        client: &mut Client,
        protein_file_paths: &[PathBuf],
        num_partitions: u64,
        allowed_ram_fraction: f64,
        partitioner_false_positive_probability: f64,
        initial_configuration_opt: Option<Configuration>,
    ) -> Result<Configuration> {
        info!("Getting or setting configuration..");
        let config_res = ConfigurationTable::select(client).await;
        // return if configuration is ok or if it is not a ConfigurationIncompleteError
        if config_res.as_ref().is_ok()
            || !config_res
                .as_ref()
                .unwrap_err()
                .is::<ConfigurationIncompleteError>()
        {
            if config_res.as_ref().is_ok() {
                info!("... configuration found in database");
            }
            return config_res;
        }
        // throw error if no initial configuration is given
        if initial_configuration_opt.is_none() {
            bail!("No configuration given and no configuration found in database.");
        }
        // unwrap is safe because of `if` above
        let initial_configuration = initial_configuration_opt.unwrap();

        if num_partitions == 0 && initial_configuration.get_partition_limits().is_empty() {
            bail!("Number partition is 0 and initial configuration has no partition limits. Without any partitions the database cannot be built.");
        }

        let new_configuration = if initial_configuration.get_partition_limits().is_empty() {
            info!("... initial configuration has no partition limits list, creating one.");
            // create digestion protease
            let protease = get_protease_by_name(
                initial_configuration.get_protease_name(),
                initial_configuration.get_min_peptide_length(),
                initial_configuration.get_max_peptide_length(),
                initial_configuration.get_max_number_of_missed_cleavages(),
            )?;
            // create partition limits
            let mass_counts = PeptideMassCounter::count(
                protein_file_paths,
                protease.as_ref(),
                initial_configuration.get_remove_peptides_containing_unknown(),
                partitioner_false_positive_probability,
                allowed_ram_fraction,
                10,
                40,
            )
            .await?;
            let partition_limits =
                PeptidePartitioner::create_partition_limits(&mass_counts, num_partitions, None)?;

            // create new configuration with partition limits
            Configuration::new(
                initial_configuration.get_protease_name().to_owned(),
                initial_configuration.get_max_number_of_missed_cleavages(),
                initial_configuration.get_min_peptide_length(),
                initial_configuration.get_max_peptide_length(),
                initial_configuration.get_remove_peptides_containing_unknown(),
                partition_limits,
            )
        } else {
            info!("... initial configuration has partition limits list, using it");
            // if partition limits were given just clone the initial configuration
            initial_configuration.clone()
        };

        // insert new_configuration
        ConfigurationTable::insert(client, &new_configuration).await?;
        info!("... new configuration saved");

        Ok(new_configuration)
    }

    /// Digests the proteins in the given files and inserts/updates the
    /// proteins and peptides in the database.
    ///
    /// # Arguments
    /// * `database_url` - The database url
    /// * `num_threads` - The number of threads
    /// * `protein_file_paths` - The paths to the protein files
    /// * `protease` - The digestion protease
    /// * `remove_peptides_containing_unknown` - Remove peptides containing unknown amino acids
    /// * `partition_limits` - The partition limits
    /// * `log_folder` - The folder where build logs are saved
    ///
    async fn protein_digestion(
        database_url: &str,
        num_threads: usize,
        protein_file_paths: &[PathBuf],
        protease: Arc<dyn Protease>,
        remove_peptides_containing_unknown: bool,
        partition_limits: Vec<i64>,
        log_folder: &Path,
    ) -> Result<usize> {
        debug!("Digesting proteins and inserting peptides");

        let protein_queue_size = num_threads * PROTEIN_QUEUE_MULTIPLICATOR;

        // Database client
        let client = Arc::new(Client::new(database_url).await?);

        // Digestion variables
        let protein_queue_arc: Arc<ArrayQueue<Protein>> =
            Arc::new(ArrayQueue::new(protein_queue_size));
        let partition_limits_arc: Arc<Vec<i64>> = Arc::new(partition_limits);
        let stop_flag = Arc::new(AtomicBool::new(false));

        // Logging variable
        describe_counter!(
            PROCESSED_PROTEINS_COUNTER_NAME,
            "Number of inserted/updated proteins"
        );
        describe_counter!(
            PROCESSED_PEPTIDES_COUNTER_NAME,
            Unit::Count,
            "Number of inserted/updated peptides"
        );
        describe_counter!(ERRORS_COUNTER_NAME, Unit::Count, "Number of errors");
        describe_gauge!(
            PROTEIN_QUEUE_SIZE_COUNTER_NAME,
            Unit::Count,
            "Size of the protein queue"
        );

        let log_stop_flag = Arc::new(AtomicBool::new(false));
        let unprocessable_proteins_log_file_path =
            log_folder.join(UNPROCESSABLE_PROTEINS_LOG_FILE_NAME);

        // Thread communication
        let (unprocessable_proteins_sender, unprocessable_proteins_receiver) =
            channel::<Protein>(1000);

        let monitorable_metrics = vec![
            MonitorableMetric::new(
                PROCESSED_PROTEINS_COUNTER_NAME.to_string(),
                MonitorableMetricType::Rate,
            ),
            MonitorableMetric::new(
                PROCESSED_PEPTIDES_COUNTER_NAME.to_string(),
                MonitorableMetricType::Rate,
            ),
            MonitorableMetric::new(ERRORS_COUNTER_NAME.to_string(), MonitorableMetricType::Rate),
            MonitorableMetric::new(
                UNRECOVERABLE_ERRORS_COUNTER_NAME.to_string(),
                MonitorableMetricType::Rate,
            ),
            MonitorableMetric::new(
                PROTEIN_QUEUE_SIZE_COUNTER_NAME.to_string(),
                MonitorableMetricType::Queue(protein_queue_size as u64),
            ),
        ];

        let mut metrics_monitor = MetricsMonitor::new(
            "macpepdb.build.digest",
            monitorable_metrics,
            "http://127.0.0.1:9494/metrics".to_string(),
        )?;

        // Unprocessable proteins logger
        let mut unprocessable_proteins_logger = MessageLogger::new(
            unprocessable_proteins_log_file_path.clone(),
            unprocessable_proteins_receiver,
            1, // Important to save each and every protein which fails
        )
        .await;

        let digestion_thread_handles = (0..num_threads)
            .map(|_| {
                // Start digestion thread
                Ok(spawn(Self::digestion_thread(
                    client.clone(),
                    protein_queue_arc.clone(),
                    partition_limits_arc.clone(),
                    stop_flag.clone(),
                    protease.clone(),
                    remove_peptides_containing_unknown,
                    unprocessable_proteins_sender.clone(),
                )))
            })
            .collect::<Result<Vec<_>>>()?;

        // Drop the original sender its not needed anymore
        drop(unprocessable_proteins_sender);

        // Reader is not async (yet) so it needs to run in a separate thread
        // so the queue is not stareved
        let thread_protein_file_paths = protein_file_paths.to_vec();
        let reader_thread: std::thread::JoinHandle<Result<()>> = std::thread::spawn(move || {
            for protein_file_path in thread_protein_file_paths {
                let mut reader = Reader::new(&protein_file_path, 4096)?;
                while let Some(protein) = reader.next()? {
                    let mut next_protein = protein;
                    loop {
                        match protein_queue_arc.push(next_protein) {
                            Ok(_) => {
                                gauge!(PROTEIN_QUEUE_SIZE_COUNTER_NAME)
                                    .set(protein_queue_arc.len() as f64);
                                break;
                            }
                            Err(protein) => {
                                next_protein = protein;
                            }
                        }
                    }
                }
            }
            Ok(())
        });

        match reader_thread.join() {
            Ok(Ok(_)) => {
                info!("Proteins queued, waiting for digestion threads to finish ...");
            }
            Ok(Err(err)) => {
                error!("Error reading protein files: {:?}", err);
                return Err(err);
            }
            Err(err) => {
                error!("Error reading protein files: {:?}", err);
                return Err(anyhow::anyhow!("Error reading protein files"));
            }
        }

        // // Set stop flag
        stop_flag.store(true, Ordering::Relaxed);

        debug!("last proteins queued, waiting for digestion threads to finish ...");

        // Wait for digestion threads to finish
        join_all(digestion_thread_handles).await;
        debug!("Digestion threads joined");

        log_stop_flag.store(true, Ordering::Relaxed);
        let num_unprocessable_proteins = unprocessable_proteins_logger.stop().await?;
        metrics_monitor.stop().await?;

        Ok(num_unprocessable_proteins)
    }

    /// Function to which digests protein, provided by a queue
    /// and inserts it along with the peptides into the database.
    ///
    /// # Arguments
    /// * `database_url` - The url of the database
    /// * `protein_queue_arc` - The queue from which the proteins are taken
    /// * `partition_limits_arc` - The partition limits
    /// * `stop_flag` - The flag which indicates if the digestion should stop
    /// * `protease` - The protease which is used for digestion
    /// * `remove_peptides_containing_unknown` - If true, peptides containing unknown amino acids are removed
    /// * `unprocessable_proteins_sender` - The sender which is used to send unprocessable proteins to the logger
    ///
    async fn digestion_thread(
        client: Arc<Client>,
        protein_queue_arc: Arc<ArrayQueue<Protein>>,
        partition_limits_arc: Arc<Vec<i64>>,
        stop_flag: Arc<AtomicBool>,
        protease: Arc<dyn Protease>,
        remove_peptides_containing_unknown: bool,
        unprocessable_proteins_sender: Sender<Protein>,
    ) -> Result<()> {
        loop {
            let protein = protein_queue_arc.pop();
            if protein.is_none() {
                if stop_flag.load(Ordering::Relaxed) {
                    trace!("Protein queue empty and stop flag set, stopping digestion thread");
                    break;
                }
                trace!("Protein queue empty, sleeping");
                continue;
            }

            let protein = protein.unwrap();
            debug!("Processing protein {}", protein.get_accession());

            let mut accession_list = protein.get_secondary_accessions().clone();
            accession_list.push(protein.get_accession().to_owned());

            let stream = ProteinTable::select(
                client.as_ref(),
                "WHERE accession IN ?",
                &[&CqlValue::List(
                    accession_list
                        .into_iter()
                        .map(|x| CqlValue::Text(x.to_owned()))
                        .collect(),
                )],
            )
            .await?;

            pin!(stream);

            let existing_protein = stream.try_next().await?;

            debug!("Existing protein {:?}", existing_protein);

            let mut tries: u64 = 0;
            loop {
                tries += 1;
                // After MAX_INSERT_TRIES is reached, we log the proteins as something may seem wrong
                if tries > MAX_INSERT_TRIES {
                    debug!("Failed to process {}", protein.get_accession());
                    unprocessable_proteins_sender.send(protein).await?;
                    break;
                }

                let upsert_result = async {
                    // or contained in secondary accessions
                    if let Some(existing_protein) = &existing_protein {
                        if existing_protein.get_updated_at() == protein.get_updated_at() {
                            return Ok(());
                        }
                        return Self::update_protein(
                            client.as_ref(),
                            &protein,
                            existing_protein,
                            protease.as_ref(),
                            remove_peptides_containing_unknown,
                            &partition_limits_arc,
                        )
                        .await;
                    } else {
                        return Self::insert_protein(
                            client.as_ref(),
                            &protein,
                            protease.as_ref(),
                            remove_peptides_containing_unknown,
                            &partition_limits_arc,
                        )
                        .await;
                    };
                }
                .await;

                match upsert_result {
                    Ok(_) => {
                        counter!(PROCESSED_PROTEINS_COUNTER_NAME).increment(1);
                        break;
                    }
                    Err(err) => {
                        let error_msg = format!(
                            "Upsert failed for `{}` (attempt {})",
                            protein.get_accession(),
                            tries
                        );
                        if tries <= MAX_INSERT_TRIES {
                            counter!(ERRORS_COUNTER_NAME).increment(1);
                            warn!("{}", error_msg);
                        } else {
                            counter!(UNRECOVERABLE_ERRORS_COUNTER_NAME).increment(1);
                            error!("{}\n{:?}\n", error_msg, err);
                        }
                        sleep(Duration::from_millis(100));
                        continue;
                    }
                };
            }
        }

        Ok(())
    }

    /// Handles the update of a protein, in case it was merged with another entry or has various changes.
    /// 1. Digests the existing_protein
    /// 2. Digests the new protein
    /// 3. Remove peptide containing unknown amino acids if remove_peptides_containing_unknown is true
    /// 4. Handle 3 different cases
    ///     1. Accession and sequence changed -> Change accession in peptides and deassociate peptides which are not contained in the new protein
    ///     2. Only accession changed -> Change accession in associated peptides
    ///     3. Only sequence changed -> Deassociate peptides which are not contained in the new protein and create new ones.
    /// 5. Update protein itself
    ///
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `updated_protein` - The updated protein
    /// * `stored_protein` - The existing protein to update stored in the database
    /// * `protease` - The protease which is used for digestion
    /// * `remove_peptides_containing_unknown` - If true, peptides containing unknown amino acids are removed
    /// * `partition_limits` - The partition limits
    /// * `prepared` - The prepared statement for updating the peptides
    ///
    #[allow(clippy::borrowed_box)]
    async fn update_protein(
        client: &Client,
        updated_protein: &Protein,
        stored_protein: &Protein,
        protease: &dyn Protease,
        remove_peptides_containing_unknown: bool,
        partition_limits: &[i64],
    ) -> Result<()> {
        let peptides_of_stored_protein = convert_to_internal_peptide(
            match remove_peptides_containing_unknown {
                true => Box::new(remove_unknown_from_digest(
                    protease.cleave(stored_protein.get_sequence())?,
                )),
                false => Box::new(protease.cleave(stored_protein.get_sequence())?),
            },
            partition_limits,
            stored_protein,
        )
        .collect::<HashSet<Peptide>>()?;

        let peptides_of_updated_protein = convert_to_internal_peptide(
            match remove_peptides_containing_unknown {
                true => Box::new(remove_unknown_from_digest(
                    protease.cleave(updated_protein.get_sequence())?,
                )),
                false => Box::new(protease.cleave(updated_protein.get_sequence())?),
            },
            partition_limits,
            updated_protein,
        )
        .collect::<HashSet<Peptide>>()?;

        // Update peptide metadata if:
        // 1. taxonomy id changed
        // 2. proteome id changed
        // 3. Review status changed
        let flag_for_metadata_update =
            Protein::is_peptide_metadata_changed(stored_protein, updated_protein);
        // If protein got a new accession (e.g. when entries were merged) and a new sequence
        if updated_protein.get_accession() != stored_protein.get_accession()
            && updated_protein.get_sequence() != stored_protein.get_sequence()
        {
            // Deassociate the peptides which are not contained in the new protein
            Self::deassociate_protein_peptides_difference(
                client,
                stored_protein,
                &peptides_of_stored_protein,
                &peptides_of_updated_protein,
            )
            .await?;

            // Update the old accession in the peptides to the new accession
            PeptideTable::update_protein_accession(
                client,
                &mut peptides_of_updated_protein.iter(),
                stored_protein.get_accession(),
                Some(updated_protein.get_accession()),
            )
            .await?;

            // Create and associate the peptides which are not contained in the existing protein
            Self::create_protein_peptide_difference(
                client,
                &peptides_of_stored_protein,
                &peptides_of_updated_protein,
            )
            .await?;
        } else if updated_protein.get_accession() != stored_protein.get_accession() {
            PeptideTable::update_protein_accession(
                client,
                &mut peptides_of_updated_protein.iter(),
                stored_protein.get_accession().as_ref(),
                Some(updated_protein.get_accession().as_ref()),
            )
            .await?;
        } else if updated_protein.get_sequence() != stored_protein.get_sequence() {
            // Deassociate the peptides which are not contained in the new protein
            Self::deassociate_protein_peptides_difference(
                client,
                stored_protein,
                &peptides_of_stored_protein,
                &peptides_of_updated_protein,
            )
            .await?;
            // Create and associate the peptides which are not contained in the existing protein
            Self::create_protein_peptide_difference(
                client,
                &peptides_of_stored_protein,
                &peptides_of_updated_protein,
            )
            .await?;
        }

        if flag_for_metadata_update {
            PeptideTable::unset_is_metadata_updated(
                client,
                &mut peptides_of_updated_protein.iter(),
            )
            .await?;
        }

        // Update protein itself
        ProteinTable::update(client, stored_protein, updated_protein).await?;
        counter!(PROCESSED_PEPTIDES_COUNTER_NAME)
            .increment(peptides_of_updated_protein.len() as u64);

        Ok(())
    }

    /// Determines peptides which are contained in the existing protein but not in the updated protein are deassociated them.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `stored_protein` - The existing protein
    /// * `peptides_from_stored_protein` - The peptides from the existing protein
    /// * `peptides_from_updated_protein` - The peptides from the updated protein
    ///
    async fn deassociate_protein_peptides_difference(
        client: &Client,
        stored_protein: &Protein,
        peptides_from_stored_protein: &HashSet<Peptide>,
        peptides_from_updated_protein: &HashSet<Peptide>,
    ) -> Result<()> {
        // Disassociate all peptides from existing protein which are not contained by the new protein
        let peptides_to_deassociate = peptides_from_stored_protein
            .difference(peptides_from_updated_protein)
            .collect::<Vec<&Peptide>>();

        if !peptides_to_deassociate.is_empty() {
            PeptideTable::update_protein_accession(
                client,
                &mut peptides_to_deassociate.into_iter(),
                stored_protein.get_accession(),
                None,
            )
            .await?;
        }
        Ok(())
    }

    /// Creates the peptides from the updated protein, which are not already stored in the database.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `peptides_from_stored_protein` - The peptides from the existing protein
    /// * `peptides_from_updated_protein` - The peptides from the updated protein
    /// * `prepared` - The prepared statement for updating the peptides
    ///
    async fn create_protein_peptide_difference(
        client: &Client,
        peptides_from_stored_protein: &HashSet<Peptide>,
        peptides_from_updated_protein: &HashSet<Peptide>,
    ) -> Result<()> {
        // Disassociate all peptides from existing protein which are not contained by the new protein
        let peptides_to_create: Vec<&Peptide> = peptides_from_updated_protein
            .difference(peptides_from_stored_protein)
            .collect::<Vec<&Peptide>>();

        if !peptides_to_create.is_empty() {
            PeptideTable::bulk_upsert(client, peptides_to_create.into_iter()).await?
        }
        Ok(())
    }

    /// Handles the insertion of a new protein.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `protein` - The protein
    /// * `protease` - The protease which is used for digestion
    /// * `remove_peptides_containing_unknown` - If true, peptides containing unknown amino acids are removed
    /// * `partition_limits` - The partition limits
    /// * `prepared` - The prepared statement for 'upserting' the peptides
    ///
    #[allow(clippy::borrowed_box)]
    async fn insert_protein(
        client: &Client,
        protein: &Protein,
        protease: &dyn Protease,
        remove_peptides_containing_unknown: bool,
        partition_limits: &[i64],
    ) -> Result<()> {
        // Digest protein
        let peptides = convert_to_internal_peptide(
            match remove_peptides_containing_unknown {
                true => Box::new(remove_unknown_from_digest(
                    protease.cleave(protein.get_sequence())?,
                )),
                false => Box::new(protease.cleave(protein.get_sequence())?),
            },
            partition_limits,
            protein,
        )
        .collect::<HashSet<Peptide>>()?;

        ProteinTable::insert(client, protein).await?;
        // PeptideTable::bulk_insert(client, &mut peptides.iter(), prepared).await?;
        trace!(
            "Protein '{}' => {} peptides",
            protein.get_accession(),
            peptides.len()
        );

        PeptideTable::bulk_upsert(client, &mut peptides.iter()).await?;
        counter!(PROCESSED_PEPTIDES_COUNTER_NAME).increment(peptides.len() as u64);

        Ok(())
    }

    /// Collecting peptide metadata from the proteins of origin
    ///
    /// # Arguments
    /// * `num_threads` - The number of threads
    /// * `database_url` - The database url
    /// * `configuration` - The configuration
    /// * `protease` - The digestion protease
    /// * `include_domains` - If true, domains are collected
    /// * `log_folder` - The folder where build logs are saved
    ///
    async fn collect_peptide_metadata(
        num_threads: usize,
        database_url: &str,
        configuration: &Configuration,
        protease: Arc<dyn Protease>,
        include_domains: bool,
    ) -> Result<()> {
        debug!("Collecting peptide metadata");
        // Process num_threads but max 2/3 of the partitions in parallel
        // Seems to work fine for smaller and larger installation.
        let num_threads = std::cmp::min(
            num_threads,
            configuration.get_partition_limits().len() / 3 * 2,
        );
        debug!("Collecting peptide metadata...");

        // (Metrics) logging variables
        describe_counter!(
            METADATA_PROCESSED_PEPTIDES_COUNTER_NAME,
            "Number of processed peptides"
        );
        describe_counter!(METADATA_ERRORS_COUNTER_NAME, "Number of errors");

        describe_counter!(
            METADATA_UNRESOLVABLE_ERRORS_COUNTER_NAME,
            "Number of unrecoverable errors"
        );

        let monitorable_metrics = vec![
            MonitorableMetric::new(
                METADATA_PROCESSED_PEPTIDES_COUNTER_NAME.to_string(),
                MonitorableMetricType::Rate,
            ),
            MonitorableMetric::new(
                METADATA_ERRORS_COUNTER_NAME.to_string(),
                MonitorableMetricType::Rate,
            ),
            MonitorableMetric::new(
                METADATA_UNRESOLVABLE_ERRORS_COUNTER_NAME.to_string(),
                MonitorableMetricType::Rate,
            ),
        ];

        let mut metrics_monitor = MetricsMonitor::new(
            "macpepdb.build.digest",
            monitorable_metrics,
            "http://127.0.0.1:9494/metrics".to_string(),
        )?;

        let partition_queue: Arc<ArrayQueue<i64>> =
            Arc::new(ArrayQueue::new(configuration.get_partition_limits().len()));

        // Metadata update variables
        for i in 0..(configuration.get_partition_limits().len() as i64) {
            match partition_queue.push(i) {
                Ok(_) => {}
                Err(_) => bail!("Could not push partition {} to partition queue", i),
            }
        }

        let configuration_arc = Arc::new(configuration.clone());

        debug!("Starting {} metadata update threads", num_threads);
        let client = Arc::new(Client::new(database_url).await?);
        let metadata_collector_thread_handles: Vec<_> = (0..num_threads * 2)
            .map(|_| {
                // Start digestion thread
                Ok(spawn(Self::collect_peptide_metadata_thread(
                    client.clone(),
                    partition_queue.clone(),
                    configuration_arc.clone(),
                    protease.clone(),
                    include_domains,
                )))
            })
            .collect::<Result<Vec<_>>>()?;

        let progress_span = info_span!("progress");
        progress_span.pb_set_message("Finished partitions");
        progress_span.pb_set_style(
            &ProgressStyle::with_template("        {msg} {wide_bar} {pos}/{len} {per_sec} ")
                .unwrap(),
        );
        progress_span.pb_set_length(configuration.get_partition_limits().len() as u64);

        while !partition_queue.is_empty() {
            progress_span.pb_set_position(
                (configuration.get_partition_limits().len() - partition_queue.len()) as u64,
            );
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }

        info!("... all partitions processed");

        drop(progress_span);

        debug!("Waiting metadata update threads to stop ...");
        // Wait for digestion threads to finish
        join_all(metadata_collector_thread_handles).await;
        debug!("... all metadata update threads stopped");

        metrics_monitor.stop().await?;
        debug!("Waiting for logging threads to stop ...");
        debug!("... all logging threads stopped");

        Ok(())
    }

    /// Collecting peptide metadata from the proteins of origin
    ///
    /// # Arguments
    /// * `database_url` - The database url
    /// * `partitions` - The partitions
    /// * `protease` - The digestion protease
    /// * `include_domains` - If true, domains are collected
    ///
    async fn collect_peptide_metadata_thread(
        client: Arc<Client>,
        partition_queue: Arc<ArrayQueue<i64>>,
        configuration: Arc<Configuration>,
        protease: Arc<dyn Protease>,
        include_domains: bool,
    ) -> Result<()> {
        let protease_cleavage_codes: Vec<char> = protease
            .get_cleavage_amino_acids()
            .iter()
            .map(|aa| *aa.get_code())
            .collect();
        let protease_cleavage_blocker_codes: Vec<char> = protease
            .get_cleavage_blocking_amino_acids()
            .iter()
            .map(|aa| *aa.get_code())
            .collect();

        while let Some(partition) = partition_queue.pop() {
            info!("Processing partition {}", partition);

            let peptide_stream = match PeptideTable::select_peptides_for_metadata_update(
                client.as_ref(),
                configuration.get_partition_limits(),
                partition,
            )
            .await
            {
                Ok(stream) => stream,
                Err(err) => {
                    counter!(METADATA_ERRORS_COUNTER_NAME).increment(1);
                    error!(
                        "Could not select peptides for partition {}: {:?}",
                        partition, err
                    );
                    continue;
                }
            };

            pin_mut!(peptide_stream);

            while let Some(peptide) = peptide_stream.next().await {
                let peptide = match peptide {
                    Ok(peptide) => peptide,
                    Err(err) => {
                        counter!(METADATA_ERRORS_COUNTER_NAME).increment(1);
                        error!("Could not get peptide from stream: {:?}", err);
                        continue;
                    }
                };

                let mut update_success = false;
                for attempt in 0..3 {
                    let protein_stream = match ProteinTable::get_proteins_of_peptide(
                        client.as_ref(),
                        &peptide,
                    )
                    .await
                    {
                        Ok(stream) => stream,
                        Err(err) => {
                            counter!(METADATA_ERRORS_COUNTER_NAME).increment(1);
                            error!(
                                "Could not select proteins for peptide {} in {attempt}. attempt: {:?}",
                                peptide.get_sequence(),
                                err
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
                            continue;
                        }
                    };

                    let associated_proteins = match protein_stream.try_collect::<Vec<_>>().await {
                        Ok(proteins) => proteins,
                        Err(err) => {
                            counter!(METADATA_ERRORS_COUNTER_NAME).increment(1);
                            error!(
                                "Could not collect proteins for peptide {} in {attempt}. attempt: {:?}",
                                peptide.get_sequence(),
                                err
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
                            continue;
                        }
                    };

                    let (
                        is_swiss_prot,
                        is_trembl,
                        taxonomy_ids,
                        unique_taxonomy_ids,
                        proteome_ids,
                        domains,
                    ) = peptide.get_metadata_from_proteins(
                        &associated_proteins,
                        &protease_cleavage_codes,
                        &protease_cleavage_blocker_codes,
                        include_domains,
                    );

                    let update_result = PeptideTable::update_metadata(
                        client.as_ref(),
                        &peptide,
                        is_swiss_prot,
                        is_trembl,
                        &taxonomy_ids,
                        &unique_taxonomy_ids,
                        &proteome_ids,
                        &domains,
                    )
                    .await;

                    match update_result {
                        Ok(_) => {
                            counter!(METADATA_PROCESSED_PEPTIDES_COUNTER_NAME).increment(1);
                            update_success = true;
                            break;
                        }
                        Err(err) => {
                            counter!(METADATA_ERRORS_COUNTER_NAME).increment(1);
                            error!(
                                "Metadata update failed for {} in in {attempt}. attempt: {:?}\n",
                                peptide.get_sequence(),
                                err
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
                        }
                    };
                }
                if !update_success {
                    counter!(METADATA_UNRESOLVABLE_ERRORS_COUNTER_NAME).increment(1);
                }
            }
        }
        Ok(())
    }

    pub async fn build_taxonomy_tree(client: &Client, taxonomy_file_path: &Path) -> Result<()> {
        debug!("Build taxonomy tree...");
        let taxonomy_tree = TaxonomyReader::new(taxonomy_file_path)?.read()?;
        TaxonomyTreeTable::insert(client, &taxonomy_tree).await?;
        Ok(())
    }

    /// Creates a new instance of the database builder for the given database
    ///
    /// # Arguments
    /// * `database_url` - URL of the database.
    ///
    pub fn new(database_url: &str) -> Self {
        Self {
            database_url: database_url.to_owned(),
        }
    }

    /// Builds / Maintains the database.
    /// 1. Builds the deserializes the taxonomy tree and saves it to the database.
    /// 2. Inserts / updates the proteins and peptides from the files
    /// 3. Collects and updates peptide metadata like taxonomies, proteomes and review status
    ///
    /// Will panic if database contains not configuration and not initial configuration is provided.
    ///
    /// # Arguments
    /// * `protein_file_paths` - Paths to the protein files.
    /// * `taxonomy_file_path` - Path to the taxonomy file.
    /// * `num_threads` - Number of threads to use.
    /// * `num_partitions` - Number of partitions to use.
    /// * `allowed_ram_usage` - Allowed RAM usage in GB for the partitioner Bloom filter.
    /// * `partitioner_false_positive_probability` - False positive probability of the partitioners Bloom filters.
    /// * `initial_configuration_opt` - Optional initial configuration.
    /// * `log_folder` - Path to the log folder.
    /// * `include_do
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn build(
        &self,
        protein_file_paths: &[PathBuf],
        taxonomy_file_path: &Option<PathBuf>,
        num_threads: usize,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        initial_configuration_opt: Option<Configuration>,
        log_folder: &Path,
        include_domains: bool,
    ) -> Result<()> {
        info!("Starting database build");

        let mut client = Client::new(&self.database_url).await?;

        debug!("applying database migrations...");

        // Run migrations
        run_migrations(&client).await?;

        // get or set configuration
        let configuration = Self::get_or_set_configuration(
            &mut client,
            protein_file_paths,
            num_partitions,
            allowed_ram_usage,
            partitioner_false_positive_probability,
            initial_configuration_opt,
        )
        .await?;
        let protease: Arc<dyn Protease> = get_protease_by_name(
            configuration.get_protease_name(),
            configuration.get_min_peptide_length(),
            configuration.get_max_peptide_length(),
            configuration.get_max_number_of_missed_cleavages(),
        )?
        .into();

        if let Some(taxonomy_file_path) = taxonomy_file_path {
            info!("Taxonomy tree build...");
            Self::build_taxonomy_tree(&client, taxonomy_file_path).await?;
        }

        if !protein_file_paths.is_empty() {
            // read, digest and insert proteins and peptides
            info!("Protein digestion ...");

            let mut attempt_protein_file_path = protein_file_paths.to_owned();
            // Insert proteins/peptides until no error occurred
            for attempt in 1.. {
                info!("Proteins digestion attempt {}", attempt);
                // set variable for this digestion attempt
                let attempt_log_folder = log_folder.join(attempt.to_string());
                if !attempt_log_folder.is_dir() {
                    create_dir_all(&attempt_log_folder).await?;
                }
                // Reduce threads for each attempt until 1 thread is reached
                let attempt_num_threads = max(num_threads / attempt, 1);

                // Start protein digestion
                let num_unprocessable_proteins = Self::protein_digestion(
                    &self.database_url,
                    attempt_num_threads,
                    &attempt_protein_file_path,
                    protease.clone(),
                    configuration.get_remove_peptides_containing_unknown(),
                    configuration.get_partition_limits().to_vec(),
                    &attempt_log_folder,
                )
                .await?;

                // If no errors occurred, break
                if num_unprocessable_proteins == 0 {
                    info!("Digestion finished");
                    break;
                } else {
                    attempt_protein_file_path =
                        vec![attempt_log_folder.join(UNPROCESSABLE_PROTEINS_LOG_FILE_NAME)];
                    info!(
                        "Digestion failed for {} proteins. Retrying with less threads.",
                        num_unprocessable_proteins
                    );
                }
            }
        }

        info!("Metadata update ...");
        Self::collect_peptide_metadata(
            num_threads,
            &self.database_url,
            &configuration,
            protease.clone(),
            include_domains,
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // std imports
    use std::env;
    use std::fs::{create_dir_all, remove_dir_all};
    use std::path::Path;

    // 3rd party imports
    use serial_test::serial;

    // internal imports
    use super::*;
    use crate::database::scylla::drop_keyspace;
    use crate::database::scylla::tests::get_test_database_url;
    use crate::database::scylla::{peptide_table::PeptideTable, protein_table::ProteinTable};

    use crate::io::uniprot_text::reader::Reader;
    use crate::tools::tests::get_taxdmp_zip;

    lazy_static! {
        static ref CONFIGURATION: Configuration = Configuration::new(
            "trypsin".to_owned(),
            Some(2),
            Some(5),
            Some(60),
            true,
            Vec::with_capacity(0)
        );
    }

    const EXPECTED_ASSOCIATED_PROTEINS_FOR_DUPLICATED_TRYPSIN: [&str; 2] = ["P07477", "DUPLIC"];
    const EXPECTED_ASSOCIATED_TAXONOMY_IDS_FOR_DUPLICATED_TRYPSIN: [i64; 2] = [9922, 9606];
    const EXPECTED_PROTEOME_IDS_FOR_DUPLICATED_TRYPSIN: [&str; 2] = ["UP000005640", "UP000291000"];

    // Test the database building
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_database_build_without_initial_config() {
        let taxdmp_zip_path = Some(get_taxdmp_zip().await.unwrap());
        let client = Client::new(&get_test_database_url()).await.unwrap();

        drop_keyspace(&client).await;

        let protein_file_paths = vec![Path::new("test_files/uniprot.txt").to_path_buf()];
        let log_folder = env::temp_dir().join("macpepdb_rs/database_build");
        if log_folder.exists() {
            remove_dir_all(&log_folder).unwrap();
        }
        create_dir_all(&log_folder).unwrap();

        let database_builder = DatabaseBuild::new(&get_test_database_url());
        let build_res = database_builder
            .build(
                &protein_file_paths,
                &taxdmp_zip_path,
                2,
                100,
                0.5,
                0.0002,
                None,
                &log_folder,
                true,
            )
            .await;
        assert!(build_res.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_database_build() {
        let taxdmp_zip_path = Some(get_taxdmp_zip().await.unwrap());
        let client = Client::new(&get_test_database_url()).await.unwrap();

        drop_keyspace(&client).await;

        let protein_file_paths = vec![
            Path::new("test_files/uniprot.txt").to_path_buf(),
            Path::new("test_files/trypsin_duplicate.txt").to_path_buf(),
        ];

        let log_folder = env::temp_dir().join("macpepdb_rs/database_build");
        if log_folder.exists() {
            remove_dir_all(&log_folder).unwrap();
        }
        create_dir_all(&log_folder).unwrap();

        let database_builder = DatabaseBuild::new(&get_test_database_url());
        database_builder
            .build(
                &protein_file_paths,
                &taxdmp_zip_path,
                2,
                100,
                0.5,
                0.0002,
                Some(CONFIGURATION.clone()),
                &log_folder,
                true,
            )
            .await
            .unwrap();

        let configuration = ConfigurationTable::select(&client).await.unwrap();

        let protease = get_protease_by_name(
            configuration.get_protease_name(),
            configuration.get_min_peptide_length(),
            configuration.get_max_peptide_length(),
            configuration.get_max_number_of_missed_cleavages(),
        )
        .unwrap();

        // Check if every peptide is in the database
        for protein_file_path in protein_file_paths {
            let mut reader = Reader::new(&protein_file_path, 4096).unwrap();
            while let Some(protein) = reader.next().unwrap() {
                let proteins = ProteinTable::select(
                    &client,
                    "WHERE accession = ?",
                    &[&CqlValue::Text(protein.get_accession().to_owned())],
                )
                .await
                .unwrap()
                .try_collect::<Vec<Protein>>()
                .await
                .unwrap();
                assert_eq!(proteins.len(), 1);

                let expected_peptides: Vec<Peptide> = convert_to_internal_peptide(
                    Box::new(protease.cleave(protein.get_sequence()).unwrap()),
                    configuration.get_partition_limits(),
                    &protein,
                )
                .collect()
                .unwrap();

                for peptide in expected_peptides {
                    let peptides = PeptideTable::select(
                        &client,
                        "WHERE partition = ? AND mass = ? AND sequence = ? LIMIT 1",
                        (
                            peptide.get_partition(),
                            peptide.get_mass(),
                            peptide.get_sequence(),
                        ),
                    )
                    .await
                    .unwrap()
                    .try_collect::<Vec<Peptide>>()
                    .await
                    .unwrap();

                    assert_eq!(peptides.len(), 1);

                    // TODO: See if domains are there
                }
            }
        }

        // Select the duplicated trpsin protein
        // Digest it again, and check the metadata fit to the original trypsin and the duplicated trypsin
        let stream = ProteinTable::select(
            &client,
            "WHERE accession = ?",
            &[&CqlValue::Text("DUPLIC".to_string())],
        )
        .await
        .unwrap();

        pin!(stream);

        let trypsin_duplicate = stream.try_next().await.unwrap().unwrap();

        let trypsin_duplicate_peptides: Vec<Peptide> = convert_to_internal_peptide(
            Box::new(protease.cleave(trypsin_duplicate.get_sequence()).unwrap()),
            configuration.get_partition_limits(),
            &trypsin_duplicate,
        )
        .collect()
        .unwrap();

        for peptide in trypsin_duplicate_peptides {
            let peptide = PeptideTable::select(
                &client,
                "WHERE partition = ? AND mass = ? AND sequence = ? LIMIT 1",
                (
                    peptide.get_partition(),
                    peptide.get_mass_as_ref(),
                    peptide.get_sequence(),
                ),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Peptide>>()
            .await
            .unwrap()
            .pop()
            .unwrap();
            assert_eq!(peptide.get_proteins().len(), 2);

            EXPECTED_ASSOCIATED_PROTEINS_FOR_DUPLICATED_TRYPSIN
                .iter()
                .for_each(|x| {
                    assert!(
                        peptide.get_proteins().contains(&x.to_string()),
                        "{} not found in proteins",
                        x
                    )
                });
            EXPECTED_ASSOCIATED_TAXONOMY_IDS_FOR_DUPLICATED_TRYPSIN
                .iter()
                .for_each(|x| {
                    assert!(
                        peptide.get_taxonomy_ids().contains(x),
                        "{} not found in taxonomy_ids",
                        x
                    )
                });
            EXPECTED_ASSOCIATED_TAXONOMY_IDS_FOR_DUPLICATED_TRYPSIN
                .iter()
                .for_each(|x| {
                    assert!(
                        peptide.get_unique_taxonomy_ids().contains(x),
                        "{} not found in unique_taxonomy_ids",
                        x
                    )
                });
            EXPECTED_PROTEOME_IDS_FOR_DUPLICATED_TRYPSIN
                .iter()
                .for_each(|x| {
                    assert!(
                        peptide.get_proteome_ids().contains(&x.to_string()),
                        "{} not found in proteome_ides",
                        x
                    )
                });

            assert!(peptide.get_is_swiss_prot());
            assert!(peptide.get_is_trembl());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_peptide_metadata_update() {
        let client = Client::new("scylla://192.168.124.58,192.168.124.61,192.168.124.92,192.168.124.106,192.168.124.147,192.168.124.136,192.168.124.34,192.168.124.194,192.168.124.180,192.168.124.245/uniprottry?read_consistency_level=one").await.unwrap();
        let config = ConfigurationTable::select(&client).await.unwrap();

        let target = "DAQAGK";

        let peptide =
            PeptideTable::select_by_sequence(&client, target, config.get_partition_limits())
                .await
                .unwrap()
                .unwrap();

        println!("{} ", peptide.get_partition());

        println!("{}", config.get_partition_limits().len());

        // let associated_proteins = ProteinTable::get_proteins_of_peptide(&client, &peptide)
        //     .await
        //     .unwrap()
        //     .try_collect::<Vec<_>>()
        //     .await
        //     .unwrap();

        // for protein in &associated_proteins {
        //     println!("{}", protein.get_accession(),);
        // }

        // let (
        //     is_swiss_prot,
        //     is_trembl,
        //     mut taxonomy_ids,
        //     mut unique_taxonomy_ids,
        //     proteome_ids,
        //     domains,
        // ) = peptide.get_metadata_from_proteins(&associated_proteins, &['K', 'R'], &['P'], false);

        // taxonomy_ids.sort();
        // unique_taxonomy_ids.sort();

        // println!("is_swiss_prot: {}", is_swiss_prot);
        // println!("is_trembl: {}", is_trembl);
        // println!("taxonomy_ids: {:?}", taxonomy_ids);
        // println!("unique_taxonomy_ids: {:?}", unique_taxonomy_ids);
        // println!("proteome_ids: {:?}", proteome_ids);

        // let update_result = PeptideTable::update_metadata(
        //     &client,
        //     &peptide,
        //     is_swiss_prot,
        //     is_trembl,
        //     &taxonomy_ids,
        //     &unique_taxonomy_ids,
        //     &proteome_ids,
        //     &domains,
        // )
        // .await;

        // println!("{:?}", update_result);

        // assert!(update_result.is_ok());

        // // match update_result {
        // //     Ok(_) => {
        // //         counter!(METADATA_PROCESSED_PEPTIDES_COUNTER_NAME).increment(1);
        // //     }
        // //     Err(err) => {
        // //         counter!(METADATA_ERRORS_COUNTER_NAME).increment(1);
        // //         error!(
        // //             "Metadata update failed `{}`\n{:?}\n",
        // //             peptide.get_sequence(),
        // //             err
        // //         );
        // //     }
        // // };
    }

    // TODO: Test update
}
