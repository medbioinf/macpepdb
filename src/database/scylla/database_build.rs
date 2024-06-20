// std imports
use std::cmp::max;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
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
use scylla::prepared_statement::PreparedStatement;
use tokio::fs::create_dir_all;
use tokio::spawn;
use tokio::sync::mpsc::{channel, Sender};
use tracing::{debug, error, info, warn};

// internal imports
use crate::database::configuration_table::{
    ConfigurationIncompleteError, ConfigurationTable as ConfigurationTableTrait,
};
use crate::database::database_build::DatabaseBuild as DatabaseBuildTrait;
use crate::database::generic_client::GenericClient;
use crate::database::scylla::client::Client;
use crate::database::scylla::migrations::run_migrations;
use crate::database::scylla::{
    configuration_table::ConfigurationTable, peptide_table::PeptideTable,
    protein_table::ProteinTable,
};
use crate::database::selectable_table::SelectableTable;
use crate::database::table::Table;
use crate::tools::message_logger::MessageLogger;
use crate::tools::metrics_logger::MetricsLogger;
use crate::tools::omicstools::{convert_to_internal_peptide, remove_unknown_from_digest};
use crate::tools::peptide_mass_counter::PeptideMassCounter;
use crate::tools::progress_monitor::ProgressMonitor;
use crate::tools::protein_counter::ProteinCounter;
use crate::tools::queue_monitor::QueueMonitor;
use scylla::frame::response::result::CqlValue;

use crate::entities::{configuration::Configuration, peptide::Peptide, protein::Protein};
use crate::io::uniprot_text::reader::Reader;
use crate::tools::peptide_partitioner::PeptidePartitioner;

use super::peptide_table::{TABLE_NAME, UPDATE_SET_PLACEHOLDER};
use super::taxonomy_tree_table::TaxonomyTreeTable;

lazy_static! {
    static ref PROTEIN_QUEUE_WRITE_SLEEP_TIME: Duration = Duration::from_millis(100);
    static ref PROTEIN_QUEUE_READ_SLEEP_TIME: Duration = Duration::from_secs(2);
}

const MAX_INSERT_TRIES: u64 = 5;

/// Name for unprocessable proteins log file
///
const UNPROCESSABLE_PROTEINS_LOG_FILE_NAME: &str = "unprocessable_proteins.txt";

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
        protein_file_paths: &Vec<PathBuf>,
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

        let new_configuration = if initial_configuration.get_partition_limits().len() == 0 {
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
        protein_file_paths: &Vec<PathBuf>,
        protease: &dyn Protease,
        remove_peptides_containing_unknown: bool,
        partition_limits: Vec<i64>,
        log_folder: &PathBuf,
        metrics_log_intervals: u64,
    ) -> Result<usize> {
        debug!("Digesting proteins and inserting peptides");

        let protein_queue_size = num_threads * 300;

        // Count proteins
        info!("Counting proteins in files");
        let proteins_to_process = ProteinCounter::count(protein_file_paths, num_threads).await?;

        // Database client
        let client = Arc::new(Client::new(database_url).await?);

        // Digestion variables
        let protein_queue_arc: Arc<ArrayQueue<Protein>> =
            Arc::new(ArrayQueue::new(protein_queue_size));
        let partition_limits_arc: Arc<Vec<i64>> = Arc::new(partition_limits);
        let stop_flag = Arc::new(AtomicBool::new(false));

        // Logging variable
        let processed_proteins = Arc::new(AtomicUsize::new(0));
        let processed_peptides = Arc::new(AtomicUsize::new(0));
        let occurred_errors = Arc::new(AtomicUsize::new(0));
        let metric_names = vec![
            "proteins".to_owned(),
            "peptides".to_owned(),
            "errors".to_owned(),
        ];

        let log_stop_flag = Arc::new(AtomicBool::new(false));
        let metrics_log_file_path = log_folder.join("digestion.metrics.tsv");
        let unprocessable_proteins_log_file_path =
            log_folder.join(UNPROCESSABLE_PROTEINS_LOG_FILE_NAME);
        let error_log_file_path = log_folder.join("digestion.errors.log");

        // Thread communication
        let (unprocessable_proteins_sender, unprocessable_proteins_receiver) =
            channel::<Protein>(1000);
        let (error_sender, error_receiver) = channel::<String>(1000);

        // Error logger
        let mut error_logger =
            MessageLogger::new(error_log_file_path.clone(), error_receiver, 10).await;

        // Unprocessable proteins logger
        let mut unprocessable_proteins_logger = MessageLogger::new(
            unprocessable_proteins_log_file_path.clone(),
            unprocessable_proteins_receiver,
            1, // Important to save each and every protein which fails
        )
        .await;

        let mut progress_monitor = ProgressMonitor::new(
            "",
            vec![
                processed_proteins.clone(),
                processed_peptides.clone(),
                occurred_errors.clone(),
            ],
            vec![Some(proteins_to_process as u64), None, None],
            vec![
                "proteins".to_string(),
                "peptides".to_string(),
                "errors".to_string(),
            ],
            None,
        )?;

        // Metrics logger
        let mut metrics_logger = MetricsLogger::new(
            vec![
                processed_proteins.clone(),
                processed_peptides.clone(),
                occurred_errors.clone(),
            ],
            metric_names.clone(),
            metrics_log_file_path,
            metrics_log_intervals,
        )?;

        let mut queue_monitor = QueueMonitor::new(
            "",
            vec![protein_queue_arc.clone()],
            vec![protein_queue_size as u64],
            vec!["protein queue".to_string()],
            None,
        )?;

        let digestion_thread_handles = (0..num_threads)
            .map(|_| {
                // Create a boxed protease
                let protease_box = get_protease_by_name(
                    protease.get_name(),
                    protease.get_min_length(),
                    protease.get_max_length(),
                    protease.get_max_missed_cleavages(),
                )?;
                // Start digestion thread
                Ok(spawn(Self::digestion_thread(
                    client.clone(),
                    protein_queue_arc.clone(),
                    partition_limits_arc.clone(),
                    stop_flag.clone(),
                    protease_box,
                    remove_peptides_containing_unknown,
                    unprocessable_proteins_sender.clone(),
                    error_sender.clone(),
                    processed_proteins.clone(),
                    processed_peptides.clone(),
                    occurred_errors.clone(),
                )))
            })
            .collect::<Result<Vec<_>>>()?;

        // Drop the original sender its not needed anymore
        drop(unprocessable_proteins_sender);
        drop(error_sender);

        for protein_file_path in protein_file_paths {
            let mut reader = Reader::new(protein_file_path, 4096)?;
            while let Some(protein) = reader.next()? {
                let mut next_protein = protein;
                loop {
                    match protein_queue_arc.push(next_protein) {
                        Ok(_) => break,
                        Err(protein) => {
                            next_protein = protein;
                        }
                    }
                }
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
        let _ = error_logger.stop().await?;
        progress_monitor.stop().await?;
        metrics_logger.stop().await?;
        queue_monitor.stop().await?;

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
    /// * `error_sender` - The sender which is used to send general errors to the logger
    /// * `metrics` - The metrics
    ///
    async fn digestion_thread(
        client: Arc<Client>,
        protein_queue_arc: Arc<ArrayQueue<Protein>>,
        partition_limits_arc: Arc<Vec<i64>>,
        stop_flag: Arc<AtomicBool>,
        protease: Box<dyn Protease>,
        remove_peptides_containing_unknown: bool,
        unprocessable_proteins_sender: Sender<Protein>,
        error_sender: Sender<String>,
        processed_proteins: Arc<AtomicUsize>,
        processed_peptides: Arc<AtomicUsize>,
        occurred_errors: Arc<AtomicUsize>,
    ) -> Result<()> {
        let statement = format!(
            "UPDATE {}.{} SET {}, is_metadata_updated = false WHERE partition = ? and mass = ? and sequence = ?",
            client.get_database(),
            TABLE_NAME,
            UPDATE_SET_PLACEHOLDER.as_str()
        );
        let prepared = client.prepare(statement).await?;

        loop {
            let protein = protein_queue_arc.pop();
            if protein.is_none() {
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                continue;
            }

            let protein = protein.unwrap();

            let mut accession_list = protein.get_secondary_accessions().clone();
            accession_list.push(protein.get_accession().to_owned());

            let existing_protein = ProteinTable::select(
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
                            &existing_protein,
                            &protease,
                            remove_peptides_containing_unknown,
                            &partition_limits_arc,
                            &prepared,
                            processed_peptides.as_ref(),
                        )
                        .await;
                    } else {
                        return Self::insert_protein(
                            client.as_ref(),
                            &protein,
                            &protease,
                            remove_peptides_containing_unknown,
                            &partition_limits_arc,
                            &prepared,
                            processed_peptides.as_ref(),
                        )
                        .await;
                    };
                }
                .await;

                match upsert_result {
                    Ok(_) => {
                        processed_proteins.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Err(err) => {
                        occurred_errors.fetch_add(1, Ordering::Relaxed);
                        let error_msg = format!(
                            "Upsert failed  for `{}` ({})",
                            protein.get_accession(),
                            tries
                        );
                        if tries < MAX_INSERT_TRIES {
                            warn!("{}", error_msg);
                        } else {
                            error!("{}", error_msg);
                        }
                        error_sender
                            .send(format!("{}\n{:?}\n", error_msg, err))
                            .await?;
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
    /// * `peptide_sender` - The sender which is used to send the number of processed peptides to the logger
    ///
    async fn update_protein(
        client: &Client,
        updated_protein: &Protein,
        stored_protein: &Protein,
        protease: &Box<dyn Protease>,
        remove_peptides_containing_unknown: bool,
        partition_limits: &Vec<i64>,
        prepared: &PreparedStatement,
        processed_peptides: &AtomicUsize,
    ) -> Result<()> {
        let peptides_of_stored_protein = convert_to_internal_peptide(
            match remove_peptides_containing_unknown {
                true => Box::new(remove_unknown_from_digest(
                    protease.cleave(&stored_protein.get_sequence())?,
                )),
                false => Box::new(protease.cleave(&stored_protein.get_sequence())?),
            },
            partition_limits,
            stored_protein,
        )
        .collect::<HashSet<Peptide>>()?;

        let peptides_of_updated_protein = convert_to_internal_peptide(
            match remove_peptides_containing_unknown {
                true => Box::new(remove_unknown_from_digest(
                    protease.cleave(&updated_protein.get_sequence())?,
                )),
                false => Box::new(protease.cleave(&updated_protein.get_sequence())?),
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
                prepared,
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
                prepared,
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
        ProteinTable::update(client, &stored_protein, &updated_protein).await?;
        processed_peptides.fetch_add(peptides_of_updated_protein.len(), Ordering::Relaxed);

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
            .difference(&peptides_from_updated_protein)
            .collect::<Vec<&Peptide>>();

        if peptides_to_deassociate.len() > 0 {
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
        prepared: &PreparedStatement,
    ) -> Result<()> {
        // Disassociate all peptides from existing protein which are not contained by the new protein
        let peptides_to_create: Vec<&Peptide> = peptides_from_updated_protein
            .difference(peptides_from_stored_protein)
            .collect::<Vec<&Peptide>>();

        if peptides_to_create.len() > 0 {
            PeptideTable::bulk_insert(client, peptides_to_create.into_iter(), prepared).await?
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
    /// * `peptide_sender` - The sender which is used to send the number of processed peptides to the logger
    ///
    async fn insert_protein(
        client: &Client,
        protein: &Protein,
        protease: &Box<dyn Protease>,
        remove_peptides_containing_unknown: bool,
        partition_limits: &Vec<i64>,
        prepared: &PreparedStatement,
        processed_peptides: &AtomicUsize,
    ) -> Result<()> {
        // Digest protein
        let peptides = convert_to_internal_peptide(
            match remove_peptides_containing_unknown {
                true => Box::new(remove_unknown_from_digest(
                    protease.cleave(&protein.get_sequence())?,
                )),
                false => Box::new(protease.cleave(&protein.get_sequence())?),
            },
            partition_limits,
            protein,
        )
        .collect::<HashSet<Peptide>>()?;

        ProteinTable::insert(client, &protein).await?;
        // PeptideTable::bulk_insert(client, &mut peptides.iter(), prepared).await?;
        PeptideTable::bulk_insert(client, &mut peptides.iter(), prepared)
            .await
            .unwrap();
        processed_peptides.fetch_add(peptides.len(), Ordering::Relaxed);

        return Ok(());
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
    /// * `metrics_log_intervals` - The intervals in which the metrics are logged
    ///
    async fn collect_peptide_metadata(
        num_threads: usize,
        database_url: &str,
        configuration: &Configuration,
        protease: &dyn Protease,
        include_domains: bool,
        log_folder: &PathBuf,
        metrics_log_intervals: u64,
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
        let processed_peptides = Arc::new(AtomicUsize::new(0));
        let occurred_errors = Arc::new(AtomicUsize::new(0));
        let metric_names = vec!["peptides".to_owned(), "errors".to_owned()];
        let metrics_log_file_path = log_folder.join("metadata_update.metrics.tsv");
        let log_file_path = log_folder.join("metadata_update.error.log");
        let (error_sender, error_receiver) = channel::<String>(1000);

        // Metadata update variables
        let partition_queue: Vec<i64> =
            (0..(configuration.get_partition_limits().len() as i64)).collect();
        let partition_queue = Arc::new(Mutex::new(partition_queue));

        // Error log
        let mut error_logger =
            MessageLogger::new(log_file_path.to_path_buf(), error_receiver, 10).await;

        let mut progress_monitor = ProgressMonitor::new(
            "",
            vec![processed_peptides.clone(), occurred_errors.clone()],
            vec![None, None],
            metric_names.clone(),
            None,
        )?;

        // Metrics logger
        let mut metrics_logger = MetricsLogger::new(
            vec![processed_peptides.clone(), occurred_errors.clone()],
            metric_names.clone(),
            metrics_log_file_path,
            metrics_log_intervals,
        )?;

        debug!("Starting {} metadata update threads", num_threads);
        let client = Arc::new(Client::new(database_url).await?);
        let metadata_collector_thread_handles: Vec<_> = (0..num_threads * 2)
            .map(|_| {
                let protease = get_protease_by_name(
                    protease.get_name(),
                    protease.get_min_length(),
                    protease.get_max_length(),
                    protease.get_max_missed_cleavages(),
                )?;
                // Start digestion thread
                Ok(spawn(Self::collect_peptide_metadata_thread(
                    client.clone(),
                    partition_queue.clone(),
                    protease,
                    include_domains,
                    processed_peptides.clone(),
                    occurred_errors.clone(),
                    error_sender.clone(),
                )))
            })
            .collect::<Result<Vec<_>>>()?;

        // Drop the original sender its not needed anymore and would block the error logging thread
        drop(error_sender);

        debug!("Waiting metadata update threads to stop ...");
        // Wait for digestion threads to finish
        join_all(metadata_collector_thread_handles).await;
        debug!("... all metadata update threads stopped");

        debug!("Waiting for logging threads to stop ...");
        let _ = error_logger.stop().await?;
        progress_monitor.stop().await?;
        metrics_logger.stop().await?;
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
    /// * `metrics` - The metrics
    /// * `error_sender` - The sender which is used to send general errors to the logger
    ///
    async fn collect_peptide_metadata_thread(
        client: Arc<Client>,
        partition_queue: Arc<Mutex<Vec<i64>>>,
        protease: Box<dyn Protease>,
        include_domains: bool,
        processed_peptides: Arc<AtomicUsize>,
        occurred_errors: Arc<AtomicUsize>,
        error_sender: Sender<String>,
    ) -> Result<()> {
        let update_query = format!(
            "UPDATE {}.{} SET is_metadata_updated = true, is_swiss_prot = ?, is_trembl = ?, taxonomy_ids = ?, unique_taxonomy_ids = ?, proteome_ids = ?, domains = ? WHERE partition = ? AND mass = ? and sequence = ?",
            client.get_database(),
            PeptideTable::table_name()
        );
        let update_query_prepared_statement = client.prepare(update_query).await?;

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

        loop {
            let partition = {
                let mut partition_queue = match partition_queue.lock() {
                    Ok(partition_queue) => partition_queue,
                    Err(err) => bail!(format!("Could not lock partition queue: {}", err)),
                };
                match partition_queue.pop() {
                    Some(partition) => partition,
                    None => break,
                }
            };

            let partition_cql = CqlValue::BigInt(partition);
            let select_args_refs = vec![&partition_cql];

            let peptide_stream = PeptideTable::stream(
                client.as_ref(),
                "WHERE partition = ? AND is_metadata_updated = false ALLOW FILTERING",
                &select_args_refs,
                10000,
            )
            .await?;

            pin_mut!(peptide_stream);

            while let Some(peptide) = peptide_stream.next().await {
                let peptide = peptide?;
                let associated_proteins =
                    ProteinTable::get_proteins_of_peptide(client.as_ref(), &peptide)
                        .await?
                        .try_collect()
                        .await?;

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

                let update_result = client
                    .execute(
                        &update_query_prepared_statement,
                        (
                            &is_swiss_prot,
                            &is_trembl,
                            &taxonomy_ids,
                            &unique_taxonomy_ids,
                            &proteome_ids,
                            &domains,
                            peptide.get_partition(),
                            peptide.get_mass(),
                            peptide.get_sequence(),
                        ),
                    )
                    .await;

                match update_result {
                    Ok(_) => {
                        processed_peptides.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) => {
                        occurred_errors.fetch_add(1, Ordering::Relaxed);
                        error!("Metadata update failed for {}", peptide.get_sequence());
                        error_sender
                            .send(format!(
                                "Metadata update failed `{}`\n{:?}\n",
                                peptide.get_sequence(),
                                err
                            ))
                            .await?;
                    }
                };
            }
        }
        Ok(())
    }

    async fn build_taxonomy_tree(client: &Client, taxonomy_file_path: &Path) -> Result<()> {
        debug!("Build taxonomy tree...");
        let taxonomy_tree = TaxonomyReader::new(taxonomy_file_path)?.read()?;
        TaxonomyTreeTable::insert(client, &taxonomy_tree).await?;
        Ok(())
    }
}

/// TODO: Trait and this struct should be merged as they only existed separately when
/// another database engine was supported.
impl DatabaseBuildTrait for DatabaseBuild {
    fn new(database_url: &str) -> Self {
        return Self {
            database_url: database_url.to_owned(),
        };
    }

    async fn build(
        &self,
        protein_file_paths: &Vec<PathBuf>,
        taxonomy_file_path: &Option<PathBuf>,
        num_threads: usize,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        initial_configuration_opt: Option<Configuration>,
        log_folder: &PathBuf,
        include_domains: bool,
        metrics_log_interval: u64,
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
        let protease = get_protease_by_name(
            configuration.get_protease_name(),
            configuration.get_min_peptide_length(),
            configuration.get_max_peptide_length(),
            configuration.get_max_number_of_missed_cleavages(),
        )?;

        if let Some(taxonomy_file_path) = taxonomy_file_path {
            info!("Taxonomy tree build...");
            Self::build_taxonomy_tree(&client, taxonomy_file_path).await?;
        }

        if !protein_file_paths.is_empty() {
            // read, digest and insert proteins and peptides
            info!("Protein digestion ...");

            let mut attempt_protein_file_path = protein_file_paths.clone();
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
                    protease.as_ref(),
                    configuration.get_remove_peptides_containing_unknown(),
                    configuration.get_partition_limits().to_vec(),
                    &attempt_log_folder,
                    metrics_log_interval,
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
            protease.as_ref(),
            include_domains,
            log_folder,
            metrics_log_interval,
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
    use tracing_test::traced_test;

    // internal imports
    use super::*;
    use crate::database::scylla::drop_keyspace;
    use crate::database::scylla::tests::DATABASE_URL;
    use crate::database::scylla::{peptide_table::PeptideTable, protein_table::ProteinTable};
    use crate::database::selectable_table::SelectableTable;
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

    const EXPECTED_ASSOCIATED_PROTEINS_FOR_DUPLICATED_TRYPSIN: [&'static str; 2] =
        ["P07477", "DUPLIC"];
    const EXPECTED_ASSOCIATED_TAXONOMY_IDS_FOR_DUPLICATED_TRYPSIN: [i64; 2] = [9922, 9606];
    const EXPECTED_PROTEOME_IDS_FOR_DUPLICATED_TRYPSIN: [&'static str; 2] =
        ["UP000005640", "UP000291000"];

    // Test the database building
    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    #[serial]
    async fn test_database_build_without_initial_config() {
        let taxdmp_zip_path = Some(get_taxdmp_zip().await.unwrap());
        let client = Client::new(DATABASE_URL).await.unwrap();

        drop_keyspace(&client).await;

        let protein_file_paths = vec![Path::new("test_files/uniprot.txt").to_path_buf()];
        let log_folder = env::temp_dir().join("macpepdb_rs/database_build");
        if log_folder.exists() {
            remove_dir_all(&log_folder).unwrap();
        }
        create_dir_all(&log_folder).unwrap();

        let database_builder = DatabaseBuild::new(DATABASE_URL);
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
                5,
            )
            .await;
        assert!(build_res.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    #[serial]
    async fn test_database_build() {
        let taxdmp_zip_path = Some(get_taxdmp_zip().await.unwrap());
        let client = Client::new(DATABASE_URL).await.unwrap();

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

        let database_builder = DatabaseBuild::new(DATABASE_URL);
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
                5,
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
                let proteins = ProteinTable::select_multiple(
                    &client,
                    "WHERE accession = ?",
                    &[&CqlValue::Text(protein.get_accession().to_owned())],
                )
                .await
                .unwrap();
                assert_eq!(proteins.len(), 1);

                let expected_peptides: Vec<Peptide> = convert_to_internal_peptide(
                    Box::new(protease.cleave(&protein.get_sequence()).unwrap()),
                    configuration.get_partition_limits(),
                    &protein,
                )
                .collect()
                .unwrap();

                for peptide in expected_peptides {
                    let peptides = PeptideTable::select_multiple(
                        &client,
                        "WHERE partition = ? AND mass = ? AND sequence = ?",
                        &[
                            &CqlValue::BigInt(peptide.get_partition().to_owned()),
                            &CqlValue::BigInt(peptide.get_mass_as_ref().to_owned()),
                            &CqlValue::Text(peptide.get_sequence().to_owned()),
                        ],
                    )
                    .await
                    .unwrap();

                    assert_eq!(peptides.len(), 1);

                    // TODO: See if domains are there
                }
            }
        }

        // Select the duplicated trpsin protein
        // Digest it again, and check the metadata fit to the original trypsin and the duplicated trypsin
        let trypsin_duplicate = ProteinTable::select(
            &client,
            "WHERE accession = ?",
            &[&CqlValue::Text("DUPLIC".to_string())],
        )
        .await
        .unwrap()
        .unwrap();

        let trypsin_duplicate_peptides: Vec<Peptide> = convert_to_internal_peptide(
            Box::new(protease.cleave(&trypsin_duplicate.get_sequence()).unwrap()),
            configuration.get_partition_limits(),
            &trypsin_duplicate,
        )
        .collect()
        .unwrap();

        for peptide in trypsin_duplicate_peptides {
            let peptide = PeptideTable::select(
                &client,
                "WHERE partition = ? AND mass = ? AND sequence = ?",
                &[
                    &CqlValue::BigInt(peptide.get_partition().to_owned()),
                    &CqlValue::BigInt(peptide.get_mass_as_ref().to_owned()),
                    &CqlValue::Text(peptide.get_sequence().to_owned()),
                ],
            )
            .await
            .unwrap()
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
                        peptide.get_taxonomy_ids().contains(&x),
                        "{} not found in taxonomy_ids",
                        x
                    )
                });
            EXPECTED_ASSOCIATED_TAXONOMY_IDS_FOR_DUPLICATED_TRYPSIN
                .iter()
                .for_each(|x| {
                    assert!(
                        peptide.get_unique_taxonomy_ids().contains(&x),
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

    // TODO: Test update
}
