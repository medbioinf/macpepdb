// std import
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::sleep;
use std::time::Duration;

// 3rd party imports
use anyhow::{bail, Result};
use fallible_iterator::FallibleIterator;
use futures::future::join_all;
use rand::{self, Rng};
use refinery::embed_migrations;
use tokio::spawn;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_postgres::{
    error::{Error as PSQLError, SqlState},
    Client, GenericClient, NoTls,
};
use tracing::{debug, debug_span, error};

// internal imports
use crate::biology::digestion_enzyme::{
    enzyme::Enzyme,
    functions::{
        create_peptides_entities_from_digest, get_enzyme_by_name, remove_unknown_from_digest,
    },
};
use crate::database::citus::{
    configuration_table::ConfigurationTable, peptide_table::PeptideTable,
    protein_table::ProteinTable,
};
use crate::database::configuration_table::{
    ConfigurationIncompleteError, ConfigurationTable as ConfigurationTableTrait,
};
use crate::database::database_build::DatabaseBuild as DatabaseBuildTrait;
use crate::database::selectable_table::SelectableTable;
use crate::database::table::Table;

use crate::entities::{configuration::Configuration, peptide::Peptide, protein::Protein};
use crate::io::uniprot_text::reader::Reader;
use crate::tools::peptide_partitioner::PeptidePartitioner;
use crate::tools::performance_logger::performance_log_receiver;
use crate::tools::{
    error_logger::error_logger, performance_logger::performance_log_thread,
    unprocessable_protein_logger::unprocessable_proteins_logger,
};

// add module migration to the current module
embed_migrations!("src/database/citus/migrations");

const MAX_INSERT_TRIES: u64 = 3;

lazy_static! {
    static ref PROTEIN_QUEUE_WRITE_SLEEP_TIME: Duration = Duration::from_millis(100);
    static ref PROTEIN_QUEUE_READ_SLEEP_TIME: Duration = Duration::from_secs(2);
}

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
    /// * `allowed_ram_usage` - The allowed ram usage in GB
    /// * `partitioner_false_positive_probability` - The false positive probability of the partitioner
    /// * `initial_configuration_opt` - The initial configuration
    ///
    async fn get_or_set_configuration(
        client: &mut Client,
        protein_file_paths: &Vec<PathBuf>,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        initial_configuration_opt: Option<Configuration>,
    ) -> Result<Configuration> {
        let config_res = ConfigurationTable::select(client).await;
        // return if configuration is ok or if it is not a ConfigurationIncompleteError
        if config_res.as_ref().is_ok()
            || !config_res
                .as_ref()
                .unwrap_err()
                .is::<ConfigurationIncompleteError>()
        {
            if config_res.as_ref().is_ok() {
                debug!("found previous config ...");
            }
            return config_res;
        }
        // throw error if no initial configuration is given
        if initial_configuration_opt.is_none() {
            bail!("No configuration given and no configuration found in database.");
        }
        // unwrap is safe because of `if` above
        let initial_configuration = initial_configuration_opt.unwrap();

        let new_configuration = if initial_configuration.get_partition_limits().len() == 0 {
            debug!("initial configuration has no partition limits list, creating one ...");
            // create digestion enzyme
            let digestion_enzyme = get_enzyme_by_name(
                initial_configuration.get_enzyme_name(),
                initial_configuration.get_max_number_of_missed_cleavages(),
                initial_configuration.get_min_peptide_length(),
                initial_configuration.get_max_peptide_length(),
            )?;
            // create partitioner
            let partitioner = PeptidePartitioner::new(
                protein_file_paths,
                digestion_enzyme.as_ref(),
                initial_configuration.get_remove_peptides_containing_unknown(),
                partitioner_false_positive_probability,
                allowed_ram_usage,
            )?;
            // create partition limits
            let partition_limits = partitioner.partition(num_partitions, None)?;
            // create new configuration with partition limits
            Configuration::new(
                initial_configuration.get_enzyme_name().to_owned(),
                initial_configuration.get_max_number_of_missed_cleavages() as i16,
                initial_configuration.get_min_peptide_length() as i16,
                initial_configuration.get_max_peptide_length() as i16,
                initial_configuration.get_remove_peptides_containing_unknown(),
                partition_limits,
            )
        } else {
            // if partition limits were given just clone the initial configuration
            initial_configuration.clone()
        };

        // insert new_configuration
        ConfigurationTable::insert(client, &new_configuration).await?;
        debug!("new configuration saved ...");
        Ok(new_configuration)
    }

    /// Digests the proteins in the given files and inserts/updates the
    /// proteins and peptides in the database.
    ///
    /// # Arguments
    /// * `database_url` - The database url
    /// * `num_threads` - The number of threads
    /// * `protein_file_paths` - The paths to the protein files
    /// * `digestion_enzyme` - The digestion enzyme
    /// * `remove_peptides_containing_unknown` - Remove peptides containing unknown amino acids
    /// * `partition_limits` - The partition limits
    ///
    async fn protein_digestion(
        database_url: &str,
        num_threads: usize,
        protein_file_paths: &Vec<PathBuf>,
        digestion_enzyme: &dyn Enzyme,
        remove_peptides_containing_unknown: bool,
        partition_limits: Vec<i64>,
        num_proteins: usize,
        log_folder: &PathBuf,
    ) -> Result<()> {
        debug!("processing proteins using {} threads ...", num_threads);

        let unprocessable_proteins_log_file_path = log_folder.join("unprocessable_proteins.txt");
        let error_log_file_path = log_folder.join("errors.log");

        let protein_queue_size = num_threads * 300;
        let protein_queue_arc: Arc<Mutex<Vec<Protein>>> = Arc::new(Mutex::new(Vec::new()));
        let partition_limits_arc = Arc::new(partition_limits);
        let stop_flag = Arc::new(AtomicBool::new(false));
        let num_proteins_processed: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
        let num_peptides_processed: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
        let (unprocessable_proteins_sender, unprocessable_proteins_receiver): (
            Sender<Protein>,
            Receiver<Protein>,
        ) = channel(1000);
        let (error_sender, error_receiver): (Sender<String>, Receiver<String>) = channel(1000);
        let (protein_sender, protein_receiver): (Sender<u64>, Receiver<u64>) = channel(1000);
        let (peptide_sender, peptide_receiver): (Sender<u64>, Receiver<u64>) = channel(1000);

        let mut digestion_thread_handles: Vec<JoinHandle<Result<()>>> = Vec::new();

        // Start digestion threads
        for thread_id in 0..num_threads {
            // Clone necessary variables
            let protein_queue_arc_clone = protein_queue_arc.clone();
            let partition_limits_arc_clone = partition_limits_arc.clone();
            let stop_flag_clone = stop_flag.clone();
            let database_url_clone = database_url.to_string();
            let thread_unprocessable_proteins_sender = unprocessable_proteins_sender.clone();
            let thread_protein_sender = protein_sender.clone();
            let thread_peptide_sender = peptide_sender.clone();
            let thread_error_sender = error_sender.clone();
            // Create a boxed enzyme
            let digestion_enzyme_box = get_enzyme_by_name(
                digestion_enzyme.get_name(),
                digestion_enzyme.get_max_number_of_missed_cleavages(),
                digestion_enzyme.get_min_peptide_length(),
                digestion_enzyme.get_max_peptide_length(),
            )?;
            // Start digestion thread
            digestion_thread_handles.push(spawn(async move {
                Self::digestion_thread(
                    thread_id,
                    database_url_clone,
                    protein_queue_arc_clone,
                    partition_limits_arc_clone,
                    stop_flag_clone,
                    digestion_enzyme_box,
                    remove_peptides_containing_unknown,
                    thread_protein_sender,
                    thread_peptide_sender,
                    thread_unprocessable_proteins_sender,
                    thread_error_sender,
                )
                .await?;
                Ok(())
            }));
        }

        // Drop the original senders.
        // Not required for cloning anymore
        drop(unprocessable_proteins_sender);
        drop(error_sender);
        drop(protein_sender);
        drop(peptide_sender);

        let num_proteins_processed_clone = Arc::clone(&num_proteins_processed);
        let num_peptides_processed_clone = Arc::clone(&num_peptides_processed);
        let performance_stop_flag = Arc::new(AtomicBool::new(false));
        let protein_queue_arc_clone = protein_queue_arc.clone();
        let stop_flag_clone = performance_stop_flag.clone();
        let performance_log_thread_handle: JoinHandle<Result<()>> = spawn(async move {
            performance_log_thread(
                &num_proteins,
                num_proteins_processed_clone,
                num_peptides_processed_clone,
                protein_queue_arc_clone,
                stop_flag_clone,
            )
            .await?;
            Ok(())
        });

        let num_proteins_processed = Arc::clone(&num_proteins_processed);
        let num_peptides_processed = Arc::clone(&num_peptides_processed);
        let performance_log_receiver_thread_handle: JoinHandle<Result<()>> = spawn(async move {
            performance_log_receiver(
                protein_receiver,
                peptide_receiver,
                num_proteins_processed,
                num_peptides_processed,
            )
            .await?;
            Ok(())
        });

        let unprocessable_proteins_log_thread_handle: JoinHandle<Result<()>> = spawn(async move {
            unprocessable_proteins_logger(
                unprocessable_proteins_log_file_path,
                unprocessable_proteins_receiver,
            )
            .await?;
            Ok(())
        });

        let error_log_thread_handle: JoinHandle<Result<()>> = spawn(async move {
            error_logger(error_log_file_path, error_receiver).await?;
            Ok(())
        });

        for protein_file_path in protein_file_paths {
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

        // Set stop flag
        stop_flag.store(true, Ordering::Relaxed);

        debug!("last proteins queued, waiting for digestion threads to finish ...");

        // Wait for digestion threads to finish
        join_all(digestion_thread_handles).await;
        performance_stop_flag.store(true, Ordering::Relaxed);
        performance_log_thread_handle.await??;
        performance_log_receiver_thread_handle.await??;
        unprocessable_proteins_log_thread_handle.await??;
        error_log_thread_handle.await??;

        Ok(())
    }

    /// Function to which digests protein, provided by a queue
    /// and inserts it along with the peptides into the database.
    ///
    /// # Arguments
    /// * `database_url` - The url of the database
    /// * `protein_queue_arc` - The queue from which the proteins are taken
    /// * `partition_limits_arc` - The partition limits
    /// * `stop_flag` - The flag which indicates if the digestion should stop
    /// * `digestion_enzyme` - The enzyme which is used for digestion
    /// * `remove_peptides_containing_unknown` - If true, peptides containing unknown amino acids are removed
    ///
    async fn digestion_thread(
        thread_id: usize,
        database_url: String,
        protein_queue_arc: Arc<Mutex<Vec<Protein>>>,
        partition_limits_arc: Arc<Vec<i64>>,
        stop_flag: Arc<AtomicBool>,
        digestion_enzyme: Box<dyn Enzyme>,
        remove_peptides_containing_unknown: bool,
        protein_sender: Sender<u64>,
        peptide_sender: Sender<u64>,
        unprocessable_proteins_sender: Sender<Protein>,
        error_sender: Sender<String>,
    ) -> Result<()> {
        let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        let performance_span = debug_span!("performance", thread_id);
        let performance_span_enter = performance_span.enter();

        let mut wait_for_queue = true;
        loop {
            if wait_for_queue {
                // Wait before trying to get next protein from queue
                debug!("Sleeping");
                sleep(*PROTEIN_QUEUE_READ_SLEEP_TIME);
                wait_for_queue = false;
            }
            // Get next protein from queue
            // if queue is empty and stop_flag is set, break
            let protein = {
                let mut protein_queue = match protein_queue_arc.lock() {
                    Ok(protein_queue) => protein_queue,
                    Err(err) => bail!(format!("Could not lock protein queue: {}", err)),
                };
                if protein_queue.is_empty() {
                    if stop_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    wait_for_queue = true;
                    continue;
                }
                let protein = protein_queue.pop().unwrap(); // unwrap is safe because we checked if queue is empty
                protein
            };
            let existing_protein: Option<Protein> = ProteinTable::select(
                &mut client,
                "WHERE accession = $1 OR accession = ANY($2)",
                &[protein.get_accession(), protein.get_secondary_accessions()],
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
                let db_result: Result<()> = async {
                    if let Some(existing_protein) = &existing_protein {
                        if existing_protein.get_updated_at() == protein.get_updated_at() {
                            return Ok(()); // Protein already exists and is up to date
                        }
                        return Self::update_protein(
                            &mut client,
                            &protein,
                            &existing_protein,
                            &digestion_enzyme,
                            remove_peptides_containing_unknown,
                            &partition_limits_arc,
                            &peptide_sender,
                        )
                        .await;
                    } else {
                        return Self::insert_protein(
                            &mut client,
                            &protein,
                            &digestion_enzyme,
                            remove_peptides_containing_unknown,
                            &partition_limits_arc,
                            &peptide_sender,
                        )
                        .await;
                    };
                }
                .await;

                if db_result.is_ok() {
                    protein_sender.send(1).await?;
                    break;
                }

                // We can expect deadlocks and unique violations to happen
                // when multiple threads try to insert the same peptides at the same time
                // or a peptide is already inserted by another thread.
                // Unique violations should be handled using the upsert mechanism, but lets catch it anyway.
                // If we get some of the errors, we're waiting for a random time and retry.
                if let Err(db_err) = db_result {
                    if let Some(db_err) = db_err.downcast_ref::<PSQLError>() {
                        match db_err.code() {
                            Some(&SqlState::T_R_DEADLOCK_DETECTED)
                            | Some(&SqlState::UNIQUE_VIOLATION) => {
                                let mut rng = rand::thread_rng();
                                let mut sleep_time = rng.gen_range(0..=3);
                                sleep_time += tries;
                                sleep(Duration::from_secs(sleep_time));
                                // Deadlock detected, retry
                                continue;
                            }
                            _ => {
                                error!("Unresolvable error logged: {:?}", db_err);
                                error_sender.send(format!("{:?}", db_err)).await?;
                            }
                        }
                    }
                    return Err(db_err);
                }
            }
        }

        std::mem::drop(performance_span_enter);
        std::mem::drop(performance_span);

        connection_handle.abort();
        let _ = connection_handle.await;
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
    /// * `digestion_enzyme` - The enzyme which is used for digestion
    /// * `remove_peptides_containing_unknown` - If true, peptides containing unknown amino acids are removed
    /// * `partition_limits` - The partition limits
    ///
    async fn update_protein(
        client: &mut Client,
        updated_protein: &Protein,
        stored_protein: &Protein,
        digestion_enzyme: &Box<dyn Enzyme>,
        remove_peptides_containing_unknown: bool,
        partition_limits: &Vec<i64>,
        peptide_sender: &Sender<u64>,
    ) -> Result<()> {
        let mut peptides_digest_of_stored_protein =
            digestion_enzyme.digest(&stored_protein.get_sequence());
        let mut peptide_digest_of_updated_protein =
            digestion_enzyme.digest(&updated_protein.get_sequence());
        if remove_peptides_containing_unknown {
            remove_unknown_from_digest(&mut peptides_digest_of_stored_protein);
            remove_unknown_from_digest(&mut peptide_digest_of_updated_protein);
        }
        let peptides_of_stored_protein: HashSet<Peptide> = create_peptides_entities_from_digest(
            &peptides_digest_of_stored_protein,
            partition_limits,
            Some(&stored_protein),
        )?;
        let peptides_of_updated_protein: HashSet<Peptide> = create_peptides_entities_from_digest(
            &peptide_digest_of_updated_protein,
            partition_limits,
            Some(&updated_protein),
        )?;
        let mut transaction = client.transaction().await?;
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
                &transaction,
                stored_protein,
                &peptides_of_stored_protein,
                &peptides_of_updated_protein,
            )
            .await?;

            // Update the old accession in the peptides to the new accession
            PeptideTable::update_protein_accession(
                &transaction,
                &mut peptides_of_updated_protein.iter(),
                stored_protein.get_accession(),
                Some(updated_protein.get_accession()),
            )
            .await?;

            // Create and associate the peptides which are not contained in the existing protein
            Self::create_protein_peptide_difference(
                &transaction,
                &peptides_of_stored_protein,
                &peptides_of_updated_protein,
            )
            .await?;
        } else if updated_protein.get_accession() != stored_protein.get_accession() {
            PeptideTable::update_protein_accession(
                &transaction,
                &mut peptides_of_updated_protein.iter(),
                stored_protein.get_accession().as_ref(),
                Some(updated_protein.get_accession().as_ref()),
            )
            .await?;
        } else if updated_protein.get_sequence() != stored_protein.get_sequence() {
            // Deassociatte the peptides which are not contained in the new protein
            Self::deassociate_protein_peptides_difference(
                &transaction,
                stored_protein,
                &peptides_of_stored_protein,
                &peptides_of_updated_protein,
            )
            .await?;
            // Create and associate the peptides which are not contained in the existing protein
            Self::create_protein_peptide_difference(
                &mut transaction,
                &peptides_of_stored_protein,
                &peptides_of_updated_protein,
            )
            .await?;
        }

        if flag_for_metadata_update {
            PeptideTable::unset_is_metadata_updated(
                &mut transaction,
                &mut peptides_of_updated_protein.iter(),
            )
            .await?;
        }

        // Update protein itself
        ProteinTable::update(&mut transaction, &stored_protein, &updated_protein).await?;

        transaction.commit().await?;
        peptide_sender
            .send(peptides_of_updated_protein.len() as u64)
            .await?;
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
    async fn deassociate_protein_peptides_difference<C>(
        client: &C,
        stored_protein: &Protein,
        peptides_from_stored_protein: &HashSet<Peptide>,
        peptides_from_updated_protein: &HashSet<Peptide>,
    ) -> Result<()>
    where
        C: GenericClient,
    {
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
    ///
    async fn create_protein_peptide_difference<C>(
        client: &C,
        peptides_from_stored_protein: &HashSet<Peptide>,
        peptides_from_updated_protein: &HashSet<Peptide>,
    ) -> Result<()>
    where
        C: GenericClient,
    {
        // Disassociate all peptides from existing protein which are not contained by the new protein
        let peptides_to_create: Vec<&Peptide> = peptides_from_updated_protein
            .difference(peptides_from_stored_protein)
            .collect::<Vec<&Peptide>>();

        if peptides_to_create.len() > 0 {
            PeptideTable::bulk_insert(client, peptides_to_create.into_iter()).await?
        }
        Ok(())
    }

    /// Handles the insertion of a new protein.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `protein` - The protein
    /// * `digestion_enzyme` - The enzyme which is used for digestion
    /// * `remove_peptides_containing_unknown` - If true, peptides containing unknown amino acids are removed
    /// * `partition_limits` - The partition limits
    ///
    async fn insert_protein(
        client: &mut Client,
        protein: &Protein,
        digestion_enzyme: &Box<dyn Enzyme>,
        remove_peptides_containing_unknown: bool,
        partition_limits: &Vec<i64>,
        peptide_sender: &Sender<u64>,
    ) -> Result<()> {
        // Digest protein
        let mut peptide_sequences = digestion_enzyme.digest(&protein.get_sequence());
        if remove_peptides_containing_unknown {
            remove_unknown_from_digest(&mut peptide_sequences);
        }
        let peptides = create_peptides_entities_from_digest::<Vec<Peptide>>(
            &peptide_sequences,
            partition_limits,
            Some(&protein),
        )?;

        let mut transaction = client.transaction().await?;
        ProteinTable::insert(&mut transaction, &protein).await?;
        PeptideTable::bulk_insert(&mut transaction, &mut peptides.iter()).await?;
        transaction.commit().await?;
        peptide_sender.send(peptides.len() as u64).await?;

        return Ok(());
    }

    async fn collect_peptide_metadata(
        num_threads: usize,
        database_url: &str,
        configuration: &Configuration,
    ) -> Result<()> {
        debug!("Collecting peptide metadata...");
        debug!("Chunking partitions for {} threads...", num_threads);
        let chunk_size = (configuration.get_partition_limits().len() as f64 / num_threads as f64)
            .ceil() as usize;
        let chunked_partitions: Vec<Vec<i64>> = (0..configuration.get_partition_limits().len()
            as i64)
            .collect::<Vec<i64>>()
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        let mut metadata_collector_thread_handles: Vec<JoinHandle<Result<()>>> = Vec::new();

        debug!("Starting {} threads...", num_threads);
        // Start digestion threads
        for thread_id in 0..num_threads {
            // Clone necessary variables
            let partitions = chunked_partitions[thread_id].clone();
            let database_url_clone = database_url.to_string();
            // TODO: Add logging thread
            // Start digestion thread
            metadata_collector_thread_handles.push(spawn(async move {
                Self::collect_peptide_metadata_thread(thread_id, database_url_clone, partitions)
                    .await?;
                Ok(())
            }));
        }
        debug!("Waiting threads to stop ...");
        // Wait for digestion threads to finish
        join_all(metadata_collector_thread_handles).await;
        Ok(())
    }

    async fn collect_peptide_metadata_thread(
        thread_id: usize,
        database_url: String,
        partitions: Vec<i64>,
    ) -> Result<()> {
        for partition in partitions.iter() {
            let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
            // The connection object performs the actual communication with the database,
            // so spawn it off to run on its own.
            let connection_handle = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("connection error: {}", e);
                }
            });

            let mut partition_transaction = client.transaction().await?;
            let cursor_statement = format!(
                "DECLARE peptide_cursor_{} CURSOR FOR SELECT * FROM {} WHERE partition = $1 AND is_metadata_updated = false;",
                thread_id,
                PeptideTable::table_name()
            );
            let fetch_statement = format!("FETCH 1000 FROM peptide_cursor_{};", thread_id);
            let cursor_close_statement = format!("CLOSE peptide_cursor_{};", thread_id);
            partition_transaction
                .execute(&cursor_statement, &[partition])
                .await?;
            loop {
                let rows = partition_transaction.query(&fetch_statement, &[]).await?;
                if rows.len() == 0 {
                    break;
                }
                let mut peptide_transaction = partition_transaction.transaction().await?;
                for row in rows {
                    let associated_proteins = ProteinTable::select_multiple(
                        &mut peptide_transaction,
                        "WHERE accession = ANY($1)",
                        &[&row.get::<_, Vec<String>>("proteins")],
                    )
                    .await?;
                    let (is_swiss_prot, is_trembl, taxonomy_ids, unique_taxonomy_ids, proteome_ids) =
                        Peptide::get_metadata_from_proteins(&associated_proteins);
                    let update_query = format!(
                        "UPDATE {} SET is_metadata_updated = true, is_swiss_prot = $1, is_trembl = $2, taxonomy_ids = $3, unique_taxonomy_ids = $4, proteome_ids = $5 WHERE partition = $6 AND mass = $7 and sequence = $8;",
                        PeptideTable::table_name()
                    );
                    peptide_transaction
                        .query(
                            &update_query,
                            &[
                                &is_swiss_prot,
                                &is_trembl,
                                &taxonomy_ids,
                                &unique_taxonomy_ids,
                                &proteome_ids,
                                &row.get::<_, i64>("partition"),
                                &row.get::<_, i64>("mass"),
                                &row.get::<_, &str>("sequence"),
                            ],
                        )
                        .await?;
                }
                peptide_transaction.commit().await?;
            }
            partition_transaction
                .execute(&cursor_close_statement, &[])
                .await?;
            partition_transaction.commit().await?;
            connection_handle.abort();
            let _ = connection_handle.await;
        }
        Ok(())
    }
}

impl DatabaseBuildTrait for DatabaseBuild {
    fn new(database_url: String) -> Self {
        return Self { database_url };
    }

    async fn build(
        &self,
        protein_file_paths: &Vec<PathBuf>,
        num_threads: usize,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        initial_configuration_opt: Option<Configuration>,
        log_folder: &PathBuf,
    ) -> Result<()> {
        let (mut client, connection) = tokio_postgres::connect(&self.database_url, NoTls).await?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        debug!("applying database migrations...");

        // Run migrations
        migrations::runner().run_async(&mut client).await?;

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

        connection_handle.abort();
        let _ = connection_handle.await;

        let digestion_enzyme = get_enzyme_by_name(
            configuration.get_enzyme_name(),
            configuration.get_max_number_of_missed_cleavages(),
            configuration.get_min_peptide_length(),
            configuration.get_max_peptide_length(),
        )?;

        // read, digest and insert proteins and peptides

        let mut protein_ctr: usize = 0;

        debug!("Counting proteins");

        for path in protein_file_paths.iter() {
            debug!("... {}", path.display());
            protein_ctr += Reader::new(path, 1024)?.count_proteins()?;
        }

        Self::protein_digestion(
            &self.database_url,
            num_threads,
            protein_file_paths,
            digestion_enzyme.as_ref(),
            configuration.get_remove_peptides_containing_unknown(),
            configuration.get_partition_limits().to_vec(),
            protein_ctr.clone(),
            log_folder,
        )
        .await?;
        // collect metadata
        Self::collect_peptide_metadata(num_threads, &self.database_url, &configuration).await?;
        // count peptides per partition

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
    use crate::biology::digestion_enzyme::functions::{
        create_peptides_entities_from_digest, get_enzyme_by_name,
    };
    use crate::database::citus::{
        peptide_table::PeptideTable,
        protein_table::ProteinTable,
        tests::{get_client, prepare_database_for_tests, DATABASE_URL},
    };
    use crate::database::selectable_table::SelectableTable;
    use crate::io::uniprot_text::reader::Reader;

    lazy_static! {
        static ref CONFIGURATION: Configuration =
            Configuration::new("trypsin".to_owned(), 2, 6, 50, true, Vec::with_capacity(0));
    }

    const EXPECTED_ASSOCIATED_PROTEINS_FOR_DUPLICATED_TRYPSIN: [&'static str; 2] =
        ["P07477", "DUPLIC"];
    const EXPECTED_ASSOCIATED_TAXONOMY_IDS_FOR_DUPLICATED_TRYPSIN: [i64; 2] = [9922, 9606];
    const EXPECTED_PROTEOME_IDS_FOR_DUPLICATED_TRYPSIN: [&'static str; 2] =
        ["UP000005640", "UP000291000"];

    // Test the database building
    #[tokio::test]
    #[serial]
    async fn test_database_build_without_initial_config() {
        let (mut client, connection) = get_client().await;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });
        prepare_database_for_tests(&mut client).await;

        let protein_file_paths = vec![Path::new("test_files/uniprot.txt").to_path_buf()];
        let log_folder = env::temp_dir().join("macpepdb_rs/database_build");
        if log_folder.exists() {
            remove_dir_all(&log_folder).unwrap();
        }
        create_dir_all(&log_folder).unwrap();

        let database_builder = DatabaseBuild::new(DATABASE_URL.to_owned());
        let build_res = database_builder
            .build(&protein_file_paths, 2, 100, 0.5, 0.0002, None, &log_folder)
            .await;
        assert!(build_res.is_err());
        connection_handle.abort();
        let _ = connection_handle.await;
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_database_build() {
        let (mut client, connection) = get_client().await;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        prepare_database_for_tests(&mut client).await;
        connection_handle.abort();
        let _ = connection_handle.await;

        let protein_file_paths = vec![
            Path::new("test_files/uniprot.txt").to_path_buf(),
            Path::new("test_files/trypsin_duplicate.txt").to_path_buf(),
        ];
        let log_folder = env::temp_dir().join("macpepdb_rs/database_build");
        if log_folder.exists() {
            remove_dir_all(&log_folder).unwrap();
        }
        create_dir_all(&log_folder).unwrap();

        let database_builder = DatabaseBuild::new(DATABASE_URL.to_owned());
        database_builder
            .build(
                &protein_file_paths,
                2,
                100,
                0.5,
                0.0002,
                Some(CONFIGURATION.clone()),
                &log_folder,
            )
            .await
            .unwrap();

        let (client, connection) = get_client().await;

        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        let configuration = ConfigurationTable::select(&client).await.unwrap();

        let enzyme = get_enzyme_by_name(
            configuration.get_enzyme_name(),
            configuration.get_max_number_of_missed_cleavages(),
            configuration.get_min_peptide_length(),
            configuration.get_max_peptide_length(),
        )
        .unwrap();

        // Check if every peptide is in the database
        for protein_file_path in protein_file_paths {
            let mut reader = Reader::new(&protein_file_path, 4096).unwrap();
            while let Some(protein) = reader.next().unwrap() {
                let proteins = ProteinTable::select_multiple(
                    &client,
                    "WHERE accession = $1",
                    &[protein.get_accession()],
                )
                .await
                .unwrap();
                assert_eq!(proteins.len(), 1);

                let expected_peptides: Vec<Peptide> = create_peptides_entities_from_digest(
                    &enzyme.digest(&protein.get_sequence()),
                    configuration.get_partition_limits(),
                    Some(&protein),
                )
                .unwrap();

                for peptide in expected_peptides {
                    let peptides = PeptideTable::select_multiple(
                        &client,
                        "WHERE partition = $1 AND mass = $2 AND sequence = $3",
                        &[
                            peptide.get_partition_as_ref(),
                            peptide.get_mass_as_ref(),
                            peptide.get_sequence(),
                        ],
                    )
                    .await
                    .unwrap();
                    assert_eq!(peptides.len(), 1);
                }
            }
        }

        // Select the duplicated trpsin protein
        // Digest it again, and check the metadata fit to the original trypsin and the duplicated trypsin
        let trypsin_duplicate = ProteinTable::select(&client, "WHERE accession = $1", &[&"DUPLIC"])
            .await
            .unwrap()
            .unwrap();

        let trypsin_duplicate_peptides: Vec<Peptide> = create_peptides_entities_from_digest(
            &enzyme.digest(&trypsin_duplicate.get_sequence()),
            configuration.get_partition_limits(),
            Some(&trypsin_duplicate),
        )
        .unwrap();

        for peptide in trypsin_duplicate_peptides {
            let peptide = PeptideTable::select(
                &client,
                "WHERE partition = $1 AND mass = $2 AND sequence = $3",
                &[
                    peptide.get_partition_as_ref(),
                    peptide.get_mass_as_ref(),
                    peptide.get_sequence(),
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
        connection_handle.abort();
        let _ = connection_handle.await;
    }

    // TODO: Test update
}
