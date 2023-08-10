use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

// 3rd party imports
use anyhow::{bail, Result};
use fallible_iterator::FallibleIterator;
use futures::executor::block_on;
use futures::future::join_all;
use futures::StreamExt;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tracing::{debug, debug_span, info, info_span, span, Level, Span};

// internal imports
use crate::biology::digestion_enzyme::{
    enzyme::Enzyme,
    functions::{
        create_peptides_entities_from_digest, get_enzyme_by_name, remove_unknown_from_digest,
    },
};
use crate::database::configuration_table::{
    ConfigurationIncompleteError, ConfigurationTable as ConfigurationTableTrait,
};
use crate::database::database_build::DatabaseBuild as DatabaseBuildTrait;
use crate::database::scylla::client::Client;
use crate::database::scylla::client::GenericClient;
use crate::database::scylla::migrations::run_migrations;
use crate::database::scylla::peptide_table::SELECT_COLS;
use crate::database::scylla::{
    configuration_table::ConfigurationTable, get_client, peptide_table::PeptideTable,
    protein_table::ProteinTable,
};
use crate::database::selectable_table::SelectableTable;
use crate::database::table::Table;
use crate::tools::performance_logger::performance_log_thread;
use scylla::frame::response::result::{CqlValue, Row};

use crate::entities::{configuration::Configuration, peptide::Peptide, protein::Protein};
use crate::io::uniprot_text::reader::Reader;
use crate::tools::peptide_partitioner::PeptidePartitioner;

use super::{prepare_database_for_tests, SCYLLA_KEYSPACE_NAME};

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
                debug!("Found previous config");
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
            info!("initial configuration has no partition limits list, creating one ...");
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
        info!("new configuration saved ...");

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
    ) -> Result<()> {
        debug!("processing proteins using {} threads ...", num_threads);

        let protein_queue_size = num_threads * 300;

        let protein_queue_arc: Arc<Mutex<Vec<Protein>>> = Arc::new(Mutex::new(Vec::new()));
        let partition_limits_arc: Arc<Vec<i64>> = Arc::new(partition_limits);
        let stop_flag = Arc::new(AtomicBool::new(false));
        let num_proteins_processed: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));

        let mut digestion_thread_handles: Vec<JoinHandle<Result<()>>> = Vec::new();

        // Start digestion threads
        for thread_id in 0..num_threads {
            // Clone necessary variables
            let protein_queue_arc_clone = protein_queue_arc.clone();
            let num_proteins_processed = Arc::clone(&num_proteins_processed);
            let partition_limits_arc_clone = partition_limits_arc.clone();
            let stop_flag_clone = stop_flag.clone();
            let database_url_clone = database_url.to_string();
            // Create a boxed enzyme
            let digestion_enzyme_box = get_enzyme_by_name(
                digestion_enzyme.get_name(),
                digestion_enzyme.get_max_number_of_missed_cleavages(),
                digestion_enzyme.get_min_peptide_length(),
                digestion_enzyme.get_max_peptide_length(),
            )?;
            // TODO: Add logging thread
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
                    num_proteins_processed,
                )
                .await?;
                Ok(())
            }));
        }

        let num_proteins_processed = Arc::clone(&num_proteins_processed);
        let performance_stop_flag = Arc::new(AtomicBool::new(false));
        let protein_queue_arc_clone = protein_queue_arc.clone();
        let stop_flag_clone = performance_stop_flag.clone();
        let performance_log_thread_handle: JoinHandle<Result<()>> = spawn(async move {
            performance_log_thread(
                &num_proteins,
                num_proteins_processed,
                protein_queue_arc_clone,
                stop_flag_clone,
            )
            .await;
            Ok(())
        });

        let mut last_wait_instant: Option<Instant> = None;

        for protein_file_path in protein_file_paths {
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
        num_proteins_processed: Arc<Mutex<u64>>,
    ) -> Result<()> {
        let mut client = get_client(Some(database_url.as_str())).await.unwrap();

        let mut wait_for_queue = true;

        let performance_span = debug_span!("performance", thread_id);
        let performance_span_enter = performance_span.enter();

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
                let mut i = num_proteins_processed.lock().unwrap();
                *i += 1;
                protein
            };

            let mut accession_list = protein.get_secondary_accessions().clone();
            accession_list.push(protein.get_accession().to_owned());

            let existing_protein_result: Result<Option<Protein>> = ProteinTable::select(
                &client,
                "WHERE accession IN (?)",
                &[&CqlValue::Text(accession_list.join(","))],
            )
            .await;

            // or contained in secondary accessions
            if existing_protein_result.as_ref().is_ok_and(|x| x.is_some()) {
                let existing_protein = existing_protein_result?.unwrap();
                if existing_protein.get_updated_at() == protein.get_updated_at() {
                    continue;
                }
                Self::update_protein(
                    &mut client,
                    &protein,
                    &existing_protein,
                    &digestion_enzyme,
                    remove_peptides_containing_unknown,
                    &partition_limits_arc,
                )
                .await?;
            } else {
                Self::insert_protein(
                    &mut client,
                    &protein,
                    &digestion_enzyme,
                    remove_peptides_containing_unknown,
                    &partition_limits_arc,
                )
                .await?;
            }
        }
        std::mem::drop(performance_span_enter);
        std::mem::drop(performance_span);

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
        ProteinTable::update(client, &stored_protein, &updated_protein).await?;

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

        ProteinTable::insert(client, &protein).await?;
        PeptideTable::bulk_insert(client, &mut peptides.iter()).await?;

        return Ok(());
    }

    async fn collect_peptide_metadata(
        num_threads: usize,
        database_url: &str,
        configuration: &Configuration,
    ) -> Result<()> {
        debug!("Collecting peptide metadata...");
        debug!("Chunking partitions for {} threads...", num_threads);
        let chunk_size = ((configuration.get_partition_limits().len() as f64 + 1.0)
            / num_threads as f64)
            .ceil() as usize;
        let chunked_partitions: Vec<Vec<i64>> =
            (0..(configuration.get_partition_limits().len() as i64 + 1))
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
                let future = Self::collect_peptide_metadata_thread(
                    thread_id,
                    database_url_clone,
                    partitions,
                )
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
        let mut client = get_client(Some(database_url.as_str())).await?;
        let mut session = client.get_session();
        let update_query = format!(
                        "UPDATE {}.{} SET is_metadata_updated = true, is_swiss_prot = ?, is_trembl = ?, taxonomy_ids = ?, unique_taxonomy_ids = ?, proteome_ids = ? WHERE partition = ? AND mass = ? and sequence = ?",
                        SCYLLA_KEYSPACE_NAME,
                        PeptideTable::table_name()
                    );
        let update_query_prepared_statement = session.prepare(update_query).await?;

        for partition in partitions.iter() {
            let query_statement = format!(
                "SELECT {} FROM {}.{} WHERE partition = ? AND is_metadata_updated = false ALLOW FILTERING",
                SELECT_COLS,
                SCYLLA_KEYSPACE_NAME,
                PeptideTable::table_name()
            );
            let mut rows_stream = session
                .query_iter(query_statement, (partition,))
                .await
                .unwrap();

            while let Some(row_opt) = rows_stream.next().await {
                let row = row_opt?;
                // ToDo: This might be bad performance wise
                let peptide = Peptide::from(row);
                let associated_proteins = ProteinTable::select_multiple(
                    &client,
                    "WHERE accession IN ?",
                    &[&CqlValue::List(
                        peptide
                            .get_proteins()
                            .into_iter()
                            .map(|x| CqlValue::Text(x.to_owned()))
                            .collect(),
                    )],
                )
                .await?;

                let (is_swiss_prot, is_trembl, taxonomy_ids, unique_taxonomy_ids, proteome_ids) =
                    Peptide::get_metadata_from_proteins(&associated_proteins);
                session
                    .execute(
                        &update_query_prepared_statement,
                        (
                            &is_swiss_prot,
                            &is_trembl,
                            &taxonomy_ids,
                            &unique_taxonomy_ids,
                            &proteome_ids,
                            peptide.get_partition(),
                            peptide.get_mass(),
                            peptide.get_sequence(),
                        ),
                    )
                    .await?;
            }
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
    ) -> Result<()> {
        info!("Starting database build");

        let mut client = get_client(Some(&self.database_url)).await?;
        let mut session = client.get_session();

        debug!("applying database migrations...");

        // Run migrations
        run_migrations(&client).await;
        // migrations::runner().run_async(&mut client).await?;

        info!("Getting / Setting configuration");
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

        let digestion_enzyme = get_enzyme_by_name(
            configuration.get_enzyme_name(),
            configuration.get_max_number_of_missed_cleavages(),
            configuration.get_min_peptide_length(),
            configuration.get_max_peptide_length(),
        )?;

        // read, digest and insert proteins and peptides
        info!("Starting digest and insert");

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
        )
        .await?;

        // collect metadata
        let span = span!(Level::INFO, "metadata_updates");
        let _guard = span.enter();

        Self::collect_peptide_metadata(num_threads, &self.database_url, &configuration).await?;
        // count peptides per partition

        Ok(())
    }
}

#[cfg(test)]
mod test {
    // std imports
    use std::path::Path;

    // 3rd party imports
    use serial_test::serial;
    use tracing_test::traced_test;

    // internal imports
    use super::*;
    use crate::biology::digestion_enzyme::functions::{
        create_peptides_entities_from_digest, get_enzyme_by_name,
    };
    use crate::database::scylla::drop_keyspace;
    use crate::database::scylla::{
        peptide_table::PeptideTable,
        protein_table::ProteinTable,
        {get_client, prepare_database_for_tests, DATABASE_URL},
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
        let mut client = get_client(None).await.unwrap();
        let mut session = client.get_session();

        drop_keyspace(&client).await;

        let protein_file_paths = vec![Path::new("test_files/uniprot.txt").to_path_buf()];

        let database_builder = DatabaseBuild::new(DATABASE_URL.to_owned());
        let build_res = database_builder
            .build(&protein_file_paths, 2, 100, 0.5, 0.0002, None)
            .await;
        assert!(build_res.is_err());
    }

    #[tokio::test]
    #[traced_test]
    #[serial]
    async fn test_database_build() {
        let mut client = get_client(None).await.unwrap();
        let mut session = client.get_session();

        drop_keyspace(&client).await;

        let protein_file_paths = vec![
            Path::new("test_files/uniprot.txt").to_path_buf(),
            Path::new("test_files/trypsin_duplicate.txt").to_path_buf(),
        ];

        let database_builder = DatabaseBuild::new(DATABASE_URL.to_owned());
        database_builder
            .build(
                &protein_file_paths,
                2,
                100,
                0.5,
                0.0002,
                Some(CONFIGURATION.clone()),
            )
            .await
            .unwrap();

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
                    "WHERE accession = ?",
                    &[&CqlValue::Text(protein.get_accession().to_owned())],
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

        let trypsin_duplicate_peptides: Vec<Peptide> = create_peptides_entities_from_digest(
            &enzyme.digest(&trypsin_duplicate.get_sequence()),
            configuration.get_partition_limits(),
            Some(&trypsin_duplicate),
        )
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
