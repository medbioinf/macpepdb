// std import
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use std::thread::{sleep, spawn, JoinHandle};
use std::time::Duration;

// 3rd party imports
use anyhow::{bail, Result};
use fallible_iterator::FallibleIterator;
use kdam::{tqdm, Bar, BarExt};
use postgres::{Client, NoTls};
use refinery::embed_migrations;

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

// add module migration to the current module
embed_migrations!("src/database/citus/migrations");

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
    /// * `progress_bar` - Progress bar
    /// * `verbose` - Verbose output
    ///
    fn get_or_set_configuration(
        client: &mut Client,
        protein_file_paths: &Vec<PathBuf>,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        initial_configuration_opt: Option<Configuration>,
        progress_bar: &mut Bar,
        verbose: bool,
    ) -> Result<Configuration> {
        let config_res = ConfigurationTable::select(client);
        // return if configuration is ok or if it is not a ConfigurationIncompleteError
        if config_res.as_ref().is_ok()
            || !config_res
                .as_ref()
                .unwrap_err()
                .is::<ConfigurationIncompleteError>()
        {
            if verbose && config_res.as_ref().is_ok() {
                progress_bar.write("found previous config ...");
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
            if verbose {
                progress_bar
                    .write("initial configuration has no partition limits list, creating one ...");
            }
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
            let partition_limits =
                partitioner.partition(num_partitions, None, progress_bar, verbose)?;
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
        ConfigurationTable::insert(client, &new_configuration)?;
        if verbose {
            progress_bar.write("new configuration saved ...");
        }
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
    /// * `progress_bar` - Progress bar
    /// * `verbose` - Verbose output
    ///
    fn protein_digestion(
        database_url: &str,
        num_threads: usize,
        protein_file_paths: &Vec<PathBuf>,
        digestion_enzyme: &dyn Enzyme,
        remove_peptides_containing_unknown: bool,
        partition_limits: Vec<i64>,
        progress_bar: &mut Bar,
        verbose: bool,
    ) -> Result<()> {
        progress_bar.set_disable(true);
        if verbose {
            progress_bar.write(format!(
                "processing proteins using {} threads ...",
                num_threads
            ));
        }

        let protein_queue_size = num_threads * 5;
        let protein_queue_arc: Arc<Mutex<Vec<Protein>>> = Arc::new(Mutex::new(Vec::new()));
        let partition_limits_arc = Arc::new(partition_limits);
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut digestion_thread_handles: Vec<JoinHandle<Result<()>>> = Vec::new();

        // Start digestion threads
        for thread_id in 0..num_threads {
            // Clone necessary variables
            let protein_queue_arc_clone = protein_queue_arc.clone();
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
            digestion_thread_handles.push(spawn(move || {
                Self::digestion_thread(
                    thread_id,
                    database_url_clone,
                    protein_queue_arc_clone,
                    partition_limits_arc_clone,
                    stop_flag_clone,
                    digestion_enzyme_box,
                    remove_peptides_containing_unknown,
                )?;
                Ok(())
            }));
        }
        for protein_file_path in protein_file_paths {
            let mut reader = Reader::new(protein_file_path, 4096)?;
            let mut wait_for_queue = false;
            while let Some(protein) = reader.next()? {
                loop {
                    if wait_for_queue {
                        // Wait before pushing the protein into queue
                        sleep(*PROTEIN_QUEUE_WRITE_SLEEP_TIME);
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

        if verbose {
            progress_bar.write("last proteins queued, waiting for digestion threads to finish ...")
        }

        for handle in digestion_thread_handles {
            match handle.join() {
                Ok(result) => result?,
                Err(err) => bail!(format!("Digestion thread failed: {:?}", err)),
            }
        }

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
    fn digestion_thread(
        thread_id: usize,
        database_url: String,
        protein_queue_arc: Arc<Mutex<Vec<Protein>>>,
        partition_limits_arc: Arc<Vec<i64>>,
        stop_flag: Arc<AtomicBool>,
        digestion_enzyme: Box<dyn Enzyme>,
        remove_peptides_containing_unknown: bool,
    ) -> Result<()> {
        let mut client = Client::connect(&database_url, NoTls)?;
        let mut wait_for_queue = true;
        loop {
            if wait_for_queue {
                // Wait before trying to get next protein from queue
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
                protein_queue.pop().unwrap() // unwrap is safe because we checked if queue is empty
            };
            let existing_protein: Option<Protein> = ProteinTable::select(
                &mut client,
                "WHERE accession = $1 OR accession = ANY($2)",
                &[protein.get_accession(), protein.get_secondary_accessions()],
            )?;
            // or contained in secondary accessions
            if let Some(existing_protein) = existing_protein {
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
                )?;
            } else {
                Self::insert_protein(
                    &mut client,
                    &protein,
                    &digestion_enzyme,
                    remove_peptides_containing_unknown,
                    &partition_limits_arc,
                )?;
            }
        }
        client.close()?;
        Ok(())
    }

    /// Handles the update of a protein, in case it was merged with another entry or has various changes.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `protein` - The new protein
    /// * `existing_protein` - The existing protein
    /// * `digestion_enzyme` - The enzyme which is used for digestion
    /// * `remove_peptides_containing_unknown` - If true, peptides containing unknown amino acids are removed
    /// * `partition_limits` - The partition limits
    ///
    fn update_protein(
        client: &mut Client,
        protein: &Protein,
        existing_protein: &Protein,
        digestion_enzyme: &Box<dyn Enzyme>,
        remove_peptides_containing_unknown: bool,
        partition_limits: &Vec<i64>,
    ) -> Result<()> {
        let mut existing_peptide_sequences =
            digestion_enzyme.digest(&existing_protein.get_sequence());
        let mut peptide_sequences = digestion_enzyme.digest(&protein.get_sequence());
        if remove_peptides_containing_unknown {
            remove_unknown_from_digest(&mut existing_peptide_sequences);
            remove_unknown_from_digest(&mut peptide_sequences);
        }
        let existing_peptides: HashSet<Peptide> = create_peptides_entities_from_digest(
            &existing_peptide_sequences,
            partition_limits,
            Some(&existing_protein),
        )?;
        let peptides: HashSet<Peptide> = create_peptides_entities_from_digest(
            &peptide_sequences,
            partition_limits,
            Some(&protein),
        )?;
        let mut transaction = client.transaction()?;
        let flag_for_metadata_update = protein.get_taxonomy_id()
            != existing_protein.get_taxonomy_id()
            || protein.get_proteome_id() != existing_protein.get_proteome_id();
        // If protein got a new accession (e.g. when entries were merged) and a new sequence
        if protein.get_accession() != existing_protein.get_accession()
            && protein.get_sequence() != existing_protein.get_sequence()
        {
            // Associate all new peptides
            PeptideTable::update_protein_accession(
                &mut transaction,
                &mut peptides.iter(),
                existing_protein.get_accession(),
                Some(protein.get_accession()),
            )?;
            // Disassociate all peptides from existing peptides which are not in new peptides
            let peptides_to_deassociate = existing_peptides
                .difference(&peptides)
                .collect::<Vec<&Peptide>>();

            if peptides_to_deassociate.len() > 0 {
                PeptideTable::update_protein_accession(
                    &mut transaction,
                    &mut peptides_to_deassociate.into_iter(),
                    existing_protein.get_accession(),
                    None,
                )?;
            }
        } else if protein.get_accession() != existing_protein.get_accession() {
            PeptideTable::update_protein_accession(
                &mut transaction,
                &mut peptides.iter(),
                existing_protein.get_accession().as_ref(),
                Some(protein.get_accession().as_ref()),
            )?;
        }

        if flag_for_metadata_update {
            PeptideTable::unset_is_metadata_updated(&mut transaction, &mut peptides.iter())?;
        }

        transaction.commit()?;
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
    fn insert_protein(
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

        let mut transaction = client.transaction()?;
        ProteinTable::insert(&mut transaction, &protein)?;
        PeptideTable::bulk_insert(&mut transaction, &mut peptides.iter())?;
        transaction.commit()?;

        return Ok(());
    }

    fn collect_peptide_metadata(
        num_threads: usize,
        database_url: &str,
        configuration: &Configuration,
        progress_bar: &mut Bar,
        verbose: bool,
    ) -> Result<()> {
        if verbose {
            println!("Collecting peptide metadata...");
            println!("Chunking partitions for {} threads...", num_threads);
        }
        let chunk_size = (configuration.get_partition_limits().len() as f64 / num_threads as f64)
            .ceil() as usize;
        let chunked_partitions: Vec<Vec<i64>> = (0..configuration.get_partition_limits().len()
            as i64)
            .collect::<Vec<i64>>()
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        let mut metadata_collector_thread_handles: Vec<JoinHandle<Result<()>>> = Vec::new();

        if verbose {
            println!("Starting {} threads...", num_threads);
        }
        // Start digestion threads
        for thread_id in 0..num_threads {
            // Clone necessary variables
            let partitions = chunked_partitions[thread_id].clone();
            let database_url_clone = database_url.to_string();
            // TODO: Add logging thread
            // Start digestion thread
            metadata_collector_thread_handles.push(spawn(move || {
                Self::collect_peptide_metadata_thread(thread_id, database_url_clone, partitions)?;
                Ok(())
            }));
        }
        if verbose {
            println!("Waiting threads to stop ...");
        }
        for handle in metadata_collector_thread_handles {
            match handle.join() {
                Ok(result) => result?,
                Err(err) => bail!(format!("Digestion thread failed: {:?}", err)),
            }
        }
        Ok(())
    }

    fn collect_peptide_metadata_thread(
        thread_id: usize,
        database_url: String,
        partitions: Vec<i64>,
    ) -> Result<()> {
        for partition in partitions.iter() {
            let mut client = Client::connect(&database_url, NoTls)?;
            let mut partition_transaction = client.transaction()?;
            let cursor_statement = format!(
                "DECLARE peptide_cursor_{} CURSOR FOR SELECT * FROM {} WHERE partition = $1 AND is_metadata_updated = false;",
                thread_id,
                PeptideTable::table_name()
            );
            let fetch_statement = format!("FETCH 1000 FROM peptide_cursor_{};", thread_id);
            let cursor_close_statement = format!("CLOSE peptide_cursor_{};", thread_id);
            partition_transaction.execute(&cursor_statement, &[partition])?;
            loop {
                let rows = partition_transaction.query(&fetch_statement, &[])?;
                if rows.len() == 0 {
                    break;
                }
                let mut peptide_transaction = partition_transaction.transaction()?;
                for row in rows {
                    let associated_proteins = ProteinTable::select_multiple(
                        &mut peptide_transaction,
                        "WHERE accession = ANY($1)",
                        &[&row.get::<_, Vec<String>>("proteins")],
                    )?;
                    let (is_swiss_prot, is_trembl, taxonomy_ids, unique_taxonomy_ids, proteome_ids) =
                        Peptide::get_metadata_from_proteins(&associated_proteins);
                    let update_query = format!(
                        "UPDATE {} SET is_metadata_updated = true, is_swiss_prot = $1, is_trembl = $2, taxonomy_ids = $3, unique_taxonomy_ids = $4, proteome_ids = $5 WHERE partition = $6 AND mass = $7 and sequence = $8;",
                        PeptideTable::table_name()
                    );
                    peptide_transaction.query(
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
                    )?;
                }
                peptide_transaction.commit()?;
            }
            partition_transaction.execute(&cursor_close_statement, &[])?;
            partition_transaction.commit()?
        }
        Ok(())
    }
}

impl DatabaseBuildTrait for DatabaseBuild {
    fn new(database_url: String) -> Self {
        return Self { database_url };
    }

    fn build(
        &self,
        protein_file_paths: &Vec<PathBuf>,
        num_threads: usize,
        num_partitions: u64,
        allowed_ram_usage: f64,
        partitioner_false_positive_probability: f64,
        initial_configuration_opt: Option<Configuration>,
        show_progress: bool,
        verbose: bool,
    ) -> Result<()> {
        let mut progress_bar = tqdm!(
            total = 0,
            desc = "partitioning",
            animation = "ascii",
            disable = !show_progress
        );

        let mut client = Client::connect(&self.database_url, NoTls)?;

        if verbose {
            progress_bar.write("applying database migrations...");
        }

        // Run migrations
        migrations::runner().run(&mut client)?;

        // get or set configuration
        let configuration = Self::get_or_set_configuration(
            &mut client,
            protein_file_paths,
            num_partitions,
            partitioner_false_positive_probability,
            allowed_ram_usage,
            initial_configuration_opt,
            &mut progress_bar,
            verbose,
        )?;

        let digestion_enzyme = get_enzyme_by_name(
            configuration.get_enzyme_name(),
            configuration.get_max_number_of_missed_cleavages(),
            configuration.get_min_peptide_length(),
            configuration.get_max_peptide_length(),
        )?;

        // read, digest and insert proteins and peptides
        Self::protein_digestion(
            &self.database_url,
            num_threads,
            protein_file_paths,
            digestion_enzyme.as_ref(),
            configuration.get_remove_peptides_containing_unknown(),
            configuration.get_partition_limits().to_vec(),
            &mut progress_bar,
            verbose,
        )?;
        // collect metadata
        Self::collect_peptide_metadata(
            num_threads,
            &self.database_url,
            &configuration,
            &mut progress_bar,
            verbose,
        )?;
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
    #[test]
    #[serial]
    fn test_database_build_without_initial_config() {
        prepare_database_for_tests();

        let protein_file_paths = vec![Path::new("test_files/uniprot.txt").to_path_buf()];

        let database_builder = DatabaseBuild::new(DATABASE_URL.to_owned());
        let build_res =
            database_builder.build(&protein_file_paths, 2, 100, 0.5, 0.0002, None, false, false);
        assert!(build_res.is_err());
    }

    #[test]
    #[serial]
    fn test_database_build() {
        prepare_database_for_tests();

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
                true,
                true,
            )
            .unwrap();

        let mut client = get_client();

        let configuration = ConfigurationTable::select(&mut client).unwrap();

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
                    &mut client,
                    "WHERE accession = $1",
                    &[protein.get_accession()],
                )
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
                        &mut client,
                        "WHERE partition = $1 AND mass = $2 AND sequence = $3",
                        &[
                            peptide.get_partition_as_ref(),
                            peptide.get_mass_as_ref(),
                            peptide.get_sequence(),
                        ],
                    )
                    .unwrap();
                    assert_eq!(peptides.len(), 1);
                }
            }

            // Select the duplicated trpsin protein
            // Digest it again, and check the metadata fit to the original trypsin and the duplicated trypsin
            let trypsin_duplicate =
                ProteinTable::select(&mut client, "WHERE accession = $1", &[&"DUPLIC"])
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
                    &mut client,
                    "WHERE partition = $1 AND mass = $2 AND sequence = $3",
                    &[
                        peptide.get_partition_as_ref(),
                        peptide.get_mass_as_ref(),
                        peptide.get_sequence(),
                    ],
                )
                .unwrap()
                .unwrap();
                assert_eq!(peptide.get_proteins().len(), 2);
                for protein_accession in peptide.get_proteins() {
                    assert!(EXPECTED_ASSOCIATED_PROTEINS_FOR_DUPLICATED_TRYPSIN
                        .contains(&protein_accession.as_str()));
                }
                for protein_accession in peptide.get_proteins() {
                    assert!(EXPECTED_ASSOCIATED_PROTEINS_FOR_DUPLICATED_TRYPSIN
                        .contains(&protein_accession.as_str()));
                }
                for taxonomy_id in peptide.get_taxonomy_ids() {
                    assert!(EXPECTED_ASSOCIATED_TAXONOMY_IDS_FOR_DUPLICATED_TRYPSIN
                        .contains(&taxonomy_id));
                }
                for taxonomy_id in peptide.get_unique_taxonomy_ids() {
                    assert!(EXPECTED_ASSOCIATED_TAXONOMY_IDS_FOR_DUPLICATED_TRYPSIN
                        .contains(&taxonomy_id));
                }
                for proteome_id in peptide.get_proteome_ids() {
                    assert!(EXPECTED_PROTEOME_IDS_FOR_DUPLICATED_TRYPSIN
                        .contains(&proteome_id.as_str()));
                }
                assert!(peptide.get_is_swiss_prot());
                assert!(peptide.get_is_trembl());
            }
        }
    }

    // TODO: Test update
}
