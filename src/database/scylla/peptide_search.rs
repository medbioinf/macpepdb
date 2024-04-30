use std::pin::Pin;
// std imports
use std::cmp::{max, min};
use std::sync::mpsc::{channel, Sender};
use std::sync::RwLock;
use std::sync::{Arc, Mutex};
use std::thread::available_parallelism;

// 3rd party imports
use anyhow::{bail, Result};
use async_stream::try_stream;
use dihardts_cstools::bloom_filter::BloomFilter;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use futures::{pin_mut, Stream, StreamExt};
use scylla::frame::response::result::CqlValue;

use crate::database::generic_client::GenericClient;
use crate::database::selectable_table::SelectableTable;
use crate::functions::post_translational_modification::get_ptm_conditions;
use crate::tools::peptide_partitioner::get_mass_partition;
// local imports
use crate::{
    database::scylla::peptide_table::PeptideTable, entities::peptide::Peptide,
    functions::post_translational_modification::PTMCondition,
};

use super::client::Client;

/// Trait to check conditions on peptides
///
pub trait FilterFunction: Send + Sync {
    fn is_match(&self, peptide: &Peptide) -> Result<bool>;
}

/// Filters peptides which not are in SwissProt
///
struct IsSwissProtFilterFunction;

impl FilterFunction for IsSwissProtFilterFunction {
    fn is_match(&self, peptide: &Peptide) -> Result<bool> {
        Ok(peptide.get_is_swiss_prot())
    }
}

/// Filter peptides which are not in TrEMBL
///
struct IsTrEMBLFilterFunction;

impl FilterFunction for IsTrEMBLFilterFunction {
    fn is_match(&self, peptide: &Peptide) -> Result<bool> {
        Ok(peptide.get_is_trembl())
    }
}

/// Makes sure that no peptide is returned twice
///
pub struct ThreadSafeDistinctFilterFunction {
    bloom_filter: RwLock<BloomFilter>,
}

impl FilterFunction for ThreadSafeDistinctFilterFunction {
    fn is_match(&self, peptide: &Peptide) -> Result<bool> {
        match self.bloom_filter.read() {
            Ok(bloom_filter) => {
                if bloom_filter.contains(peptide.get_sequence())? {
                    return Ok(false);
                }
            }
            Err(_) => bail!("Could not read bloom filter"),
        }

        match self.bloom_filter.write() {
            Ok(mut bloom_filter) => {
                // Check again
                if bloom_filter.contains(peptide.get_sequence())? {
                    return Ok(false);
                }
                bloom_filter.add(peptide.get_sequence())?;
                Ok(true)
            }
            Err(_) => bail!("Could not write bloom filter"),
        }
    }
}

/// Filters peptides which are not in the given taxonomy IDs
///
struct TaxonomyFilterFunction {
    taxonomy_ids: Vec<i64>,
}

impl FilterFunction for TaxonomyFilterFunction {
    fn is_match(&self, peptide: &Peptide) -> Result<bool> {
        for taxonomy_id in &self.taxonomy_ids {
            if peptide.get_taxonomy_ids().contains(taxonomy_id) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Filters peptides which are not in the given proteome IDs
///
struct ProteomeFilterFunction {
    proteome_ids: Vec<String>,
}

impl FilterFunction for ProteomeFilterFunction {
    fn is_match(&self, peptide: &Peptide) -> Result<bool> {
        for proteome_id in &self.proteome_ids {
            if peptide.get_proteome_ids().contains(proteome_id) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub type FalliblePeptideStream = Pin<Box<dyn Stream<Item = Result<Peptide>> + Send>>;

pub trait Search<'a> {
    fn search(
        client: Arc<Client>,
        partition_limits: Arc<Vec<i64>>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: i16,
        distinct: bool,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptms: Vec<PTM>,
        num_threads: Option<usize>,
    ) -> impl std::future::Future<Output = Result<FalliblePeptideStream>> + Send;

    fn create_filter_pipeline(
        distinct: bool,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
    ) -> Result<Vec<Box<dyn FilterFunction>>> {
        let mut filter_pipeline: Vec<Box<dyn FilterFunction>> = Vec::new();
        if distinct {
            filter_pipeline.push(Box::new(ThreadSafeDistinctFilterFunction {
                bloom_filter: RwLock::new(BloomFilter::new_by_size_and_fp_prob(80_000_000, 0.001)?),
            }));
        }
        if let Some(taxonomy_ids) = taxonomy_ids {
            filter_pipeline.push(Box::new(TaxonomyFilterFunction { taxonomy_ids }));
        }
        if let Some(proteome_ids) = proteome_ids {
            filter_pipeline.push(Box::new(ProteomeFilterFunction { proteome_ids }));
        }
        if let Some(is_reviewed) = is_reviewed {
            if is_reviewed {
                filter_pipeline.push(Box::new(IsSwissProtFilterFunction {}));
            } else {
                filter_pipeline.push(Box::new(IsTrEMBLFilterFunction {}));
            }
        }
        Ok(filter_pipeline)
    }

    /// In case the mass range exceeds on partition, this function will calculate the mass range for the partition.
    ///
    /// # Arguments
    /// * `partition` - The partition to calculate the mass range for
    /// * `partition_limits` - The partition limits
    /// * `lower_mass_limit` - The lower mass limit
    /// * `upper_mass_limit` - The upper mass limit
    ///
    fn get_query_limits_for_partition(
        partition: usize,
        partition_limits: &Vec<i64>,
        lower_mass_limit: i64,
        upper_mass_limit: i64,
    ) -> (CqlValue, CqlValue) {
        // Get mass limits for partition
        let partition_lower_mass_limit = if partition > 0 {
            partition_limits[partition as usize - 1] + 1
        } else {
            0
        };
        let partition_upper_mass_limit = partition_limits[partition as usize];

        let query_lower_mass_limit =
            CqlValue::BigInt(max(lower_mass_limit, partition_lower_mass_limit));
        let query_upper_mass_limit =
            CqlValue::BigInt(min(upper_mass_limit, partition_upper_mass_limit));
        (query_lower_mass_limit, query_upper_mass_limit)
    }

    /// Query PTM condition which needs more work prior to the actual query than just querying the mass.
    ///
    /// # Arguments
    /// * `client` - The client to use for the query
    /// * `partition_limits` - The partition limits
    /// * `ptm_condition` - The PTM condition to query
    /// * `lower_mass_tolerance_ppm` - The lower mass tolerance in ppm
    /// * `upper_mass_tolerance_ppm` - The upper mass tolerance in ppm
    /// * `filter_pipeline` - The filter pipeline
    /// * `peptide_sender` - The sender to send the peptides to the final stream
    ///
    fn search_with_ptm_conditions(
        client: Arc<Client>,
        partition_limits: Arc<Vec<i64>>,
        ptm_condition: PTMCondition,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        filter_pipeline: Arc<Vec<Box<dyn FilterFunction>>>,
        peptide_sender: Sender<Result<Peptide>>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // Calculate mass range based on ptm condition
            let lower_mass_limit = ptm_condition.get_mass()
                - (ptm_condition.get_mass() / 1000000 * lower_mass_tolerance_ppm);
            let upper_mass_limit = ptm_condition.get_mass()
                + (ptm_condition.get_mass() / 1000000 * upper_mass_tolerance_ppm);

            // Get partition
            let lower_partition_index =
                get_mass_partition(partition_limits.as_ref(), lower_mass_limit)?;
            let upper_partition_index =
                get_mass_partition(partition_limits.as_ref(), upper_mass_limit)?;

            for partition in lower_partition_index..=upper_partition_index {
                let (query_lower_mass_limit, query_upper_mass_limit) =
                    if lower_partition_index != upper_partition_index {
                        // in case we have to query multiple partitions make sure only query the mass range for the partition
                        // E.g s`lower_mass_limit` might by in partition n but no in n+1 as
                        Self::get_query_limits_for_partition(
                            partition,
                            partition_limits.as_ref(),
                            lower_mass_limit,
                            upper_mass_limit,
                        )
                    } else {
                        // If everything is in one partition, just query the mass range
                        (
                            CqlValue::BigInt(lower_mass_limit),
                            CqlValue::BigInt(upper_mass_limit),
                        )
                    };

                let partition_cql_value = CqlValue::BigInt(partition as i64);
                let query_params = vec![
                    &partition_cql_value,
                    &query_lower_mass_limit,
                    &query_upper_mass_limit,
                ];

                let peptide_stream = PeptideTable::stream(
                    client.as_ref(),
                    "WHERE partition = ? AND mass >= ? AND mass <= ?",
                    &query_params,
                    10000,
                )
                .await?;
                pin_mut!(peptide_stream);
                while let Some(peptide) = peptide_stream.next().await {
                    let peptide = peptide?;
                    if !ptm_condition.check_peptide(&peptide) {
                        continue;
                    }
                    for filter in filter_pipeline.iter() {
                        if !filter.is_match(&peptide)? {
                            continue;
                        }
                    }
                    peptide_sender.send(Ok(peptide))?;
                }
            }
            Ok(())
        }
    }

    /// Query PTM condition which needs more work prior to the actual query than just querying the mass.
    ///
    /// # Arguments
    /// * `client` - The client to use for the query
    /// * `partition_limits` - The partition limits
    /// * `mass` - The mass to query
    /// * `lower_mass_tolerance_ppm` - The lower mass tolerance in ppm
    /// * `upper_mass_tolerance_ppm` - The upper mass tolerance in ppm
    /// * `ptm_condition` - The PTM condition to query
    /// * `filter_pipeline` - The filter pipeline
    /// * `peptide_sender` - The sender to send the peptides to the final stream
    ///
    fn search_without_ptm_condition(
        client: Arc<Client>,
        partition_limits: Arc<Vec<i64>>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        filter_pipeline: Arc<Vec<Box<dyn FilterFunction>>>,
        peptide_sender: Sender<Result<Peptide>>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // Calculate mass range
            let lower_mass_limit = mass - (mass / 1000000 * lower_mass_tolerance_ppm);
            let upper_mass_limit = mass + (mass / 1000000 * upper_mass_tolerance_ppm);

            // Get partition
            let lower_partition_index =
                get_mass_partition(partition_limits.as_ref(), lower_mass_limit)?;
            let upper_partition_index =
                get_mass_partition(partition_limits.as_ref(), upper_mass_limit)?;

            for partition in lower_partition_index..=upper_partition_index {
                let (query_lower_mass_limit, query_upper_mass_limit) =
                    if lower_partition_index != upper_partition_index {
                        // in case we have to query multiple partitions make sure only query the mass range for the partition
                        // E.g s`lower_mass_limit` might by in partition n but no in n+1 as
                        Self::get_query_limits_for_partition(
                            partition,
                            partition_limits.as_ref(),
                            lower_mass_limit,
                            upper_mass_limit,
                        )
                    } else {
                        // If everything is in one partition, just query the mass range
                        (
                            CqlValue::BigInt(lower_mass_limit),
                            CqlValue::BigInt(upper_mass_limit),
                        )
                    };

                let partition_cql_value = CqlValue::BigInt(partition as i64);
                let query_params = vec![
                    &partition_cql_value,
                    &query_lower_mass_limit,
                    &query_upper_mass_limit,
                ];

                let peptide_stream = PeptideTable::stream(
                    client.as_ref(),
                    "WHERE partition = ? AND mass >= ? AND mass <= ?",
                    &query_params,
                    10000,
                )
                .await?;
                pin_mut!(peptide_stream);
                while let Some(peptide) = peptide_stream.next().await {
                    let peptide = peptide?;
                    for filter in filter_pipeline.iter() {
                        if !filter.is_match(&peptide)? {
                            continue;
                        }
                    }
                    peptide_sender.send(Ok(peptide))?;
                }
            }
            Ok(())
        }
    }
}

/// Asynchronous filter where one task is spawned for each PTM condition.
///
pub struct MultiTaskSearch;

impl<'a> Search<'a> for MultiTaskSearch {
    async fn search(
        client: Arc<Client>,
        partition_limits: Arc<Vec<i64>>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: i16,
        distinct: bool,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptms: Vec<PTM>,
        _num_threads: Option<usize>,
    ) -> Result<FalliblePeptideStream> {
        let ptm_conditions = get_ptm_conditions(mass, max_variable_modifications, &ptms)?;
        let filter_pipeline = Arc::new(Self::create_filter_pipeline(
            distinct,
            taxonomy_ids,
            proteome_ids,
            is_reviewed,
        )?);

        let (peptide_sender, peptide_receiver) = channel::<Result<Peptide>>();

        Ok(Box::pin(try_stream! {
            let _tasks: Vec<tokio::task::JoinHandle<Result<()>>> = if ptm_conditions.len() > 0 {
                ptm_conditions
                .into_iter()
                .map(|ptm_condition|
                    // Spawn on task for each PTM condition
                    tokio::task::spawn(
                        Self::search_with_ptm_conditions(
                            client.clone(),
                            partition_limits.clone(),
                            ptm_condition,
                            lower_mass_tolerance_ppm,
                            upper_mass_tolerance_ppm,
                            filter_pipeline.clone(),
                            peptide_sender.clone(),
                        )
                    )
                ).collect()
            } else {
                // Spawn one task for the mass range as there are not PTMs
                vec![
                    tokio::task::spawn(
                        Self::search_without_ptm_condition(
                            client.clone(),
                            partition_limits.clone(),
                            mass,
                            lower_mass_tolerance_ppm,
                            upper_mass_tolerance_ppm,
                            filter_pipeline.clone(),
                            peptide_sender.clone(),
                        )
                    )
                ]
            };

            drop(peptide_sender);

            loop {
                match peptide_receiver.recv() {
                    Ok(peptide) => yield peptide?,
                    Err(_) => break,
                }
            }

            loop {
                match peptide_receiver.recv() {
                    Ok(peptide) => yield peptide?,
                    Err(_) => break,
                }
            }
        }))
    }
}

/// Multi-threaded filter where one thread is spawned for each PTM condition but they share a single client.
/// Each thread will spawn single threaded Tokio engine to deal with Scylla's async driver.
///
/// **Attention:** All performance tests indicate, that [MultiTaskFilter] is the fastest search.
/// Therefore this one is also not feature complete as it lacks the ability to query a given mass without any PTMs
///
pub struct MultiThreadSingleClientSearch;

impl<'a> Search<'a> for MultiThreadSingleClientSearch {
    async fn search(
        client: Arc<Client>,
        partition_limits: Arc<Vec<i64>>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: i16,
        distinct: bool,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptms: Vec<PTM>,
        _num_threads: Option<usize>,
    ) -> Result<FalliblePeptideStream> {
        let ptm_conditions = get_ptm_conditions(mass, max_variable_modifications, &ptms)?;

        let filter_pipeline = Arc::new(Self::create_filter_pipeline(
            distinct,
            taxonomy_ids,
            proteome_ids,
            is_reviewed,
        )?);

        let (peptide_sender, peptide_receiver) = channel::<Result<Peptide>>();

        Ok(Box::pin(try_stream! {
            let _threads: Vec<std::thread::JoinHandle<Result<()>>> = ptm_conditions
                .into_iter()
                .map(|ptm_condition| {
                    let thread_client = client.clone();
                    let thread_partition_limits = partition_limits.clone();
                    let thread_filter_pipeline = filter_pipeline.clone();
                    let thread_peptide_sender = peptide_sender.clone();
                    std::thread::spawn(move || {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()?;

                        runtime.block_on(
                            Self::search_with_ptm_conditions(
                                thread_client,
                                thread_partition_limits,
                                ptm_condition,
                                lower_mass_tolerance_ppm,
                                upper_mass_tolerance_ppm,
                                thread_filter_pipeline,
                                thread_peptide_sender,
                            )
                        )
                    })
                }).collect();

            drop(peptide_sender);

            loop {
                match peptide_receiver.recv() {
                    Ok(peptide) => yield peptide?,
                    Err(_) => break,
                }
            }
        }))
    }
}

/// Multi-threaded filter where one thread is spawned for each PTM condition and a separate client.
/// Each thread will spawn single threaded Tokio engine to deal with Scylla's async driver.
///
/// **Attention:** All performance tests indicate, that [MultiTaskFilter] is the fastest search.
/// Therefore this one is also not feature complete as it lacks the ability to query a given mass without any PTMs
///
///
pub struct MultiThreadMultiClientSearch {}

impl<'a> Search<'a> for MultiThreadMultiClientSearch {
    async fn search(
        client: Arc<Client>,
        partition_limits: Arc<Vec<i64>>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: i16,
        distinct: bool,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptms: Vec<PTM>,
        _num_threads: Option<usize>,
    ) -> Result<FalliblePeptideStream> {
        let ptm_conditions = get_ptm_conditions(mass, max_variable_modifications, &ptms)?;

        let filter_pipeline = Arc::new(Self::create_filter_pipeline(
            distinct,
            taxonomy_ids,
            proteome_ids,
            is_reviewed,
        )?);

        let (peptide_sender, peptide_receiver) = channel::<Result<Peptide>>();

        let mut clients = Vec::with_capacity(ptm_conditions.len());
        for _ in 0..ptm_conditions.len() {
            clients.push(Arc::new(Client::new(client.get_url()).await?));
        }

        Ok(Box::pin(try_stream! {
            let _threads: Vec<std::thread::JoinHandle<Result<()>>> = ptm_conditions
                .into_iter()
                .map(|ptm_condition| {
                    let thread_client = clients.pop().unwrap();
                    let thread_partition_limits = partition_limits.clone();
                    let thread_filter_pipeline = filter_pipeline.clone();
                    let thread_peptide_sender = peptide_sender.clone();
                    std::thread::spawn(move || {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()?;

                        runtime.block_on(
                            Self::search_with_ptm_conditions(
                                thread_client,
                                thread_partition_limits,
                                ptm_condition,
                                lower_mass_tolerance_ppm,
                                upper_mass_tolerance_ppm,
                                thread_filter_pipeline,
                                thread_peptide_sender,
                            )
                        )
                    })
                }).collect();

            drop(peptide_sender);

            loop {
                match peptide_receiver.recv() {
                    Ok(peptide) => yield peptide?,
                    Err(_) => break,
                }
            }
        }))
    }
}

/// Multi-threaded filter where a number of threads are spawned, each one is using the same DB client and is processing
/// one PTM condition until the each one is processed.
/// Each thread will spawn single threaded Tokio engine to deal with Scylla's async driver.
///
/// **Attention:** All performance tests indicate, that [MultiTaskFilter] is the fastest search.
/// Therefore this one is also not feature complete as it lacks the ability to query a given mass without any PTMs
///
pub struct QueuedMultiThreadSingleClientSearch;

impl<'a> Search<'a> for QueuedMultiThreadSingleClientSearch {
    async fn search(
        client: Arc<Client>,
        partition_limits: Arc<Vec<i64>>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: i16,
        distinct: bool,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptms: Vec<PTM>,
        num_threads: Option<usize>,
    ) -> Result<FalliblePeptideStream> {
        let num_threads = num_threads.unwrap_or(available_parallelism()?.get());

        let ptm_conditions = get_ptm_conditions(mass, max_variable_modifications, &ptms)?;
        let ptm_conditions = Arc::new(Mutex::new(ptm_conditions));

        let filter_pipeline = Arc::new(Self::create_filter_pipeline(
            distinct,
            taxonomy_ids,
            proteome_ids,
            is_reviewed,
        )?);

        let (peptide_sender, peptide_receiver) = channel::<Result<Peptide>>();

        Ok(Box::pin(try_stream! {
            let _threads: Vec<std::thread::JoinHandle<Result<()>>> = (0..num_threads)
                .map(|_| {
                    let thread_ptm_conditions = ptm_conditions.clone();
                    let thread_client = client.clone();
                    let thread_partition_limits = partition_limits.clone();
                    let thread_filter_pipeline = filter_pipeline.clone();
                    let thread_peptide_sender = peptide_sender.clone();
                    std::thread::spawn(move || {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()?;

                        loop {
                            let ptm_condition = match thread_ptm_conditions.lock() {
                                Ok(mut ptm_conditions) => ptm_conditions.pop(),
                                Err(_) => bail!("Could not lock PTM conditions"),
                            };
                            if ptm_condition.is_none() {
                                break;
                            }
                            let ptm_condition = ptm_condition.unwrap();

                            runtime.block_on(
                                Self::search_with_ptm_conditions(
                                    thread_client.clone(),
                                    thread_partition_limits.clone(),
                                    ptm_condition,
                                    lower_mass_tolerance_ppm,
                                    upper_mass_tolerance_ppm,
                                    thread_filter_pipeline.clone(),
                                    thread_peptide_sender.clone(),
                                )
                            )?;
                        }
                        Ok(())
                    })
                }).collect();

            drop(peptide_sender);

            loop {
                match peptide_receiver.recv() {
                    Ok(peptide) => yield peptide?,
                    Err(_) => break,
                }
            }
        }))
    }
}

/// Multi-threaded filter where a number of threads are spawned, each one is using the same DB client and is processing
/// one PTM condition until the each one is processed.
/// Each thread will spawn single threaded Tokio engine to deal with Scylla's async driver.
///
/// **Attention:** All performance tests indicate, that [MultiTaskFilter] is the fastest search.
/// Therefore this one is also not feature complete as it lacks the ability to query a given mass without any PTMs
///
pub struct QueuedMultiThreadMultiClientSearch;

impl<'a> Search<'a> for QueuedMultiThreadMultiClientSearch {
    async fn search(
        client: Arc<Client>,
        partition_limits: Arc<Vec<i64>>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: i16,
        distinct: bool,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptms: Vec<PTM>,
        num_threads: Option<usize>,
    ) -> Result<FalliblePeptideStream> {
        let num_threads = num_threads.unwrap_or(available_parallelism()?.get());

        let ptm_conditions = get_ptm_conditions(mass, max_variable_modifications, &ptms)?;

        let filter_pipeline = Arc::new(Self::create_filter_pipeline(
            distinct,
            taxonomy_ids,
            proteome_ids,
            is_reviewed,
        )?);

        let mut clients = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            clients.push(Arc::new(Client::new(client.get_url()).await?));
        }

        let ptm_conditions = Arc::new(Mutex::new(ptm_conditions));

        let (peptide_sender, peptide_receiver) = channel::<Result<Peptide>>();

        Ok(Box::pin(try_stream! {
            let _threads: Vec<std::thread::JoinHandle<Result<()>>> = (0..num_threads)
                .map(|_| {
                    let thread_ptm_conditions = ptm_conditions.clone();
                    let thread_client = clients.pop().unwrap();
                    let thread_partition_limits = partition_limits.clone();
                    let thread_filter_pipeline = filter_pipeline.clone();
                    let thread_peptide_sender = peptide_sender.clone();
                    std::thread::spawn(move || {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()?;

                        loop {
                            let ptm_condition = match thread_ptm_conditions.lock() {
                                Ok(mut ptm_conditions) => ptm_conditions.pop(),
                                Err(_) => bail!("Could not lock PTM conditions"),
                            };
                            if ptm_condition.is_none() {
                                break;
                            }
                            let ptm_condition = ptm_condition.unwrap();

                            runtime.block_on(
                                Self::search_with_ptm_conditions(
                                    thread_client.clone(),
                                    thread_partition_limits.clone(),
                                    ptm_condition,
                                    lower_mass_tolerance_ppm,
                                    upper_mass_tolerance_ppm,
                                    thread_filter_pipeline.clone(),
                                    thread_peptide_sender.clone(),
                                )
                            )?;
                        }
                        Ok(())
                    })
                }).collect();

            drop(peptide_sender);

            loop {
                match peptide_receiver.recv() {
                    Ok(peptide) => yield peptide?,
                    Err(_) => break,
                }
            }
        }))
    }
}
