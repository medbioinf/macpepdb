use std::pin::Pin;
// std imports
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

/// Filter function for SwissProt
///
struct IsSwissProtFilterFunction;

impl FilterFunction for IsSwissProtFilterFunction {
    fn is_match(&self, peptide: &Peptide) -> Result<bool> {
        Ok(peptide.get_is_swiss_prot())
    }
}

/// Filter function for TrEMBL
///
struct IsTrEMBLFilterFunction;

impl FilterFunction for IsTrEMBLFilterFunction {
    fn is_match(&self, peptide: &Peptide) -> Result<bool> {
        Ok(peptide.get_is_trembl())
    }
}

/// Filter function for PTM
///
struct PTMConditionFilterFunction {
    ptm_condition: PTMCondition,
}

impl FilterFunction for PTMConditionFilterFunction {
    fn is_match(&self, peptide: &Peptide) -> Result<bool> {
        Ok(self.ptm_condition.check_peptide(peptide))
    }
}

/// Distinct filter function for each PTM condition
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

pub type FalliblePeptideStream = Pin<Box<dyn Stream<Item = Result<Peptide>>>>;

pub trait Filter<'a> {
    fn filter(
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
    fn filter_with_ptm_conditions(
        client: Arc<Client>,
        partition_limits: Arc<Vec<i64>>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        ptm_condition: PTMCondition,
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

            // Convert to CqlValue
            let lower_mass_limit = CqlValue::BigInt(lower_mass_limit);
            let upper_mass_limit = CqlValue::BigInt(upper_mass_limit);

            for partition in lower_partition_index..=upper_partition_index {
                let partition_cql_value = CqlValue::BigInt(partition as i64);
                let query_params = vec![&partition_cql_value, &lower_mass_limit, &upper_mass_limit];

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
                    if !ptm_condition.check_peptide(&peptide) {
                        continue;
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
pub struct MultiTaskFilter;

impl<'a> Filter<'a> for MultiTaskFilter {
    async fn filter(
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
            let _tasks: Vec<tokio::task::JoinHandle<Result<()>>> = ptm_conditions
                .into_iter()
                .map(|ptm_condition|
                    tokio::task::spawn(
                        Self::filter_with_ptm_conditions(
                            client.clone(),
                            partition_limits.clone(),
                            mass,
                            lower_mass_tolerance_ppm,
                            upper_mass_tolerance_ppm,
                            ptm_condition,
                            filter_pipeline.clone(),
                            peptide_sender.clone(),
                        )
                    )
                ).collect();

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
pub struct MultiThreadSingleClientFilter;

impl<'a> Filter<'a> for MultiThreadSingleClientFilter {
    async fn filter(
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
                            Self::filter_with_ptm_conditions(
                                thread_client,
                                thread_partition_limits,
                                mass,
                                lower_mass_tolerance_ppm,
                                upper_mass_tolerance_ppm,
                                ptm_condition,
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
pub struct MultiThreadMultiClientFilter {}

impl<'a> Filter<'a> for MultiThreadMultiClientFilter {
    async fn filter(
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
                            Self::filter_with_ptm_conditions(
                                thread_client,
                                thread_partition_limits,
                                mass,
                                lower_mass_tolerance_ppm,
                                upper_mass_tolerance_ppm,
                                ptm_condition,
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
pub struct QueuedMultiThreadSingleClientFilter;

impl<'a> Filter<'a> for QueuedMultiThreadSingleClientFilter {
    async fn filter(
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
                                Self::filter_with_ptm_conditions(
                                    thread_client.clone(),
                                    thread_partition_limits.clone(),
                                    mass,
                                    lower_mass_tolerance_ppm,
                                    upper_mass_tolerance_ppm,
                                    ptm_condition,
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
pub struct QueuedMultiThreadMultiClientFilter;

impl<'a> Filter<'a> for QueuedMultiThreadMultiClientFilter {
    async fn filter(
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
                                Self::filter_with_ptm_conditions(
                                    thread_client.clone(),
                                    thread_partition_limits.clone(),
                                    mass,
                                    lower_mass_tolerance_ppm,
                                    upper_mass_tolerance_ppm,
                                    ptm_condition,
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