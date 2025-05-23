// std imports
use std::cmp::{max, min};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use async_stream::try_stream;
use dihardts_cstools::bloom_filter::BloomFilter;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use futures::{pin_mut, Stream, StreamExt};
use scylla::value::CqlValue;
use tokio::sync::mpsc::{unbounded_channel as channel, UnboundedSender as Sender};
use tracing::error;

// local imports
use crate::functions::post_translational_modification::get_ptm_conditions;
use crate::tools::peptide_partitioner::get_mass_partition;
use crate::{
    database::scylla::peptide_table::PeptideTable, entities::peptide::Peptide,
    functions::post_translational_modification::PTMCondition,
};

use super::client::Client;

/// Trait to check conditions on peptides
///
pub trait FilterFunction: Send + Sync {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool>;
}

/// Filters peptides which not are in SwissProt
///
struct IsSwissProtFilterFunction;

impl FilterFunction for IsSwissProtFilterFunction {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool> {
        Ok(peptide.get_is_swiss_prot())
    }
}

/// Filter peptides which are not in TrEMBL
///
struct IsTrEMBLFilterFunction;

impl FilterFunction for IsTrEMBLFilterFunction {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool> {
        Ok(peptide.get_is_trembl())
    }
}

/// Makes sure that no peptide is returned twice
///
pub struct ThreadSafeDistinctFilterFunction {
    bloom_filter: BloomFilter,
}

impl FilterFunction for ThreadSafeDistinctFilterFunction {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool> {
        if self.bloom_filter.contains(peptide.get_sequence())? {
            return Ok(false);
        }
        self.bloom_filter.add(peptide.get_sequence())?;
        Ok(true)
    }
}

/// Filters peptides which are not in the given taxonomy IDs
///
struct TaxonomyFilterFunction {
    taxonomy_ids: Arc<Vec<i64>>,
}

impl FilterFunction for TaxonomyFilterFunction {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool> {
        for taxonomy_id in self.taxonomy_ids.iter() {
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
    proteome_ids: Arc<Vec<String>>,
}

impl FilterFunction for ProteomeFilterFunction {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool> {
        for proteome_id in self.proteome_ids.iter() {
            if peptide.get_proteome_ids().contains(proteome_id) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub type FalliblePeptideStream = Pin<Box<dyn Stream<Item = Result<Peptide>> + Send>>;

/// Maps PTM conditions to partitions
/// key is the partition, value is a vector of tuples with the lower and upper mass limit and the PTM condition
///
pub type PtmConditionMap = HashMap<usize, Vec<(i64, i64, PTMCondition)>>;

#[allow(clippy::too_many_arguments)]
pub trait Search {
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
        ptms: &[PTM],
        num_threads: Option<usize>,
    ) -> impl std::future::Future<Output = Result<FalliblePeptideStream>> + Send;

    fn create_filter_pipeline(
        distinct: bool,
        taxonomy_ids: Option<Arc<Vec<i64>>>,
        proteome_ids: Option<Arc<Vec<String>>>,
        is_reviewed: Option<bool>,
    ) -> Result<Vec<Box<dyn FilterFunction>>> {
        let mut filter_pipeline: Vec<Box<dyn FilterFunction>> = Vec::new();
        if distinct {
            filter_pipeline.push(Box::new(ThreadSafeDistinctFilterFunction {
                bloom_filter: BloomFilter::new_by_size_and_fp_prob(80_000_000, 0.001)?,
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
    /// * `ptm_condition` - The PTM condition to query
    /// * `lower_mass_tolerance_ppm` - The lower mass tolerance in ppm
    /// * `upper_mass_tolerance_ppm` - The upper mass tolerance in ppm
    /// * `filter_pipeline` - The filter pipeline
    /// * `peptide_sender` - The sender to send the peptides to the final stream
    ///
    fn search_with_ptm_conditions(
        task_id: usize,
        client: Arc<Client>,
        partition: usize,
        conditions: Vec<(i64, i64, PTMCondition)>,
        mut filter_pipeline: Vec<Box<dyn FilterFunction>>,
        peptide_sender: Sender<Result<Peptide>>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            let partition = CqlValue::BigInt(partition as i64);
            for (lower_mass_limit, upper_mass_limit, ptm_condition) in conditions.iter() {
                let lower_mass_limit = CqlValue::BigInt(*lower_mass_limit);
                let upper_mass_limit = CqlValue::BigInt(*upper_mass_limit);

                let query_params = vec![&partition, &lower_mass_limit, &upper_mass_limit];

                let peptide_stream = match PeptideTable::select(
                    client.as_ref(),
                    "WHERE partition = ? AND mass >= ? AND mass <= ?",
                    &query_params,
                )
                .await
                {
                    Ok(stream) => stream,
                    Err(err) => {
                        error!("task {}: error creating peptide stream: {}", task_id, err);
                        return Err(err);
                    }
                };
                pin_mut!(peptide_stream);
                'peptide_loop: while let Some(peptide) = peptide_stream.next().await {
                    let peptide = match peptide {
                        Ok(peptide) => peptide,
                        Err(err) => {
                            error!("task {}: error receiving peptide: {}", task_id, err);
                            return Err(err);
                        }
                    };
                    if !ptm_condition.check_peptide(&peptide) {
                        continue;
                    }
                    for filter in filter_pipeline.iter_mut() {
                        if !filter.is_match(&peptide)? {
                            // debug!("peptide: {} filtered", &seq);
                            continue 'peptide_loop;
                        }
                    }
                    match peptide_sender.send(Ok(peptide)) {
                        Ok(_) => {}
                        Err(err) => {
                            error!("task {}: error sending peptide: {}", task_id, err);
                            return Err(err.into());
                        }
                    };
                }
            }
            drop(peptide_sender);
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
        mut filter_pipeline: Vec<Box<dyn FilterFunction>>,
        peptide_sender: Sender<Result<Peptide>>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // Calculate mass range
            let lower_mass_limit = mass - (mass / 1000000 * lower_mass_tolerance_ppm);
            let upper_mass_limit = mass + (mass / 1000000 * upper_mass_tolerance_ppm);

            let peptide_stream = PeptideTable::select_by_mass_range(
                client.as_ref(),
                lower_mass_limit,
                upper_mass_limit,
                partition_limits.as_ref(),
            )
            .await?;
            pin_mut!(peptide_stream);
            'peptide_loop: while let Some(peptide) = peptide_stream.next().await {
                let peptide = peptide?;
                for filter in filter_pipeline.iter_mut() {
                    if !filter.is_match(&peptide)? {
                        continue 'peptide_loop;
                    }
                }
                peptide_sender.send(Ok(peptide))?;
            }

            Ok(())
        }
    }

    /// Splitup and sort PTM condition by partition
    ///
    /// # Arguments
    /// * ptm_conditions - The PTM conditions to split and sort
    /// * partition_limits - The partition limits
    ///
    fn split_and_sort_ptm_conditions(
        ptm_conditions: Vec<PTMCondition>,
        partition_limits: &[i64],
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
    ) -> Result<PtmConditionMap> {
        let mut sorted_ptm_conditions: PtmConditionMap = HashMap::new();
        for ptm_condition in ptm_conditions {
            // Calculate mass range based on ptm condition
            let lower_mass_limit = ptm_condition.get_mass()
                - (ptm_condition.get_mass() / 1000000 * lower_mass_tolerance_ppm);
            let upper_mass_limit = ptm_condition.get_mass()
                + (ptm_condition.get_mass() / 1000000 * upper_mass_tolerance_ppm);

            // Get partition
            let lower_partition_index = get_mass_partition(partition_limits, lower_mass_limit)?;
            let upper_partition_index = get_mass_partition(partition_limits, upper_mass_limit)?;

            if lower_partition_index == upper_partition_index {
                sorted_ptm_conditions
                    .entry(lower_partition_index)
                    .or_default()
                    .push((lower_mass_limit, upper_mass_limit, ptm_condition));
            } else {
                #[allow(clippy::needless_range_loop)]
                for partition in lower_partition_index..=upper_partition_index {
                    sorted_ptm_conditions.entry(partition).or_default().push((
                        max(partition_limits[partition], lower_mass_limit),
                        min(partition_limits[partition], upper_mass_limit),
                        ptm_condition.clone(),
                    ));
                }
            }
        }
        Ok(sorted_ptm_conditions)
    }
}

/// Asynchronous filter where one task is spawned for each PTM condition.
///
pub struct MultiTaskSearch;

impl Search for MultiTaskSearch {
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
        ptms: &[PTM],
        _num_threads: Option<usize>,
    ) -> Result<FalliblePeptideStream> {
        let taxonomy_ids = taxonomy_ids.map(Arc::new);
        let proteome_ids = proteome_ids.map(Arc::new);

        let sorted_ptm_conditions = Self::split_and_sort_ptm_conditions(
            get_ptm_conditions(mass, max_variable_modifications, ptms)?,
            partition_limits.as_ref(),
            lower_mass_tolerance_ppm,
            upper_mass_tolerance_ppm,
        )?;

        let (peptide_sender, mut peptide_receiver) = channel::<Result<Peptide>>();

        Ok(Box::pin(try_stream! {
            let mut tasks: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(max(sorted_ptm_conditions.len(), 1));
            if !sorted_ptm_conditions.is_empty() {
                for (task_id, (partition, conditions)) in sorted_ptm_conditions.into_iter().enumerate() {
                    let filter_pipeline = Self::create_filter_pipeline(
                        distinct,
                        taxonomy_ids.clone(),
                        proteome_ids.clone(),
                        is_reviewed,
                    )?;

                    // Spawn on task for each PTM condition
                    tasks.push(tokio::task::spawn(
                        Self::search_with_ptm_conditions(
                            task_id,
                            client.clone(),
                            partition,
                            conditions,
                            filter_pipeline,
                            peptide_sender.clone(),
                        )
                    ));
                }
            } else {
                // Spawn one task for the mass range as there are no PTMs
                tasks.push(tokio::task::spawn(
                    Self::search_without_ptm_condition(
                        client.clone(),
                        partition_limits.clone(),
                        mass,
                        lower_mass_tolerance_ppm,
                        upper_mass_tolerance_ppm,
                        Self::create_filter_pipeline(
                            distinct,
                            taxonomy_ids,
                            proteome_ids,
                            is_reviewed,
                        )?,
                        peptide_sender.clone(),
                    )
                ));
            }

            drop(peptide_sender);

            while let Some(peptide) = peptide_receiver.recv().await {
                yield peptide?;
            }

            for task in tasks {
                task.await??;
            }
        }))
    }
}

// See commit 9926c71adaf7fda760f4dae3be611c18e5cfc233 for other implementations of the Search trait
