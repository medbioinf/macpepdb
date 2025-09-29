use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_stream::try_stream;
use dihardts_cstools::bloom_filter::BloomFilter;
use dihardts_omicstools::chemistry::amino_acid::{AminoAcid, CANONICAL_AMINO_ACIDS};
use futures::{pin_mut, Stream, StreamExt};
use itertools::Itertools;
use scylla::value::CqlValue;
use tokio::sync::mpsc::{unbounded_channel as channel, UnboundedSender as Sender};
use tracing::error;

use crate::chemistry::amino_acid::INTERNAL_GLYCINE;
use crate::entities::configuration::Configuration;
use crate::entities::peptide::MatchingPeptide;
use crate::functions::post_translational_modification::{
    PTMCollection, PostTranslationalModification,
};
use crate::mass::convert::{to_float as mass_to_float, to_int as mass_to_int};
use crate::tools::peptide_partitioner::get_mass_partition;
use crate::{database::scylla::peptide_table::PeptideTable, entities::peptide::Peptide};

use super::client::Client;

/// Trait to check conditions on peptides
///
pub trait FilterFunction: Send + Sync + Display {
    /// Returns true if the peptide matches the condition, false otherwise.
    ///
    /// # Arguments
    /// * `peptide` - The peptide to check
    ///
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

impl Display for IsSwissProtFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "is SwissProt")
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

impl Display for IsTrEMBLFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "is TrEMBL")
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

impl Display for ThreadSafeDistinctFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "distinct")
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

impl Display for TaxonomyFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "taxonomy in [{}]", self.taxonomy_ids.iter().join(", "))
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

impl Display for ProteomeFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "proteome in [{}]", self.proteome_ids.iter().join(", "))
    }
}

/// Filters peptides which start with a specific amino acid
///
struct StartsWithFilterFunction {
    amino_acid: char,
}

impl FilterFunction for StartsWithFilterFunction {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool> {
        Ok(peptide.get_sequence().starts_with(self.amino_acid))
    }
}

impl Display for StartsWithFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "starts with '{}'", self.amino_acid)
    }
}

/// Filters peptides which end with a specific amino acid
///
struct EndsWithFilterFunction {
    /// One letter code of the amino acid
    amino_acid: char,
}

impl FilterFunction for EndsWithFilterFunction {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool> {
        Ok(peptide.get_sequence().ends_with(self.amino_acid))
    }
}

impl Display for EndsWithFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ends with '{}'", self.amino_acid)
    }
}

/// Filters peptides contains an specific amount occurrences of an amino acid
///
struct EqualsNumberOfOccurrencesFilterFunction {
    /// One letter code of the amino acid
    amino_acid: char,
    amount: i16,
}

impl FilterFunction for EqualsNumberOfOccurrencesFilterFunction {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool> {
        let count = peptide.get_aa_count(self.amino_acid);
        Ok(count == self.amount)
    }
}

impl Display for EqualsNumberOfOccurrencesFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "occurences of '{}' == {}", self.amino_acid, self.amount,)
    }
}

/// Filters peptides contains an specific amount occurrences of an amino acid
///
struct GreaterOrEqualsNumberOfOccurrencesFilterFunction {
    /// One letter code of the amino acid
    amino_acid: char,
    amount: i16,
}

impl FilterFunction for GreaterOrEqualsNumberOfOccurrencesFilterFunction {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool> {
        let count = peptide.get_aa_count(self.amino_acid);
        Ok(count >= self.amount)
    }
}

impl Display for GreaterOrEqualsNumberOfOccurrencesFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "occurences of '{}' >= {}", self.amino_acid, self.amount,)
    }
}

/// Filters peptides contains an specific amount occurrences of an amino acid
///
struct NoOccurrencesFilterFunction {
    /// One letter code of the amino acid
    amino_acid: char,
}

impl FilterFunction for NoOccurrencesFilterFunction {
    fn is_match(&mut self, peptide: &Peptide) -> Result<bool> {
        let count = peptide.get_aa_count(self.amino_acid);
        Ok(count == 0)
    }
}

impl Display for NoOccurrencesFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "occurences of '{}' == 0", self.amino_acid,)
    }
}

pub type FalliblePeptideStream = Pin<Box<dyn Stream<Item = Result<MatchingPeptide>> + Send>>;

/// Maps peptide conditions to partitions
/// key is the partition, value is a vector of tuples with the lower and upper mass limit and the PTM condition
///
pub type FinalizedPeptideConditionMap<'a> =
    HashMap<usize, Vec<(i64, i64, FinalizedPeptideCondition)>>;

/// Defines the search for peptides in the database and provides some helper functions
///
#[allow(clippy::too_many_arguments)]
pub trait Search {
    /// Search for peptides in the database based on the given parameters.
    ///
    /// # Arguments
    /// * `client` - The client to use for the query
    /// * `configuration` - The configuration to use for the query
    /// * `mass` - The mass to search for
    /// * `lower_mass_tolerance_ppm` - The lower mass tolerance in ppm
    /// * `upper_mass_tolerance_ppm` - The upper mass tolerance in ppm
    /// * `max_variable_modifications` - The maximum number of variable modifications to apply
    /// * `distinct` - Whether to return distinct peptides only
    /// * `taxonomy_ids` - The taxonomy IDs to filter the peptides by
    /// * `proteome_ids` - The proteome IDs to filter the peptides by
    /// * `is_reviewed` - Whether to filter the peptides by SwissProt or TrEMBL
    /// * `ptm_collection` - The PTM collection to use for the query
    /// * `resolve_modifications` - Wether to resolve modifications and return the modified sequences as ProForma compliant strings
    /// * `num_threads` - The number of concurrent searches
    ///
    fn search(
        client: Arc<Client>,
        configuration: Arc<Configuration>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        distinct: bool,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptm_collection: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
        resolve_modifications: bool,
        num_threads: Option<usize>,
    ) -> impl std::future::Future<Output = Result<FalliblePeptideStream>> + Send;

    /// Creates a vecotor of filter functions based on the given parameters.
    ///
    /// # Arguments
    /// * `distinct` - Whether to return distinct peptides only
    /// * `taxonomy_ids` - The taxonomy IDs to filter the peptides by
    /// * `proteome_ids` - The proteome IDs to filter the peptides by
    /// * `is_reviewed` - Whether to filter the peptides by SwissProt or TrEMBL
    ///
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

    /// Query condition (e.g. PTMs) which needs more work prior to the actual query than just querying the mass.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task, used for logging
    /// * `client` - The client to use for the query
    /// * `partition` - The partition to query
    /// * `conditions` - The conditions pepitdes need to fullfill e.g PTMs
    /// * `filter_pipeline` - Global filters to apply (e.g. distinct, taxonomy, proteome, is_reviewed)
    /// * `resolve_modifications` - Whether to resolve modifications and return the modified sequences as ProForma compliant strings
    /// * `peptide_sender` - The sender to send the peptides to the final stream
    ///
    fn search_with_ptm_conditions(
        task_id: usize,
        client: Arc<Client>,
        partition: usize,
        mut conditions: Vec<(i64, i64, FinalizedPeptideCondition)>,
        mut filter_pipeline: Vec<Box<dyn FilterFunction>>,
        resolve_modifications: bool,
        peptide_sender: Sender<Result<MatchingPeptide>>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            let partition = CqlValue::BigInt(partition as i64);
            for (lower_mass_limit, upper_mass_limit, ptm_condition) in conditions.iter_mut() {
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

                    let additional_sequences = if resolve_modifications {
                        ptm_condition.modify_sequence(peptide.get_sequence())
                    } else {
                        Vec::new()
                    };

                    match peptide_sender
                        .send(Ok(MatchingPeptide::new(peptide, additional_sequences)))
                    {
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

    /// Query without and conditions
    ///
    /// # Arguments
    /// * `client` - The client to use for the query
    /// * `configuration` - Configuration from the database
    /// * `mass` - The mass to query
    /// * `lower_mass_tolerance_ppm` - The lower mass tolerance in ppm
    /// * `upper_mass_tolerance_ppm` - The upper mass tolerance in ppm
    /// * `filter_pipeline` - Global filters to apply (e.g. distinct, taxonomy, proteome, is_reviewed)
    /// * `peptide_sender` - The sender to send the peptides to the final stream
    ///
    fn search_without_ptm_condition(
        client: Arc<Client>,
        configuration: Arc<Configuration>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        mut filter_pipeline: Vec<Box<dyn FilterFunction>>,
        peptide_sender: Sender<Result<MatchingPeptide>>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            // Calculate mass range
            let lower_mass_limit = mass - (mass / 1_000_000 * lower_mass_tolerance_ppm);
            let upper_mass_limit = mass + (mass / 1_000_000 * upper_mass_tolerance_ppm);

            let peptide_stream = PeptideTable::select_by_mass_range(
                client.as_ref(),
                lower_mass_limit,
                upper_mass_limit,
                configuration.get_partition_limits(),
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
                peptide_sender.send(Ok(MatchingPeptide::new(peptide, Vec::new())))?;
            }

            Ok(())
        }
    }

    /// Splitup and sort peptide condition by partition and finalize them.
    ///
    /// # Arguments
    /// * peptide_conditions - The conditions pepitdes need to fullfill e.g PTMs
    /// * partition_limits - The partition limits from configuration
    /// * lower_mass_tolerance_ppm - The lower mass tolerance in ppm
    /// * upper_mass_tolerance_ppm - The upper mass tolerance in ppm
    ///
    fn split_and_sort_peptide_conditions<'a>(
        peptide_conditions: Vec<PeptideCondition>,
        partition_limits: &[i64],
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
    ) -> Result<FinalizedPeptideConditionMap<'a>> {
        let mut sorted_peptide_conditions: FinalizedPeptideConditionMap<'a> = HashMap::new();
        for peptide_condition in peptide_conditions {
            // Calculate mass range based on ptm condition
            let lower_mass_limit = peptide_condition.query_mass
                - (peptide_condition.query_mass / 1_000_000 * lower_mass_tolerance_ppm);
            let upper_mass_limit = peptide_condition.query_mass
                + (peptide_condition.query_mass / 1_000_000 * upper_mass_tolerance_ppm);

            // Get partition
            let lower_partition_index = get_mass_partition(partition_limits, lower_mass_limit)?;
            let upper_partition_index = get_mass_partition(partition_limits, upper_mass_limit)?;

            if lower_partition_index == upper_partition_index {
                sorted_peptide_conditions
                    .entry(lower_partition_index)
                    .or_default()
                    .push((lower_mass_limit, upper_mass_limit, peptide_condition.into()));
            } else {
                #[allow(clippy::needless_range_loop)]
                for partition in lower_partition_index..=upper_partition_index {
                    sorted_peptide_conditions
                        .entry(partition)
                        .or_default()
                        .push((
                            lower_mass_limit,
                            upper_mass_limit,
                            peptide_condition.clone().into(),
                        ));
                }
            }
        }
        Ok(sorted_peptide_conditions)
    }
}

/// Asynchronous filter where one task is spawned for each PTM condition.
///
pub struct MultiTaskSearch;

impl Search for MultiTaskSearch {
    async fn search(
        client: Arc<Client>,
        configuration: Arc<Configuration>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        distinct: bool,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptm_collection: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
        resolve_modifications: bool,
        _num_threads: Option<usize>,
    ) -> Result<FalliblePeptideStream> {
        let taxonomy_ids = taxonomy_ids.map(Arc::new);
        let proteome_ids = proteome_ids.map(Arc::new);

        let min_mass = match configuration.get_min_peptide_length() {
            Some(min_length) => INTERNAL_GLYCINE.get_mono_mass_int() * min_length as i64,
            None => 0,
        };

        // Calulcate max mass as stated in PeptideCondition::from_ptm_collection() 2.3
        let largest_negative_static_ptm = ptm_collection
            .get_static_ptms()
            .iter()
            .filter(|ptm| ptm.get_mass_delta().is_sign_negative())
            .fold(0_i64, |acc, ptm| {
                acc.min(mass_to_int(*ptm.get_mass_delta()))
            })
            .abs();

        let largest_negative_variable_ptm = ptm_collection
            .get_variable_ptms()
            .iter()
            .filter(|ptm| ptm.get_mass_delta().is_sign_negative())
            .fold(0_i64, |acc, ptm| {
                acc.min(mass_to_int(*ptm.get_mass_delta()))
            })
            .abs();

        // Possible peptide length plus 30% "play" to account for errors
        let amino_acid_average = mass_to_int(
            CANONICAL_AMINO_ACIDS
                .iter()
                .map(|aa| aa.get_mono_mass())
                .sum::<f64>()
                / CANONICAL_AMINO_ACIDS.len() as f64,
        );
        let possible_peptide_length = ((mass / amino_acid_average) as f64 * 1.3) as i64;

        let max_mass = mass
            + (largest_negative_static_ptm * possible_peptide_length)
            + (largest_negative_variable_ptm * possible_peptide_length);

        let sorted_ptm_conditions = Self::split_and_sort_peptide_conditions(
            PeptideCondition::from_ptm_collection(
                &ptm_collection,
                mass,
                min_mass,
                max_mass,
                max_variable_modifications,
            ),
            configuration.get_partition_limits(),
            lower_mass_tolerance_ppm,
            upper_mass_tolerance_ppm,
        )?;

        let (peptide_sender, mut peptide_receiver) = channel::<Result<MatchingPeptide>>();

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
                            resolve_modifications,
                            peptide_sender.clone(),
                        )
                    ));
                }
            } else {
                // Spawn one task for the mass range as there are no PTMs
                tasks.push(tokio::task::spawn(
                    Self::search_without_ptm_condition(
                        client.clone(),
                        configuration.clone(),
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

/// Peptide condition which are not querieable and need to be checked "on the fly/demand"
///
#[derive(Clone)]
pub struct PeptideCondition {
    /// Mass to query
    query_mass: i64,
    /// Considered static PTMs
    static_ptms: Vec<Arc<PostTranslationalModification>>,
    /// Considered variable PTMs
    variable_ptms: Vec<Arc<PostTranslationalModification>>,
    /// N-terminal PTM
    n_terminal_ptm: Option<Arc<PostTranslationalModification>>,
    /// C-terminal PTM
    c_terminal_ptm: Option<Arc<PostTranslationalModification>>,
    /// N-terminal bond PTM
    n_bond_ptm: Option<Arc<PostTranslationalModification>>,
    /// C-terminal bond PTM
    c_bond_ptm: Option<Arc<PostTranslationalModification>>,
    /// Excluded amino acids
    excluded_amino_acids: HashSet<char>,
}

impl PeptideCondition {
    /// Creates a new PeptideCondition with no PTMs.
    ///
    /// # Arguments
    /// * `targeted_mass` - Mass of peptides to search for
    /// * `minimum_mass` - Minimum mass of peptides in the datavase. Usually 6 times Glycine
    /// * `max_variable_modifications` - Max. variable modification to apply simultaniously
    ///
    pub fn new(targeted_mass: i64) -> Self {
        Self {
            query_mass: targeted_mass,
            static_ptms: Vec::new(),
            variable_ptms: Vec::new(),
            n_terminal_ptm: None,
            c_terminal_ptm: None,
            n_bond_ptm: None,
            c_bond_ptm: None,
            excluded_amino_acids: HashSet::new(),
        }
    }

    /// Adds a static PTM to the PeptideCondition.
    ///
    pub fn add_static_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = mass_to_int(*ptm.get_mass_delta());
        if mass_delta_int > self.query_mass {
            return false;
        }

        self.static_ptms.push(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    pub fn add_variable_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = mass_to_int(*ptm.get_mass_delta());
        if mass_delta_int > self.query_mass {
            return false;
        }

        self.variable_ptms.push(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    pub fn set_n_terminal_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = mass_to_int(*ptm.get_mass_delta());
        if self.n_terminal_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.n_terminal_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    pub fn set_c_terminal_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = mass_to_int(*ptm.get_mass_delta());
        if self.c_terminal_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.c_terminal_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    pub fn set_n_bond_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = mass_to_int(*ptm.get_mass_delta());
        if self.n_bond_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.n_bond_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    pub fn set_c_bond_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = mass_to_int(*ptm.get_mass_delta());
        // if ptm is positive but larger than the remaining mass or  smaller than the minimum mass, skip it
        // a negative delta would increase the remaining mass, so we do not check for it
        if self.c_bond_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.c_bond_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    pub fn add_excluded_amino_acid(&mut self, amino_acid: &dyn AminoAcid) {
        self.excluded_amino_acids
            .insert(*amino_acid.get_one_letter_code());
    }

    /// Applies the condition to the given amino acid sequence and returns every possible modified version of it
    /// in ProForma format.
    ///
    /// # Arguments
    /// * `sequence` - The amino acid sequence to apply the condition tos
    ///
    pub fn modify_sequence(&self, sequence: &str) -> Vec<String> {
        // Map for fast access to variable modifications by amino acid
        let mut variable_modifications_map: HashMap<char, Vec<&PostTranslationalModification>> =
            HashMap::new();
        for ptm in self.variable_ptms.iter() {
            variable_modifications_map
                .entry(*ptm.get_amino_acid().get_one_letter_code())
                .and_modify(|mods| mods.push(ptm))
                .or_insert(vec![ptm]);
        }

        // Results vector to store the modified sequences
        let mut proforma_sequences: HashSet<String> = HashSet::new();

        // Prepare static modifications in ProForma format
        let static_mods = self
            .static_ptms
            .iter()
            .map(|ptm| {
                format!(
                    "[{:+}]@{}",
                    ptm.get_mass_delta(),
                    ptm.get_amino_acid().get_one_letter_code(),
                )
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .join(",");

        let mut modded_peptide = String::with_capacity(sequence.len());

        if !static_mods.is_empty() {
            modded_peptide = format!("<{static_mods}>",);
        }

        // Add n-bonf if present
        if let Some(n_bond_ptm) = &self.n_bond_ptm {
            modded_peptide.push_str(&format!("[{}]-", n_bond_ptm.get_mass_delta()));
        }

        self.inner_modify_sequence(
            sequence,
            modded_peptide.clone(),
            &variable_modifications_map,
            0,
            0,
            &mut proforma_sequences,
        );

        // return results
        proforma_sequences.into_iter().collect::<Vec<_>>()
    }

    /// Modifies the peptide sequence recursively by adding variable modifications at each necessary position.
    /// Make sure the given peptide was checked against the condition before calling this function.
    ///
    /// # Arguments
    /// * `peptide` - The original peptide sequence to modify
    /// * `modified_peptide` - The current modified peptide sequence
    /// * `variable_modifications_map` - A map of amino acids to their possible variable modifications
    /// * `position` - The current position in the peptide sequence to modify
    /// * `applied_vmods` - The number of variable modifications applied so far
    /// * `max_variable_modifications` - The maximum number of variable modifications allowed
    /// * `proforma_sequences` - A mutable vector to store the resulting proforma sequences
    ///
    #[allow(clippy::too_many_arguments)]
    fn inner_modify_sequence(
        &self,
        peptide: &str,
        mut modified_peptide: String,
        variable_modifications_map: &HashMap<char, Vec<&PostTranslationalModification>>,
        position: usize,
        applied_vmods: usize,
        proforma_sequences: &mut HashSet<String>,
    ) {
        if position >= peptide.len() {
            self.end_modify_sequence(modified_peptide, applied_vmods, proforma_sequences);
            return;
        }

        modified_peptide.push(peptide.chars().nth(position).unwrap());

        // First check for n-terminal and c-terminal modifications which must be applied when present.
        if position == 0 && self.n_terminal_ptm.is_some() {
            modified_peptide.push_str(&format!(
                "[{:+}]",
                self.n_terminal_ptm.as_ref().unwrap().get_mass_delta()
            ));
            self.inner_modify_sequence(
                peptide,
                modified_peptide,
                variable_modifications_map,
                position + 1,
                applied_vmods,
                proforma_sequences,
            );
        } else if position == peptide.len() - 1 && self.c_terminal_ptm.is_some() {
            modified_peptide.push_str(&format!(
                "[{:+}]",
                self.c_terminal_ptm.as_ref().unwrap().get_mass_delta()
            ));
            self.inner_modify_sequence(
                peptide,
                modified_peptide,
                variable_modifications_map,
                position + 1,
                applied_vmods,
                proforma_sequences,
            );
        } else {
            // # Next with unmodified amino acid
            self.inner_modify_sequence(
                peptide,
                modified_peptide.clone(),
                variable_modifications_map,
                position + 1,
                applied_vmods,
                proforma_sequences,
            );

            if applied_vmods < self.variable_ptms.len() {
                // # Next with modified amino acid
                if let Some(modifications) =
                    variable_modifications_map.get(&peptide.chars().nth(position).unwrap())
                {
                    for modification in modifications.iter() {
                        let next_modified_peptide =
                            format!("{}[{:+}]", &modified_peptide, modification.get_mass_delta());
                        self.inner_modify_sequence(
                            peptide,
                            next_modified_peptide,
                            variable_modifications_map,
                            position + 1,
                            applied_vmods + 1,
                            proforma_sequences,
                        );
                    }
                }
            }
        }
    }

    /// Modifies the peptide sequence at the end by adding c-terminal to the proforma sequences.
    ///
    /// # Arguments
    /// * `modified_peptide` - The modified peptide sequence to add
    /// * `applied_vmods` - The number of variable modifications applied to the peptide
    /// * `proforma_sequences` - The vector of proforma sequences to add the modified peptide to
    ///
    fn end_modify_sequence(
        &self,
        mut modified_peptide: String,
        applied_vmods: usize,
        proforma_sequences: &mut HashSet<String>,
    ) {
        if let Some(c_bond_ptm) = &self.c_bond_ptm {
            modified_peptide.push_str(&format!("-[{}]", c_bond_ptm.get_mass_delta(),));
        }
        // If the number of applied variable modifications not equals the number of variable PTMs,
        // this condition is not fully applied
        if applied_vmods == self.variable_ptms.len() {
            proforma_sequences.insert(modified_peptide);
        }
    }

    /// Creates a vector of PeptideConditions from a PTMCollection.
    ///
    /// # Arguments
    /// * `ptm_collection` - The PTMCollection to use
    /// * `targeted_mass` - Mass of the unmodfied peptide to search for
    /// * `min_mass` - Minimum mass of the peptides in the database, usually mass of Glycin times the minimum configured peptide length
    /// * `max_mass` - Maximum mass of the peptides in the database. This value need to be chosen with care if modifications with negative mass delta are used.
    ///   Otherwise conditions with masses will be generated, way outside the database range, generating useless operations.
    ///     1. If no modification with negative mass delta is used, `max_mass` equals the targeted mass.
    ///     2. If static modifications with negative mass delta are used, multipe options are viable:
    ///         1. Set it to the mass of Tryptophan times the configured maximum length of the peptides.
    ///         2. A more conservative approach is to set it equals the targeted mass plus
    ///            the absolute value of the largest negative mass delta of the static modifications times the configured maximum length of the peptides.
    ///         3. Instead of using the configured max length, divide the target mass by the average mass of an amino acid to get the likely length of the peptide and add
    ///            a certain amount of play (e.g. 30%) to the calculated length. Multiply this value with the absolute value of the largest negative mass delta of the static modifications.
    ///   3. If variable modifications with negative mass delta are used, `max_mass` should equals the targeted mass plus
    ///      the absolute value of the largest negative mass delta of the variable modifications times the allowd number of variable modifications.
    ///   4. If both static and variable modifications with negative mass delta are used, case 2 and 3 should be combined.
    ///  
    /// * `max_variable_modifications` - The maximum number of variable modifications to apply
    pub fn from_ptm_collection(
        ptm_collection: &PTMCollection<Arc<PostTranslationalModification>>,
        targeted_mass: i64,
        min_mass: i64,
        max_mass: i64,
        max_variable_modifications: usize,
    ) -> Vec<PeptideCondition> {
        if ptm_collection.is_empty() {
            return Vec::new();
        }

        let mut resulting_conditions: Vec<PeptideCondition> = Vec::new();

        // Handle no modifications (which excludes all static modifications)
        let mut condition = PeptideCondition::new(targeted_mass);
        for static_ptm in ptm_collection.get_static_ptms() {
            condition.add_excluded_amino_acid(static_ptm.get_amino_acid());
        }
        resulting_conditions.push(condition);

        // static modifications
        let condition = PeptideCondition::new(targeted_mass);
        Self::calculate_peptide_conditions_for_static_modifications(
            ptm_collection,
            min_mass,
            max_mass,
            condition.clone(),
            0,
            &mut resulting_conditions,
        );

        // variable modifications
        let current_len = resulting_conditions.len();
        for i in 0..current_len {
            let condition = resulting_conditions[i].clone();
            Self::calculate_peptide_conditions_for_variable_modifications(
                ptm_collection,
                min_mass,
                max_mass,
                max_variable_modifications,
                condition,
                0,
                &mut resulting_conditions,
            )
        }

        // n terminal modifications
        let current_len = resulting_conditions.len();
        for i in 0..current_len {
            let mut condition = resulting_conditions[i].clone();
            for modification in ptm_collection.get_n_terminal_ptms() {
                if condition.set_n_terminal_ptm(modification.clone()) {
                    resulting_conditions.push(condition.clone());
                }
            }
        }

        // c terminal modifications
        let current_len = resulting_conditions.len();
        for i in 0..current_len {
            let mut condition = resulting_conditions[i].clone();
            for modification in ptm_collection.get_c_terminal_ptms() {
                if condition.set_c_terminal_ptm(modification.clone()) {
                    resulting_conditions.push(condition.clone());
                }
            }
        }

        // n bond modifications
        let current_len = resulting_conditions.len();
        for i in 0..current_len {
            let mut condition = resulting_conditions[i].clone();
            for modification in ptm_collection.get_n_bond_ptms() {
                if condition.set_n_bond_ptm(modification.clone()) {
                    resulting_conditions.push(condition.clone());
                }
            }
        }

        // c bond modifications
        let current_len = resulting_conditions.len();
        for i in 0..current_len {
            let mut condition = resulting_conditions[i].clone();
            for modification in ptm_collection.get_c_bond_ptms() {
                if condition.set_c_bond_ptm(modification.clone()) {
                    resulting_conditions.push(condition.clone());
                }
            }
        }

        resulting_conditions
    }

    fn calculate_peptide_conditions_for_static_modifications(
        ptm_collection: &PTMCollection<Arc<PostTranslationalModification>>,
        min_mass: i64,
        max_mass: i64,
        mut condition: PeptideCondition,
        modification_position: usize,
        resulting_conditions: &mut Vec<PeptideCondition>,
    ) {
        if modification_position >= ptm_collection.get_static_ptms().len() {
            return;
        }

        // # Without this variable modifications apply the next one
        Self::calculate_peptide_conditions_for_static_modifications(
            ptm_collection,
            min_mass,
            max_mass,
            condition.clone(),
            modification_position + 1,
            resulting_conditions,
        );

        while condition
            .add_static_ptm(ptm_collection.get_static_ptms()[modification_position].clone())
        {
            if condition.query_mass < min_mass || condition.query_mass > max_mass {
                break;
            }
            resulting_conditions.push(condition.clone());
            // Apply next static modification
            Self::calculate_peptide_conditions_for_static_modifications(
                ptm_collection,
                min_mass,
                max_mass,
                condition.clone(),
                modification_position + 1,
                resulting_conditions,
            );
        }
    }

    fn calculate_peptide_conditions_for_variable_modifications(
        ptm_collection: &PTMCollection<Arc<PostTranslationalModification>>,
        min_mass: i64,
        max_mass: i64,
        max_variable_modifications: usize,
        mut condition: PeptideCondition,
        modification_position: usize,
        resulting_conditions: &mut Vec<PeptideCondition>,
    ) {
        if modification_position >= ptm_collection.get_variable_ptms().len() {
            return;
        }

        // # Without this variable modifications apply the next one
        Self::calculate_peptide_conditions_for_variable_modifications(
            ptm_collection,
            min_mass,
            max_mass,
            max_variable_modifications,
            condition.clone(),
            modification_position + 1,
            resulting_conditions,
        );

        // # Apply this modification until we run out of mass
        while condition
            .add_variable_ptm(ptm_collection.get_variable_ptms()[modification_position].clone())
        {
            if condition.variable_ptms.len() > max_variable_modifications
                || condition.query_mass < min_mass
                || condition.query_mass > max_mass
            {
                break;
            }
            resulting_conditions.push(condition.clone());
            // Apply next static modification
            Self::calculate_peptide_conditions_for_variable_modifications(
                ptm_collection,
                min_mass,
                max_mass,
                max_variable_modifications,
                condition.clone(),
                modification_position + 1,
                resulting_conditions,
            );
        }
    }
}

impl Display for PeptideCondition {
    /// Formats the PeptideCondition for display.
    /// if finalized, it will display the filter functions and the query mass.
    /// Otherwise, it will display the PTMs in pseudo ProForma format.
    ///
    /// # Arguments
    /// * `f` - The formatter to write to
    ///
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let static_mods = self
            .static_ptms
            .iter()
            .map(|ptm| {
                format!(
                    "[{}]@{}",
                    ptm.get_mass_delta(),
                    ptm.get_amino_acid().get_one_letter_code()
                )
            })
            .join(", ");
        let variable_mods = self
            .variable_ptms
            .iter()
            .map(|ptm| {
                format!(
                    "v[{}]@{}",
                    ptm.get_mass_delta(),
                    ptm.get_amino_acid().get_one_letter_code()
                )
            })
            .join(", ");
        let n_bind_mod = match &self.n_bond_ptm {
            Some(ptm) => format!("[{}]-", ptm.get_mass_delta()),
            None => String::new(),
        };
        let c_bind_mod = match &self.c_bond_ptm {
            Some(ptm) => format!("-[{}]", ptm.get_mass_delta()),
            None => String::new(),
        };
        let n_terminal_mod = match &self.n_bond_ptm {
            Some(ptm) => format!(
                "cterm{}@{}",
                ptm.get_mass_delta(),
                ptm.get_amino_acid().get_one_letter_code()
            ),
            None => String::new(),
        };
        let c_terminal_mod = match &self.c_bond_ptm {
            Some(ptm) => format!(
                "nterm{}@{}",
                ptm.get_mass_delta(),
                ptm.get_amino_acid().get_one_letter_code()
            ),
            None => String::new(),
        };

        write!(
                f,
                "PeptideCondition: '<{static_mods}>{n_bind_mod}{n_terminal_mod}{variable_mods}{c_terminal_mod}{c_bind_mod}' @ {} Da",
                mass_to_float(self.query_mass),
            )
    }
}

pub struct FinalizedPeptideCondition {
    inner_peptide_condition: PeptideCondition,
    /// Filter functions the peptide has to pass before it is returned
    filter_functions: Vec<Box<dyn FilterFunction>>,
}

impl FinalizedPeptideCondition {
    /// Finalizes the PeptideCondition by calculating the filter functions based on the given modifications.
    ///
    /// # Arguments
    /// * `peptide_condition` - The PeptideCondition to finalize
    ///
    fn get_filter_functions(peptide_condition: &PeptideCondition) -> Vec<Box<dyn FilterFunction>> {
        let mut filter_functions: Vec<Box<dyn FilterFunction>> = Vec::with_capacity(
            peptide_condition.static_ptms.len()
                + peptide_condition.variable_ptms.len()
                + peptide_condition.excluded_amino_acids.len()
                + 2, // N-terminal and C-terminal PTM
        );

        for excluded_aa in peptide_condition.excluded_amino_acids.iter() {
            filter_functions.push(Box::new(NoOccurrencesFilterFunction {
                amino_acid: *excluded_aa,
            }));
        }

        let mut statically_modified_amino_acid_counts: HashMap<char, i16> = HashMap::new();
        for ptm in peptide_condition.static_ptms.iter() {
            statically_modified_amino_acid_counts
                .entry(*ptm.get_amino_acid().get_one_letter_code())
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }

        for (amino_acid, amount) in statically_modified_amino_acid_counts
            .into_iter()
            .sorted_by(|x, y| x.0.cmp(&y.0))
        {
            filter_functions.push(Box::new(EqualsNumberOfOccurrencesFilterFunction {
                amino_acid,
                amount,
            }));
        }

        let mut variable_modified_amino_acid_counts: HashMap<char, i16> = HashMap::new();
        for ptm in peptide_condition.variable_ptms.iter() {
            variable_modified_amino_acid_counts
                .entry(*ptm.get_amino_acid().get_one_letter_code())
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }

        if let Some(ptm) = &peptide_condition.n_terminal_ptm {
            // N-terminal PTM is treated as variable modification
            variable_modified_amino_acid_counts
                .entry(*ptm.get_amino_acid().get_one_letter_code())
                .and_modify(|count| *count += 1)
                .or_insert(1);

            filter_functions.push(Box::new(StartsWithFilterFunction {
                amino_acid: *ptm.get_amino_acid().get_one_letter_code(),
            }));
        }

        if let Some(ptm) = &peptide_condition.c_terminal_ptm {
            // N-terminal PTM is treated as variable modification
            variable_modified_amino_acid_counts
                .entry(*ptm.get_amino_acid().get_one_letter_code())
                .and_modify(|count| *count += 1)
                .or_insert(1);

            filter_functions.push(Box::new(EndsWithFilterFunction {
                amino_acid: *ptm.get_amino_acid().get_one_letter_code(),
            }));
        }

        for (amino_acid, amount) in variable_modified_amino_acid_counts
            .into_iter()
            .sorted_by(|x, y| x.0.cmp(&y.0))
        {
            filter_functions.push(Box::new(GreaterOrEqualsNumberOfOccurrencesFilterFunction {
                amino_acid,
                amount,
            }));
        }

        filter_functions
    }

    pub fn check_peptide(&mut self, peptide: &Peptide) -> bool {
        // Check if the peptide passes all filter functions
        for filter in self.filter_functions.iter_mut() {
            if !filter.is_match(peptide).unwrap_or(false) {
                return false;
            }
        }

        true
    }
}

// Make the inner peptide condition readable
impl Deref for FinalizedPeptideCondition {
    type Target = PeptideCondition;

    fn deref(&self) -> &Self::Target {
        &self.inner_peptide_condition
    }
}

impl From<PeptideCondition> for FinalizedPeptideCondition {
    fn from(peptide_condition: PeptideCondition) -> Self {
        let filter_functions = FinalizedPeptideCondition::get_filter_functions(&peptide_condition);
        Self {
            inner_peptide_condition: peptide_condition,
            filter_functions,
        }
    }
}

impl Display for FinalizedPeptideCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let filter_descriptions = self
            .filter_functions
            .iter()
            .map(|filter| format!("{filter}"))
            .join(" && ");
        write!(
            f,
            "FinalizedPeptideCondition: {filter_descriptions} @ {}",
            mass_to_float(self.query_mass)
        )
    }
}

#[cfg(test)]
mod tests {
    use dihardts_omicstools::{
        chemistry::amino_acid::get_amino_acid_by_one_letter_code,
        proteomics::post_translational_modifications::{ModificationType, Position},
    };

    use super::*;

    #[tokio::test]
    async fn test_peptide_condition_from_ptm_collection() {
        let ptms = vec![
            Arc::new(PostTranslationalModification::new(
                "carba of C",
                get_amino_acid_by_one_letter_code('C').unwrap(),
                57.021464,
                ModificationType::Static,
                Position::Anywhere,
            )),
            Arc::new(PostTranslationalModification::new(
                "oxi of M",
                get_amino_acid_by_one_letter_code('M').unwrap(),
                15.99491,
                ModificationType::Variable,
                Position::Anywhere,
            )),
            Arc::new(PostTranslationalModification::new(
                "oxi of term M",
                get_amino_acid_by_one_letter_code('M').unwrap(),
                16.99491,
                ModificationType::Variable,
                Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::N),
            )),
            Arc::new(PostTranslationalModification::new(
                "oxi of term K",
                get_amino_acid_by_one_letter_code('K').unwrap(),
                20.3,
                ModificationType::Variable,
                Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::C),
            )),
            Arc::new(PostTranslationalModification::new(
                "something on N-bond",
                get_amino_acid_by_one_letter_code('X').unwrap(),
                10.0,
                ModificationType::Variable,
                Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::N),
            )),
            Arc::new(PostTranslationalModification::new(
                "something on N-bond",
                get_amino_acid_by_one_letter_code('X').unwrap(),
                40.3,
                ModificationType::Variable,
                Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::C),
            )),
        ];
        let ptm_collection = PTMCollection::new(ptms).unwrap();
        let mass: f64 = 839.403366202; // MFCQLAK

        let conditions = PeptideCondition::from_ptm_collection(
            &ptm_collection,
            mass_to_int(mass),
            mass_to_int(
                get_amino_acid_by_one_letter_code('G')
                    .unwrap()
                    .get_mono_mass()
                    * 6.0,
            ),
            mass_to_int(mass),
            2,
        );

        // Easiest way is to check the string representation of the conditions which gives basically a unique representation of the condition
        let stringyfied_conditions = conditions
            .into_iter()
            .map(|condition| format!("{}", FinalizedPeptideCondition::from(condition)))
            .collect::<HashSet<_>>();

        let expected_conditions =
            std::fs::read_to_string("test_files/finalized_peptide_condition.txt")
                .unwrap()
                .split("\n")
                .map(|line| line.to_string())
                .collect::<HashSet<_>>();

        assert_eq!(stringyfied_conditions.len(), expected_conditions.len());

        for condition in stringyfied_conditions.iter() {
            assert!(
                expected_conditions.contains(condition),
                "Condition not found: {condition}"
            );
        }
    }

    /// Consequently tests various types of PTMs to build conition for checking a sequence on a sequence.
    ///
    #[test]
    fn test_condition_building_and_sequence_modification() {
        let sequence = "MFCQLAKTCPVQLWVDMSTPPPGTRVR";
        let mass = 3060.516981066636;

        let peptide = Peptide::new(
            0,
            mass_to_int(3060.516981066636),
            sequence.to_string(),
            2,
            Vec::new(),
            false,
            false,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();

        let carbamidomethylation_c = Arc::new(PostTranslationalModification::new(
            "carba of C",
            get_amino_acid_by_one_letter_code('C').unwrap(),
            57.021464,
            ModificationType::Static,
            Position::Anywhere,
        ));

        let oxidation_m = Arc::new(PostTranslationalModification::new(
            "oxi of M",
            get_amino_acid_by_one_letter_code('M').unwrap(),
            15.99491,
            ModificationType::Variable,
            Position::Anywhere,
        ));

        let something_terminal_m = Arc::new(PostTranslationalModification::new(
            "oxi of term M",
            get_amino_acid_by_one_letter_code('M').unwrap(),
            16.99491,
            ModificationType::Variable,
            Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::N),
        ));

        let something_terminal_r = Arc::new(PostTranslationalModification::new(
            "oxi of term R",
            get_amino_acid_by_one_letter_code('R').unwrap(),
            20.3,
            ModificationType::Variable,
            Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::C),
        ));

        let something_bond_n = Arc::new(PostTranslationalModification::new(
            "something on N-bond",
            get_amino_acid_by_one_letter_code('X').unwrap(),
            10.0,
            ModificationType::Variable,
            Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::N),
        ));

        let something_bond_c = Arc::new(PostTranslationalModification::new(
            "something on N-bond",
            get_amino_acid_by_one_letter_code('X').unwrap(),
            40.3,
            ModificationType::Variable,
            Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::C),
        ));

        let mut condition = PeptideCondition::new(mass_to_int(mass));
        condition.add_static_ptm(carbamidomethylation_c.clone());
        condition.add_static_ptm(carbamidomethylation_c.clone());
        condition.add_variable_ptm(oxidation_m.clone());

        let mut finalized_condition: FinalizedPeptideCondition = condition.clone().into();

        assert!(finalized_condition.check_peptide(&peptide));

        let mut modified_sequences = condition.modify_sequence(sequence);
        modified_sequences.sort();
        assert_eq!(
            modified_sequences.as_slice(),
            [
                "<[+57.021464]@C>MFCQLAKTCPVQLWVDM[+15.99491]STPPPGTRVR",
                "<[+57.021464]@C>M[+15.99491]FCQLAKTCPVQLWVDMSTPPPGTRVR"
            ]
        );

        condition.set_n_terminal_ptm(something_terminal_m.clone());
        finalized_condition = condition.clone().into();
        assert!(finalized_condition.check_peptide(&peptide));

        let mut modified_sequences = condition.modify_sequence(sequence);
        modified_sequences.sort();
        assert_eq!(
            modified_sequences.as_slice(),
            ["<[+57.021464]@C>M[+16.99491]FCQLAKTCPVQLWVDM[+15.99491]STPPPGTRVR",]
        );

        condition.set_c_terminal_ptm(something_terminal_r.clone());
        finalized_condition = condition.clone().into();
        assert!(finalized_condition.check_peptide(&peptide));

        let mut modified_sequences = condition.modify_sequence(sequence);
        modified_sequences.sort();
        assert_eq!(
            modified_sequences.as_slice(),
            ["<[+57.021464]@C>M[+16.99491]FCQLAKTCPVQLWVDM[+15.99491]STPPPGTRVR[+20.3]",]
        );

        condition.set_n_bond_ptm(something_bond_n.clone());
        finalized_condition = condition.clone().into();
        assert!(finalized_condition.check_peptide(&peptide));

        let mut modified_sequences = condition.modify_sequence(sequence);
        modified_sequences.sort();
        assert_eq!(
            modified_sequences.as_slice(),
            ["<[+57.021464]@C>[10]-M[+16.99491]FCQLAKTCPVQLWVDM[+15.99491]STPPPGTRVR[+20.3]",]
        );

        condition.set_c_bond_ptm(something_bond_c.clone());
        finalized_condition = condition.clone().into();
        assert!(finalized_condition.check_peptide(&peptide));

        let mut modified_sequences = condition.modify_sequence(sequence);
        modified_sequences.sort();
        assert_eq!(
            modified_sequences.as_slice(),
            ["<[+57.021464]@C>[10]-M[+16.99491]FCQLAKTCPVQLWVDM[+15.99491]STPPPGTRVR[+20.3]-[40.3]",]
        );
    }
}
