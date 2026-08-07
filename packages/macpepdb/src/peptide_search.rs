use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

/// Lock-free accumulator for per-condition timings within a single search, so the
/// search emits one compact summary line instead of ~2 lines per condition. The
/// per-event detail is still logged at `debug` (target `search_timing`).
#[derive(Default)]
struct SearchTimingAgg {
    /// Sum/max of stream-open time (pool checkout + prepare + dispatch).
    setup_us_sum: AtomicU64,
    setup_us_max: AtomicU64,
    /// Sum/max of scan time (open -> stream exhausted), excluding setup.
    scan_us_sum: AtomicU64,
    scan_us_max: AtomicU64,
    rows: AtomicU64,
    /// Conditions covered by the statements that have finished, and the number of
    /// statements it took to cover them. They differ because conditions whose partitions
    /// share a Citus shard are OR-ed into one statement (see [`crate::shard_map`]); the
    /// ratio is the batching factor actually achieved.
    conditions: AtomicU64,
    statements: AtomicU64,
    /// Widest disjunction issued, i.e. the most conditions OR-ed into one statement.
    conditions_max: AtomicU64,
    /// Max number of distinct partitions named by a single statement. All of them live on
    /// one shard when batching applied, so this is no longer a shard fan-out measure.
    partitions_max: AtomicU64,
    /// Peptidoforms offered to the distinct filter, and how many of those it rejected as
    /// already seen. Only counted while a distinct filter is active. Duplicates can only
    /// arise *between* conditions — within one condition the DB rows are distinct and the
    /// variable-modification map is deduped by mass delta — so this measures whether the
    /// global dedup is earning its share of search CPU.
    distinct_candidates: AtomicU64,
    distinct_duplicates: AtomicU64,
}

impl SearchTimingAgg {
    fn record_setup(&self, us: u64, partitions: u64, conditions: u64) {
        self.setup_us_sum.fetch_add(us, Ordering::Relaxed);
        fetch_max(&self.setup_us_max, us);
        fetch_max(&self.partitions_max, partitions);
        fetch_max(&self.conditions_max, conditions);
    }

    fn record_scan(&self, us: u64, rows: u64, conditions: u64) {
        self.scan_us_sum.fetch_add(us, Ordering::Relaxed);
        fetch_max(&self.scan_us_max, us);
        self.rows.fetch_add(rows, Ordering::Relaxed);
        self.conditions.fetch_add(conditions, Ordering::Relaxed);
        self.statements.fetch_add(1, Ordering::Relaxed);
    }

    /// Folds one condition's distinct-filter tallies into the search total. Called once
    /// per stream at exhaustion (not per peptidoform) so the hot path stays free of
    /// cross-thread atomics.
    fn record_distinct(&self, candidates: u64, duplicates: u64) {
        self.distinct_candidates
            .fetch_add(candidates, Ordering::Relaxed);
        self.distinct_duplicates
            .fetch_add(duplicates, Ordering::Relaxed);
    }
}

fn fetch_max(slot: &AtomicU64, v: u64) {
    let mut cur = slot.load(Ordering::Relaxed);
    while v > cur {
        match slot.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => cur = observed,
        }
    }
}

use dashmap::DashSet;
use futures::stream::{Stream, StreamExt};
use itertools::Itertools;
use metrics::{Counter, counter};
use postgres_types::ToSql;
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use xxhash_rust::xxh3::Xxh3;

use crate::amino_acid::{AminoAcid, AminoAcidBitCode, GLYCINE};
use crate::configuration::RuntimeConfiguration;
use crate::database_build::MassPartitionMap;
use crate::molecules::WATER_MONO_MASS;
use crate::peptide::{
    IS_SWISS_PROT_BIT, IS_TREMBL_BIT, IsPeptide, MAX_AMINO_ACID_BIT_CODE, Peptidoform,
};
use crate::peptide_table::{
    FLAGS_COLUMN, MASS_COL, PARTITION_COL, PeptideColumnSelection, PeptideTable, TABLE_NAME,
};
use crate::post_translational_modification::{PTMCollection, PostTranslationalModification};
use crate::sequence::{IsSimpleSequence, ModifiedSequence};
use crate::shard_map;
use crate::{mass::to_float as mass_to_float, peptide::Peptide};

use super::client::Client;

/// Base name for the metric counting matching peptides emitted by a search; each search
/// instance appends a unique suffix to it to get its own counter.
pub static MATCHING_PEPTIDE_METRIC: &str = "peptide_search:matching_peptides";

const SEARCH_COLUMNS: &str =
    "mass, sequence, protein_ids, unique_taxonomy_ids, non_unique_taxonomy_ids, flags";

/// Inlined-literal `SELECT` of `SEARCH_COLUMNS` used by the mass-search read path
/// ([`PeptideTable::select_inline`]); a `where_clause` is appended per query.
pub static SEARCH_SELECT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT {SEARCH_COLUMNS} FROM {TABLE_NAME}"));

/// Errors occurring while building or running a peptide search.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in peptide search: {0}")]
    Client(Box<crate::client::Error>),
    #[error("Invalid peptide search type `{0}`")]
    InvalidPeptideSearchType(String),
    #[error("Missing condition for condition reference index {0} of {1}")]
    MissingCondition(usize, usize),
    #[error("Database error in peptide search: {0}")]
    NextPeptide(Box<tokio_postgres::Error>),
    #[error("Filter function is not SQL-able: {0}")]
    NonSqlAbleFilter(String),
    #[error("Peptide error in peptide search: {0}")]
    Peptide(Box<crate::peptide::Error>),
    #[error("Peptide table error in peptide search: {0}")]
    PeptideTable(Box<crate::peptide_table::Error>),
    #[error("Query portal error in peptide search: {0}")]
    QueryPortal(Box<tokio_postgres::Error>),
    #[error("Transaction error in peptide search: {0}")]
    Transaction(Box<tokio_postgres::Error>),
    #[error("Row to peptide conversion error in peptide search: {0}")]
    RowPeptideConversion(Box<tokio_postgres::Error>),
    #[error("Unable to transform peptide into requested format, underlaying error: {0}")]
    Transformation(String),
}

into_thiserror_boxed!(crate::client::Error, Error, Client);
into_thiserror_boxed!(crate::peptide::Error, Error, Peptide);
into_thiserror_boxed!(crate::peptide_table::Error, Error, PeptideTable);

/// Trait to check conditions on peptides
///
pub trait FilterFunction<T: IsPeptide>: Send + Sync + Display {
    /// Returns true if the peptide matches the condition, false otherwise.
    ///
    /// # Arguments
    /// * `peptide` - The peptide to check
    ///
    fn is_match(&self, peptide: &T) -> Result<bool, Error>;

    fn to_sql(&self, filters: &mut Vec<String>, params: &mut Vec<Box<dyn ToSql + Sync + Send>>);

    fn to_sql_literal(&self, filters: &mut Vec<String>);

    fn is_sqlable(&self) -> bool;
}

/// Filters peptides which not are in SwissProt
///
struct IsSwissProtFilterFunction;

impl<T> FilterFunction<T> for IsSwissProtFilterFunction
where
    T: IsPeptide,
{
    fn is_match(&self, peptide: &T) -> Result<bool, Error> {
        Ok(peptide.is_swiss_prot())
    }

    fn to_sql(&self, filters: &mut Vec<String>, _params: &mut Vec<Box<dyn ToSql + Sync + Send>>) {
        filters.push(format!(
            "ascii({FLAGS_COLUMN}) & {} = 1",
            IS_SWISS_PROT_BIT + 1
        ))
    }

    fn to_sql_literal(&self, filters: &mut Vec<String>) {
        filters.push(format!(
            "ascii({FLAGS_COLUMN}) & {} = 1",
            IS_SWISS_PROT_BIT + 1
        ));
    }

    fn is_sqlable(&self) -> bool {
        true
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

impl<T> FilterFunction<T> for IsTrEMBLFilterFunction
where
    T: IsPeptide,
{
    fn is_match(&self, peptide: &T) -> Result<bool, Error> {
        Ok(peptide.is_trembl())
    }

    fn to_sql(&self, filters: &mut Vec<String>, _params: &mut Vec<Box<dyn ToSql + Sync + Send>>) {
        filters.push(format!("ascii({FLAGS_COLUMN}) & {} = 2", IS_TREMBL_BIT + 1));
    }

    fn to_sql_literal(&self, filters: &mut Vec<String>) {
        filters.push(format!("ascii({FLAGS_COLUMN}) & {} = 2", IS_TREMBL_BIT + 1));
    }

    fn is_sqlable(&self) -> bool {
        true
    }
}

impl Display for IsTrEMBLFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "is TrEMBL")
    }
}

/// Filters peptides which are not in the given taxonomy IDs
///
struct TaxonomyFilterFunction {
    taxonomy_ids: Arc<Vec<i32>>,
}

impl<T> FilterFunction<T> for TaxonomyFilterFunction
where
    T: IsPeptide,
{
    fn is_match(&self, peptide: &T) -> Result<bool, Error> {
        for taxonomy_id in self.taxonomy_ids.iter() {
            if peptide.non_unique_taxonomy_ids().contains(taxonomy_id)
                || peptide.unique_taxonomy_ids().contains(taxonomy_id)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn to_sql(&self, filters: &mut Vec<String>, params: &mut Vec<Box<dyn ToSql + Sync + Send>>) {
        filters.push(format!(
            "(unique_taxonomy_ids && Array[${}] OR non_unique_taxonomy_ids && Array[${}])",
            params.len() + 1,
            params.len() + 2
        ));
        params.push(Box::new(
            self.taxonomy_ids.iter().cloned().collect::<Vec<i32>>(),
        ));
        params.push(Box::new(
            self.taxonomy_ids.iter().cloned().collect::<Vec<i32>>(),
        ));
    }

    fn to_sql_literal(&self, filters: &mut Vec<String>) {
        filters.push(format!(
            "(unique_taxonomy_ids && Array[{ids}] OR non_unique_taxonomy_ids && Array[{ids}])",
            ids = self.taxonomy_ids.iter().join(",")
        ));
    }

    fn is_sqlable(&self) -> bool {
        true
    }
}

impl Display for TaxonomyFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "taxonomy in [{}]", self.taxonomy_ids.iter().join(", "))
    }
}

// /// Filters peptides which are not in the given proteome IDs
// ///
// struct ProteomeFilterFunction {
//     proteome_ids: Arc<Vec<String>>,
// }

// impl FilterFunction for ProteomeFilterFunction {
//     fn is_match(&self, peptide: &Peptide) -> Result<bool, Error> {
//         for proteome_id in self.proteome_ids.iter() {
//             if peptide.get_proteome_ids().contains(proteome_id) {
//                 return Ok(true);
//             }
//         }
//         Ok(false)
//     }
// }

// impl Display for ProteomeFilterFunction {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "proteome in [{}]", self.proteome_ids.iter().join(", "))
//     }
// }

/// Filters peptides which start with a specific amino acid
///
struct StartsWithFilterFunction {
    amino_acid: AminoAcidBitCode,
}

impl<T> FilterFunction<T> for StartsWithFilterFunction
where
    T: IsPeptide,
{
    fn is_match(&self, peptide: &T) -> Result<bool, Error> {
        Ok(peptide.sequence().first() == Some(&self.amino_acid))
    }

    fn to_sql(&self, _filters: &mut Vec<String>, _params: &mut Vec<Box<dyn ToSql + Sync + Send>>) {}

    fn to_sql_literal(&self, _filters: &mut Vec<String>) {}

    fn is_sqlable(&self) -> bool {
        false
    }
}

impl Display for StartsWithFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "starts with '{}'",
            AminoAcid::by_bit_code(&self.amino_acid).code()
        )
    }
}

/// Filters peptides which end with a specific amino acid
///
struct EndsWithFilterFunction {
    /// One letter code of the amino acid
    amino_acid: AminoAcidBitCode,
}

impl<T> FilterFunction<T> for EndsWithFilterFunction
where
    T: IsPeptide,
{
    fn is_match(&self, peptide: &T) -> Result<bool, Error> {
        Ok(peptide.sequence().last() == Some(&self.amino_acid))
    }

    fn to_sql(&self, _filters: &mut Vec<String>, _params: &mut Vec<Box<dyn ToSql + Sync + Send>>) {}

    fn to_sql_literal(&self, _filters: &mut Vec<String>) {}

    fn is_sqlable(&self) -> bool {
        false
    }
}

impl Display for EndsWithFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ends with '{}'",
            AminoAcid::by_bit_code(&self.amino_acid).code()
        )
    }
}

/// Filters peptides contains an specific amount occurrences of an amino acid
///
struct EqualsNumberOfOccurrencesFilterFunction {
    /// One letter code of the amino acid
    amino_acid: AminoAcidBitCode,
    amount: u8,
}

impl<T> FilterFunction<T> for EqualsNumberOfOccurrencesFilterFunction
where
    T: IsPeptide,
{
    fn is_match(&self, peptide: &T) -> Result<bool, Error> {
        let count = peptide.amino_acid_count_by_bit_code(self.amino_acid);
        Ok(count == self.amount)
    }

    fn to_sql(&self, filters: &mut Vec<String>, params: &mut Vec<Box<dyn ToSql + Sync + Send>>) {
        filters.push(format!(
            "get_byte(amino_acid_counts, ${}) = ${}",
            params.len() + 1,
            params.len() + 2
        ));
        params.push(Box::new(
            AminoAcid::by_bit_code(&self.amino_acid).counts_idx() as i32,
        ));
        params.push(Box::new(self.amount as i32));
    }

    fn to_sql_literal(&self, filters: &mut Vec<String>) {
        filters.push(format!(
            "get_byte(amino_acid_counts, {}) = {}",
            AminoAcid::by_bit_code(&self.amino_acid).counts_idx(),
            self.amount
        ));
    }

    fn is_sqlable(&self) -> bool {
        true
    }
}

impl Display for EqualsNumberOfOccurrencesFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "occurrences of '{}' == {}",
            AminoAcid::by_bit_code(&self.amino_acid).code(),
            self.amount,
        )
    }
}

/// Filters peptides contains an specific amount occurrences of an amino acid
///
struct GreaterOrEqualsNumberOfOccurrencesFilterFunction {
    /// One letter code of the amino acid
    amino_acid: AminoAcidBitCode,
    amount: u8,
}

impl<T> FilterFunction<T> for GreaterOrEqualsNumberOfOccurrencesFilterFunction
where
    T: IsPeptide,
{
    fn is_match(&self, peptide: &T) -> Result<bool, Error> {
        let count = peptide.amino_acid_count_by_bit_code(self.amino_acid);
        Ok(count >= self.amount)
    }

    fn to_sql(&self, filters: &mut Vec<String>, params: &mut Vec<Box<dyn ToSql + Sync + Send>>) {
        filters.push(format!(
            "get_byte(amino_acid_counts, ${}) >= ${}",
            params.len() + 1,
            params.len() + 2
        ));
        params.push(Box::new(
            AminoAcid::by_bit_code(&self.amino_acid).counts_idx() as i32,
        ));
        params.push(Box::new(self.amount as i32));
    }

    fn to_sql_literal(&self, filters: &mut Vec<String>) {
        filters.push(format!(
            "get_byte(amino_acid_counts, {}) >= {}",
            AminoAcid::by_bit_code(&self.amino_acid).counts_idx(),
            self.amount
        ));
    }

    fn is_sqlable(&self) -> bool {
        true
    }
}

impl Display for GreaterOrEqualsNumberOfOccurrencesFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "occurrences of '{}' >= {}",
            AminoAcid::by_bit_code(&self.amino_acid).code(),
            self.amount,
        )
    }
}

/// Filters peptides contains an specific amount occurrences of an amino acid
///
struct NoOccurrencesFilterFunction {
    /// One letter code of the amino acid
    amino_acid: AminoAcidBitCode,
}

impl<T> FilterFunction<T> for NoOccurrencesFilterFunction
where
    T: IsPeptide,
{
    fn is_match(&self, peptide: &T) -> Result<bool, Error> {
        let count = peptide.amino_acid_count_by_bit_code(self.amino_acid);
        Ok(count == 0)
    }

    fn to_sql(&self, filters: &mut Vec<String>, params: &mut Vec<Box<dyn ToSql + Sync + Send>>) {
        filters.push(format!(
            "get_byte(amino_acid_counts, ${}) = ${}",
            params.len() + 1,
            params.len() + 2
        ));
        params.push(Box::new(
            AminoAcid::by_bit_code(&self.amino_acid).counts_idx() as i32,
        ));
        params.push(Box::new(0_i32));
    }

    fn to_sql_literal(&self, filters: &mut Vec<String>) {
        filters.push(format!(
            "get_byte(amino_acid_counts, {}) = 0",
            AminoAcid::by_bit_code(&self.amino_acid).counts_idx(),
        ));
    }

    fn is_sqlable(&self) -> bool {
        true
    }
}

impl Display for NoOccurrencesFilterFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "occurrences of '{}' == 0",
            AminoAcid::by_bit_code(&self.amino_acid).code(),
        )
    }
}

/// Ordered set of [`FilterFunction`]s applied to a peptide/peptidoform; a peptide is kept
/// only if it passes every filter in the pipeline.
pub struct FilterPipeline<T: IsPeptide> {
    filter_functions: Vec<Box<dyn FilterFunction<T>>>,
}

impl<T> FilterPipeline<T>
where
    T: IsPeptide + 'static,
{
    /// Creates a new pipeline from the given filter functions.
    pub fn new(filter_functions: Vec<Box<dyn FilterFunction<T>>>) -> Self {
        Self { filter_functions }
    }

    /// Creates sql able filters for global attribues like review status and taxonomy affiliation
    ///
    /// # Arguments
    /// * `taxonomy_ids` - Optional list of taxonomy IDs to filter by
    /// * `is_reviewed` - Optional boolean to filter by review status (true for SwissProt, false for TrEMBL)
    ///
    pub fn new_for_general_sql_able_peptide_attributes(
        taxonomy_ids: Option<Arc<Vec<i32>>>,
        is_reviewed: Option<bool>,
    ) -> Result<Self, Error> {
        let mut filter_function: Vec<Box<dyn FilterFunction<T>>> = Vec::new();
        if let Some(taxonomy_ids) = taxonomy_ids {
            filter_function.push(Box::new(TaxonomyFilterFunction { taxonomy_ids }));
        }
        if let Some(is_reviewed) = is_reviewed {
            if is_reviewed {
                filter_function.push(Box::new(IsSwissProtFilterFunction {}));
            } else {
                filter_function.push(Box::new(IsTrEMBLFilterFunction {}));
            }
        }
        Ok(Self::new(filter_function))
    }

    /// Returns true if the peptide passes every filter function in the pipeline.
    pub fn is_match(&self, peptide: &T) -> Result<bool, Error> {
        for filter in self.filter_functions.iter() {
            if !filter.is_match(peptide)? {
                // tracing::info!(
                //     "[peptide_search::filter_pipeline] peptide did not match filter: {}",
                //     filter
                // );
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Returns the number of filter functions in the pipeline.
    pub fn len(&self) -> usize {
        self.filter_functions.len()
    }

    /// Returns true if the pipeline has no filter functions.
    pub fn is_empty(&self) -> bool {
        self.filter_functions.is_empty()
    }

    /// Returns an iterator over the filter functions in the pipeline.
    pub fn iter(&self) -> impl Iterator<Item = &Box<dyn FilterFunction<T>>> {
        self.filter_functions.iter()
    }

    /// Drops every filter function that is already handled as a SQL `WHERE` condition
    /// (see [`FilterFunction::is_sqlable`]), leaving only the ones that must still be
    /// checked in-process.
    pub fn remove_sqlable_filters(&mut self) {
        self.filter_functions
            .retain(|filter_fn| !filter_fn.is_sqlable());
        self.filter_functions.shrink_to_fit();
    }
}

impl<T> Display for FilterPipeline<T>
where
    T: IsPeptide,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FilterPipeline[{}]",
            self.filter_functions
                .iter()
                .map(|f| f.to_string())
                .join(", ")
        )
    }
}

impl<T> Drop for FilterPipeline<T>
where
    T: IsPeptide,
{
    fn drop(&mut self) {
        while let Some(filter) = self.filter_functions.pop() {
            drop(filter);
        }
    }
}

pub trait IsPeptidoformTransformation: Unpin + Send {
    /// `Unpin` because [`ConditionalPeptideStream`] buffers outputs in a `Vec` before
    /// emitting them, and a stream holding one has to stay `Unpin` itself.
    type Output: Send + Unpin + 'static;
    type Error: std::fmt::Display;

    fn try_from_peptidoform(peptidoform: Peptidoform) -> Result<Self::Output, Self::Error>;
}

pub struct PeptidoformPassthroughTransformation;

impl IsPeptidoformTransformation for PeptidoformPassthroughTransformation {
    type Output = Peptidoform;
    type Error = Error;

    fn try_from_peptidoform(peptidoform: Peptidoform) -> Result<Self::Output, Self::Error> {
        Ok(peptidoform)
    }
}

pub struct PeptidoformToJsonTransformation;
impl IsPeptidoformTransformation for PeptidoformToJsonTransformation {
    type Output = String;
    type Error = serde_json::Error;

    fn try_from_peptidoform(
        peptidoform: crate::peptide::Peptidoform,
    ) -> Result<Self::Output, Self::Error> {
        serde_json::to_string(
            &macpepdb_web_common::responses::peptide::PeptideResponse::from(&peptidoform),
        )
    }
}

pub struct PeptidoformToPlainTextTransformation;
impl IsPeptidoformTransformation for PeptidoformToPlainTextTransformation {
    type Output = String;
    type Error = serde_json::Error;

    fn try_from_peptidoform(
        peptidoform: crate::peptide::Peptidoform,
    ) -> Result<Self::Output, Self::Error> {
        Ok(peptidoform.sequence().to_string())
    }
}

/// 128-bit digest of a [`ModifiedSequence`], used as the distinct filter's key instead of
/// the sequence itself. The set only ever needs to *recognise* a sequence, never reproduce
/// it, so storing a digest avoids one ~600-byte clone per candidate and shrinks the set from
/// hundreds of megabytes to 16 bytes per entry — which also keeps its buckets in cache.
///
/// Collision risk is a birthday bound of `n^2 / 2^129`; at ten million peptidoforms in a
/// single search that is ~1e-25, and a collision could only ever drop one peptidoform from
/// one result — it cannot produce a wrong one.
fn distinct_key(sequence: &ModifiedSequence) -> u128 {
    let mut hasher = Xxh3::new();
    sequence.hash(&mut hasher);
    hasher.digest128()
}

type BoxedPeptideRowStream =
    Pin<Box<dyn Stream<Item = Result<Peptide, crate::peptide_table::Error>> + Send>>;

/// Peptidoforms accumulated per message handed to the search's collector.
///
/// Emitting one message per DB row made the channel a hot shared structure: every send
/// touched the same tail cache line and woke the single receiver, which cost ~12% of
/// process CPU with 64 producers. Batching amortises the send, the wake-up and the `Vec`
/// allocation across this many peptidoforms. Sized so a batch stays small in absolute
/// terms (~11 KB of plain text, ~64 KB of JSON) — large enough that the per-message
/// overhead disappears, small enough that a stalled consumer pins little memory per
/// in-flight condition.
const OUTPUT_BATCH_SIZE: usize = 256;

/// Upper bound on how many conditions are OR-ed into a single statement.
///
/// With ~800k partitions over 1024 shards a shard collects ~3.6 conditions on average, so
/// this cap almost never bites — it exists to bound the tail. A disjunction has to be
/// evaluated against every chunk group the task scans, so an unbounded one would trade the
/// metadata scans it saves for filter evaluations it adds, and would push the planner into
/// territory that has not been measured.
const MAX_CONDITIONS_PER_STATEMENT: usize = 16;

/// The conditions covered by one statement.
///
/// Every condition in a group resolves to the same Citus shard, so the statement plans as a
/// single task (`Task Count: 1`) that scans that shard's columnar metadata once for all of
/// them, instead of once per condition. The `WHERE` clause is the disjunction of the
/// conditions' predicates; Citus pushes it into `Columnar Chunk Group Filters` intact, so
/// each disjunct still prunes to its own chunk groups.
///
/// A group of one is the pre-batching shape and keeps its behaviour exactly.
pub struct ConditionGroup {
    conditions: Vec<PeptideCondition>,
}

impl ConditionGroup {
    /// Number of conditions covered by this group's statement.
    fn len(&self) -> usize {
        self.conditions.len()
    }

    /// Distinct partitions named by this group's statement.
    fn num_partitions(&self) -> usize {
        self.conditions
            .iter()
            .flat_map(|condition| condition.partitions())
            .unique()
            .count()
    }

    /// Builds the statement's `WHERE` clause and strips from each condition's in-process
    /// filter pipeline whatever the SQL now guarantees.
    ///
    /// For a single condition the SQL fully determines the result set, so its SQL-able
    /// filters are dropped and only the leftovers are re-checked per row — the pre-batching
    /// behaviour. For a disjunction the SQL only guarantees that *some* disjunct matched, so
    /// every condition keeps its full pipeline and re-verifies it per row; that is what lets
    /// [`ConditionGroup::accepting`] attribute a row to the condition(s) it actually belongs
    /// to.
    ///
    /// # Arguments
    /// * `sql_filters` - Global filters (taxonomy, review status) AND-ed onto the disjunction
    fn where_clause(&mut self, sql_filters: &FilterPipeline<Peptide>) -> String {
        let is_batched = self.conditions.len() > 1;
        let mut disjuncts = Vec::with_capacity(self.conditions.len());

        for condition in self.conditions.iter_mut() {
            // Inline literals (no bind params): Citus prunes shards + columnar chunk groups
            // at plan time only for an inlined query — a parameterized distributed query
            // re-plans every execute (~11 ms) and cannot use a cached generic plan.
            //
            // Plain equality rather than `= ANY(ARRAY[...])` for the single-partition case
            // (which is what `finalize` always produces): that is the form measured to keep
            // chunk-group pruning intact inside a disjunction.
            let partitions = condition.partitions();
            let mut filters = vec![
                if let [partition] = partitions.as_slice() {
                    format!("{PARTITION_COL} = {partition}")
                } else {
                    format!(
                        "{PARTITION_COL} = ANY(ARRAY[{}]::bigint[])",
                        partitions.iter().join(",")
                    )
                },
                format!("{MASS_COL} >= {}", condition.lower_mass()),
                format!("{MASS_COL} <= {}", condition.upper_mass()),
            ];
            // Condition-specific clauses; these are what actually locate the peptides.
            for filter_fn in condition.filter_pipeline().iter() {
                filter_fn.to_sql_literal(&mut filters);
            }
            if !is_batched {
                condition.remove_sqlable_filters();
            }
            disjuncts.push(format!("({})", filters.join(" AND ")));
        }

        let mut clauses = vec![if is_batched {
            format!("({})", disjuncts.join(" OR "))
        } else {
            disjuncts.pop().unwrap_or_else(|| "false".to_string())
        }];
        // Global clauses apply to every disjunct, so they stay outside the parentheses.
        for filter_fn in sql_filters.iter() {
            filter_fn.to_sql_literal(&mut clauses);
        }

        format!("WHERE {}", clauses.join(" AND "))
    }

    /// Indices of the conditions `peptide` satisfies, appended to `out`.
    ///
    /// A row returned by a disjunction satisfies at least one disjunct but the statement
    /// does not say which, and it may satisfy several — overlapping ppm windows carrying
    /// different PTM hypotheses. Each match is a separate result, exactly as when every
    /// condition had its own statement and both returned the row.
    fn accepting(&self, peptide: &Peptide, out: &mut Vec<usize>) {
        out.clear();
        for (index, condition) in self.conditions.iter().enumerate() {
            if condition.accepts(peptide) {
                out.push(index);
            }
        }
    }
}

struct ConditionalPeptideStream<T: IsPeptidoformTransformation> {
    group: ConditionGroup,
    /// Scratch buffer for [`ConditionGroup::accepting`], reused across rows so the per-row
    /// path stays allocation-free.
    matching: Vec<usize>,
    inner: BoxedPeptideRowStream,
    resolve_modification: bool,
    /// Peptidoforms produced but not yet emitted. Flushed at [`OUTPUT_BATCH_SIZE`], when
    /// the inner row stream goes pending, and at exhaustion.
    batch: Vec<T::Output>,
    /// Inner row stream returned `None`; the partial batch still has to drain first.
    inner_done: bool,
    /// Error seen while the batch was non-empty. Held back so rows produced before it are
    /// still delivered, then surfaced on the following poll.
    pending_error: Option<Error>,
    // ── timing diagnostics ──
    agg: Arc<SearchTimingAgg>,
    opened_at: std::time::Instant,
    rows: u64,
    /// Peptidoforms this condition offered to `distinct_filter`, and the subset it
    /// rejected. Accumulated per-stream and folded into `agg` at exhaustion — same
    /// pattern as `rows`, to keep atomics off the per-peptidoform path.
    distinct_candidates: u64,
    distinct_duplicates: u64,
    distinct_filter: Option<Arc<DashSet<u128>>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> ConditionalPeptideStream<T>
where
    T: IsPeptidoformTransformation,
{
    /// Opens the DB query for one [`ConditionGroup`] and wraps the resulting row stream,
    /// recording open/scan timings into `agg` as the stream is consumed.
    pub async fn new(
        client: Arc<Client>,
        selection: &'static PeptideColumnSelection,
        mut group: ConditionGroup,
        sql_filters: Arc<FilterPipeline<Peptide>>,
        resolve_modification: bool,
        agg: Arc<SearchTimingAgg>,
        distinct_filter: Option<Arc<DashSet<u128>>>,
    ) -> Result<Self, Error> {
        for filter in sql_filters.iter() {
            if !filter.is_sqlable() {
                return Err(Error::NonSqlAbleFilter(format!("{filter}")));
            }
        }

        let num_partitions = group.num_partitions() as u64;
        let num_conditions = group.len() as u64;
        let where_clause = group.where_clause(&sql_filters);
        // The statement verbatim, so a real batched disjunction can be pulled from the log
        // and run through `EXPLAIN` to confirm chunk-group pruning survived the OR.
        tracing::debug!(
            target: "search_timing",
            conditions = num_conditions,
            partitions = num_partitions,
            "{where_clause}"
        );

        let setup_start = std::time::Instant::now();
        let inner: BoxedPeptideRowStream = Box::pin(
            PeptideTable::new(client)
                .select_inline(selection, &where_clause)
                .await?,
        );
        agg.record_setup(
            setup_start.elapsed().as_micros() as u64,
            num_partitions,
            num_conditions,
        );
        Ok(Self {
            resolve_modification,
            inner,
            matching: Vec::with_capacity(group.len()),
            group,
            agg,
            opened_at: std::time::Instant::now(),
            rows: 0,
            batch: Vec::with_capacity(OUTPUT_BATCH_SIZE),
            inner_done: false,
            pending_error: None,
            distinct_candidates: 0,
            distinct_duplicates: 0,
            distinct_filter,
            _marker: std::marker::PhantomData,
        })
    }

    /// Hands off the accumulated batch and installs a fresh, pre-sized buffer in its place.
    fn take_batch(&mut self) -> Vec<T::Output> {
        std::mem::replace(&mut self.batch, Vec::with_capacity(OUTPUT_BATCH_SIZE))
    }

    /// Runs one peptidoform through the distinct filter and the output transformation,
    /// pushing the result into the batch. A peptidoform the filter has already seen is
    /// dropped silently.
    fn accept(&mut self, peptidoform: Peptidoform) -> Result<(), Error> {
        if let Some(set) = self.distinct_filter.as_ref() {
            self.distinct_candidates += 1;
            if !set.insert(distinct_key(peptidoform.sequence())) {
                self.distinct_duplicates += 1;
                return Ok(());
            }
        }
        match T::try_from_peptidoform(peptidoform) {
            Ok(output) => {
                self.batch.push(output);
                Ok(())
            }
            Err(err) => Err(Error::Transformation(format!("{err}"))),
        }
    }
}

impl<T> Stream for ConditionalPeptideStream<T>
where
    T: IsPeptidoformTransformation,
{
    type Item = Result<Vec<T::Output>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        'polling_loop: loop {
            // Terminal states drain the partial batch before reporting themselves, so a
            // condition's last rows are never stranded in the buffer.
            if this.inner_done || this.pending_error.is_some() {
                if !this.batch.is_empty() {
                    return Poll::Ready(Some(Ok(this.take_batch())));
                }
                if let Some(err) = this.pending_error.take() {
                    return Poll::Ready(Some(Err(err)));
                }
                return Poll::Ready(None);
            }

            match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(peptide))) => {
                    this.rows += 1;
                    // Attribute the row to the conditions it satisfies. For an unbatched
                    // group this is the single condition's leftover, non-SQL-able filters;
                    // for a disjunction it also decides which of the OR-ed conditions
                    // actually claim the row.
                    this.group.accepting(&peptide, &mut this.matching);

                    if this.resolve_modification {
                        // Indexed rather than iterated: `accept` needs `&mut self`, which a
                        // live iterator over `this.matching` would block.
                        for i in 0..this.matching.len() {
                            let index = this.matching[i];
                            // The returned `Vec` is owned, so the borrow of `group` ends
                            // here and `accept` can take `&mut self` below.
                            let peptidoforms =
                                this.group.conditions[index].modify_peptide(&peptide);
                            for peptidoform in peptidoforms {
                                if let Err(err) = this.accept(peptidoform) {
                                    this.pending_error = Some(err);
                                    continue 'polling_loop;
                                }
                            }
                        }
                    } else if !this.matching.is_empty() {
                        // Canonical-only output carries no PTM annotation, so it is the same
                        // peptidoform whichever condition claimed the row — emit it once.
                        // (Before batching, two overlapping conditions each had their own
                        // statement, so the row came back twice and the distinct filter
                        // collapsed it; now it comes back once to begin with.)
                        if let Err(err) = this.accept(Peptidoform::from(peptide)) {
                            this.pending_error = Some(err);
                            continue 'polling_loop;
                        }
                    }

                    if this.batch.len() >= OUTPUT_BATCH_SIZE {
                        return Poll::Ready(Some(Ok(this.take_batch())));
                    }
                    continue 'polling_loop;
                }
                Poll::Ready(Some(Err(err))) => {
                    this.pending_error = Some(err.into());
                    continue 'polling_loop;
                }
                Poll::Ready(None) => {
                    // Statement exhausted: fold its scan time + row count into the
                    // per-search aggregate (the search logs one summary at the end).
                    // Recorded on the transition rather than where `None` is returned, so
                    // it happens exactly once even though the partial batch drains after.
                    this.inner_done = true;
                    this.agg.record_scan(
                        this.opened_at.elapsed().as_micros() as u64,
                        this.rows,
                        this.group.len() as u64,
                    );
                    this.agg
                        .record_distinct(this.distinct_candidates, this.distinct_duplicates);
                    continue 'polling_loop;
                }
                // Nothing available right now: emit what we have rather than holding it
                // until the socket delivers enough rows to fill a batch. The inner poll
                // above already registered the waker, so returning `Ready` here cannot
                // lose a wake-up.
                Poll::Pending => {
                    if !this.batch.is_empty() {
                        return Poll::Ready(Some(Ok(this.take_batch())));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

/// `MultiTask` search strategy: runs one concurrent DB query per [`ConditionGroup`]
/// (bounded by `concurrent_selects`), each driven by its own spawned task so the
/// per-row CPU work (decode, condition matching, PTM expansion) is parallelized across
/// OS threads rather than polled cooperatively on a single task, and merges their rows
/// into a single stream, applying the non-SQL-able filters (e.g. distinctness) as
/// results arrive.
pub struct FallibleMatchingPeptideStream<T: IsPeptidoformTransformation> {
    /// Receives batches produced by the spawned per-statement tasks below.
    rx: mpsc::UnboundedReceiver<Result<Vec<T::Output>, Error>>,
    /// Owns the per-statement tasks; dropping it (e.g. the caller abandoning the
    /// search) aborts any still-running tasks. Never read directly — held only for
    /// this cancel-on-drop side effect.
    _tasks: JoinSet<()>,
    matching_peptide_metric: String,
    matching_peptide_counter: Counter,
    // ── timing diagnostics ──
    started_at: std::time::Instant,
    total_conditions: usize,
    total_statements: usize,
    done_logged: bool,
    agg: Arc<SearchTimingAgg>,
}

impl<T> FallibleMatchingPeptideStream<T>
where
    T: IsPeptidoformTransformation,
{
    /// Builds the stream and spawns one task per [`ConditionGroup`], each gated by a
    /// shared [`Semaphore`] (bound = `concurrent_selects`) so at most that many DB
    /// queries/scans run at once — the same cap enforced today, but each task now runs
    /// its row decode / condition matching / PTM expansion on its own OS thread instead
    /// of all conditions being polled cooperatively on one task.
    pub async fn new(
        client: Arc<Client>,
        selection: &'static PeptideColumnSelection,
        is_distinct: bool,
        // Global SQL filters, e.g. review or taxonomy condition
        sql_filters: FilterPipeline<Peptide>,
        groups: VecDeque<ConditionGroup>,
        resolve_modifications: bool,
        concurrent_selects: NonZeroUsize,
    ) -> Result<Self, Error> {
        for filter in sql_filters.iter() {
            if !filter.is_sqlable() {
                return Err(Error::NonSqlAbleFilter(format!("{filter}")));
            }
        }
        let sql_filters = Arc::new(sql_filters);

        let distinct_filter: Option<Arc<DashSet<u128>>> = if is_distinct {
            Some(Arc::new(DashSet::with_capacity(300_000)))
        } else {
            None
        };

        let total_statements = groups.len();
        let total_conditions = groups.iter().map(ConditionGroup::len).sum();
        let concurrent_selects = concurrent_selects.get();
        tracing::info!(
            target: "search_timing",
            total_conditions,
            total_statements,
            concurrent_selects,
            "search started (MultiTask)"
        );
        let agg = Arc::new(SearchTimingAgg::default());
        let semaphore = Arc::new(Semaphore::new(concurrent_selects));
        let (tx, rx) = mpsc::unbounded_channel::<Result<Vec<T::Output>, Error>>();

        let mut tasks = JoinSet::new();
        for group in groups {
            let client = client.clone();
            let sql_filters = sql_filters.clone();
            let agg = agg.clone();
            let semaphore = semaphore.clone();
            let tx = tx.clone();
            let distinct_filter = distinct_filter.clone();
            tasks.spawn(async move {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => return, // semaphore never closed in practice
                };
                match ConditionalPeptideStream::<T>::new(
                    client,
                    selection,
                    group,
                    sql_filters,
                    resolve_modifications,
                    agg,
                    distinct_filter,
                )
                .await
                {
                    Ok(mut stream) => {
                        while let Some(item) = stream.next().await {
                            if tx.send(item).is_err() {
                                break; // receiver dropped — search abandoned
                            }
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err));
                    }
                }
            });
        }
        // Drop our own sender so `rx` observes `None` once every spawned task's clone
        // (and thus every task) has finished.
        drop(tx);

        let matching_peptide_metric = format!(
            "{}:{}",
            MATCHING_PEPTIDE_METRIC,
            // TODO: Think of a better way to generate the node ID
            uuid::Uuid::now_v1(&[
                resolve_modifications as u8,
                total_conditions as u8,
                is_distinct as u8,
                resolve_modifications as u8,
                total_conditions as u8,
                is_distinct as u8
            ])
        );
        let matching_peptide_counter = counter!(matching_peptide_metric.clone());

        Ok(Self {
            rx,
            _tasks: tasks,
            matching_peptide_metric,
            matching_peptide_counter,
            started_at: std::time::Instant::now(),
            total_conditions,
            total_statements,
            done_logged: false,
            agg,
        })
    }

    pub fn matching_peptide_metric(&self) -> &str {
        &self.matching_peptide_metric
    }
}

impl<T> Stream for FallibleMatchingPeptideStream<T>
where
    T: IsPeptidoformTransformation,
{
    type Item = Result<Vec<T::Output>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Every condition's spawned task streams its batches into `rx`; once all tasks
        // finish, all sender clones drop and `rx` yields `None`.
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(peptidoforms))) => {
                this.matching_peptide_counter
                    .increment(peptidoforms.len() as u64);
                Poll::Ready(Some(Ok(peptidoforms)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => {
                if !this.done_logged {
                    this.done_logged = true;
                    // Setup/scan are timed per statement, so the means divide by statements.
                    let stmts = this.agg.statements.load(Ordering::Relaxed).max(1);
                    tracing::info!(
                        target: "search_timing",
                        total_us = this.started_at.elapsed().as_micros(),
                        total_conditions = this.total_conditions,
                        total_statements = this.total_statements,
                        conditions_max = this.agg.conditions_max.load(Ordering::Relaxed),
                        setup_us_mean = this.agg.setup_us_sum.load(Ordering::Relaxed) / stmts,
                        setup_us_max = this.agg.setup_us_max.load(Ordering::Relaxed),
                        scan_us_mean = this.agg.scan_us_sum.load(Ordering::Relaxed) / stmts,
                        scan_us_max = this.agg.scan_us_max.load(Ordering::Relaxed),
                        partitions_max = this.agg.partitions_max.load(Ordering::Relaxed),
                        rows = this.agg.rows.load(Ordering::Relaxed),
                        distinct_candidates =
                            this.agg.distinct_candidates.load(Ordering::Relaxed),
                        distinct_duplicates =
                            this.agg.distinct_duplicates.load(Ordering::Relaxed),
                        "search finished (MultiTask)"
                    );
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T> Drop for FallibleMatchingPeptideStream<T>
where
    T: IsPeptidoformTransformation,
{
    fn drop(&mut self) {
        // If the stream is dropped before reaching completion (Step 3), the search was
        // abandoned — almost always the HTTP client disconnecting / timing out. These
        // never hit the "search finished" log, which would otherwise bias measurements
        // toward only the searches fast enough to complete. Log them as abandoned.
        if !self.done_logged {
            let stmts = self.agg.statements.load(Ordering::Relaxed).max(1);
            tracing::warn!(
                target: "search_timing",
                total_us = self.started_at.elapsed().as_micros(),
                total_conditions = self.total_conditions,
                total_statements = self.total_statements,
                completed_conditions = self.agg.conditions.load(Ordering::Relaxed),
                completed_statements = self.agg.statements.load(Ordering::Relaxed),
                setup_us_mean = self.agg.setup_us_sum.load(Ordering::Relaxed) / stmts,
                setup_us_max = self.agg.setup_us_max.load(Ordering::Relaxed),
                partitions_max = self.agg.partitions_max.load(Ordering::Relaxed),
                rows = self.agg.rows.load(Ordering::Relaxed),
                distinct_candidates = self.agg.distinct_candidates.load(Ordering::Relaxed),
                distinct_duplicates = self.agg.distinct_duplicates.load(Ordering::Relaxed),
                "search abandoned before completion (client disconnect/timeout?)"
            );
        }
    }
}

/// Asynchronous filter where one task is spawned for each PTM condition.
///
pub struct PeptideSearch {
    client: Arc<Client>,
    selection: &'static PeptideColumnSelection,
    configuration: Arc<RuntimeConfiguration>,
    mass: i64,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: usize,
    is_distinct: bool,
    taxonomy_ids: Option<Vec<i32>>,
    is_reviewed: Option<bool>,
    ptm_collection: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
    resolve_modifications: bool,
    num_threads: NonZeroUsize,
}

impl PeptideSearch {
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
    /// * `is_reviewed` - Whether to filter the peptides by SwissProt or TrEMBL
    /// * `ptm_collection` - The PTM collection to use for the query
    /// * `resolve_modifications` - Whether to resolve modifications and return the modified sequences as ProForma compliant strings
    /// * `num_threads` - The number of concurrent searches
    ///
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<Client>,
        selection: &'static PeptideColumnSelection,
        configuration: Arc<RuntimeConfiguration>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        is_distinct: bool,
        taxonomy_ids: Option<Vec<i32>>,
        is_reviewed: Option<bool>,
        ptm_collection: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
        resolve_modifications: bool,
        num_threads: NonZeroUsize,
    ) -> Self {
        Self {
            client,
            selection,
            configuration,
            mass,
            lower_mass_tolerance_ppm,
            upper_mass_tolerance_ppm,
            max_variable_modifications,
            is_distinct,
            taxonomy_ids,
            is_reviewed,
            ptm_collection,
            resolve_modifications,
            num_threads,
        }
    }

    /// Splitup and sort peptide condition by partition and finalize them.
    ///
    /// # Arguments
    /// * peptide_conditions - The conditions peptides need to fulfill e.g PTMs
    /// * partition_limits - The partition limits from configuration
    /// * lower_mass_tolerance_ppm - The lower mass tolerance in ppm
    /// * upper_mass_tolerance_ppm - The upper mass tolerance in ppm
    ///
    fn split_and_sort_peptide_conditions(
        peptide_conditions: Vec<PeptideConditionBuilder>,
        mass_partitioning: &MassPartitionMap,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
    ) -> Result<Vec<PeptideCondition>, Error> {
        // Different recursion paths through `from_ptm_collection` (e.g. two static PTMs
        // that both land on the same amino-acid occurrence count via different orderings)
        // can reach the exact same effective PTM multiset: same net query_mass, same
        // filter_pipeline shape. Drop the duplicates here, before `finalize()` (partition
        // lookup) and a DB round trip get paid for each one.
        let mut seen_builders: HashSet<(i64, String)> = HashSet::new();
        let conditions: Vec<PeptideCondition> = peptide_conditions
            .into_iter()
            .filter(|condition| {
                seen_builders.insert((
                    condition.query_mass,
                    condition.filter_pipeline().to_string(),
                ))
            })
            .flat_map(|condition| {
                condition.finalize(
                    mass_partitioning,
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
                )
            })
            .collect();

        Ok(Self::merge_overlapping_conditions(conditions))
    }

    /// Groups conditions by `(partitions, filter signature)` and merges, within each group,
    /// mass windows that overlap or touch into a single condition covering their union —
    /// collapsing what would otherwise be redundant DB round trips for conditions that are
    /// SQL-identical apart from window bounds. Each absorbed condition's own PTM
    /// composition is preserved (see [`PeptideCondition::absorb`]) so annotation stays
    /// correct for rows that only belong to one of the original, narrower windows.
    fn merge_overlapping_conditions(conditions: Vec<PeptideCondition>) -> Vec<PeptideCondition> {
        let mut groups: HashMap<(Vec<i64>, String), Vec<PeptideCondition>> = HashMap::new();
        for condition in conditions {
            let key = (
                condition.partitions.clone(),
                condition.filter_pipeline.to_string(),
            );
            groups.entry(key).or_default().push(condition);
        }

        let mut merged = Vec::with_capacity(groups.len());
        for (_, mut group) in groups {
            group.sort_by_key(|condition| condition.lower_mass);
            let mut group = group.into_iter();
            let Some(mut current) = group.next() else {
                continue;
            };
            for next in group {
                if next.lower_mass <= current.upper_mass {
                    current = current.absorb(next);
                } else {
                    merged.push(current);
                    current = next;
                }
            }
            merged.push(current);
        }
        merged
    }

    /// Packs conditions into as few statements as possible by OR-ing together the ones whose
    /// partitions live on the same Citus shard.
    ///
    /// The cost of a statement is dominated by the columnar chunk-group metadata scan of the
    /// shard it lands on, and that scan is paid once per task — so N conditions on one shard
    /// cost N metadata scans as N statements but only one as a single disjunction. With ~800k
    /// partitions over 1024 shards the expected packing factor is the partitions-per-shard
    /// ratio, around 3.6 for a large PTM search.
    ///
    /// Falls back to one statement per condition when the shard mapping is unavailable (see
    /// [`crate::shard_map`]), which is exactly the behaviour that preceded batching.
    ///
    /// # Arguments
    /// * `client` - Client used to resolve partitions to shards
    /// * `conditions` - Finalized conditions, each scoped to a single partition
    async fn group_conditions_by_shard(
        client: &Client,
        conditions: Vec<PeptideCondition>,
    ) -> VecDeque<ConditionGroup> {
        let partitions: Vec<i64> = conditions
            .iter()
            .flat_map(|condition| condition.partitions().iter().copied())
            .unique()
            .collect();

        let shard_by_partition = shard_map::for_client(client)
            .shards_for(client, &partitions)
            .await
            .unwrap_or_default();

        Self::pack_conditions(conditions, &shard_by_partition)
    }

    /// The pure half of [`Self::group_conditions_by_shard`]: given a partition → shard map,
    /// packs the conditions into statements.
    ///
    /// A condition is only batched when *all* of its partitions resolve to one shard;
    /// anything else gets a statement of its own, so an incomplete map only costs batching,
    /// never correctness.
    ///
    /// Within a shard, two conditions may only share a statement if they name the same
    /// partitions or their mass windows are disjoint. That invariant is what lets a returned
    /// row be attributed to the right disjunct from its mass alone: same-partition
    /// conditions with overlapping windows are genuinely both applicable (competing PTM
    /// hypotheses, exactly as when each had its own statement), while different-partition
    /// conditions are kept apart so a row can never be credited to a partition it does not
    /// live in.
    fn pack_conditions(
        conditions: Vec<PeptideCondition>,
        shard_by_partition: &HashMap<i64, i64>,
    ) -> VecDeque<ConditionGroup> {
        let mut by_shard: HashMap<i64, Vec<PeptideCondition>> = HashMap::new();
        let mut groups: VecDeque<ConditionGroup> = VecDeque::new();

        for condition in conditions {
            let mut shards = condition
                .partitions()
                .iter()
                .map(|partition| shard_by_partition.get(partition));
            match shards.next().flatten() {
                Some(shard) if shards.all(|other| other == Some(shard)) => {
                    by_shard.entry(*shard).or_default().push(condition);
                }
                _ => groups.push_back(ConditionGroup {
                    conditions: vec![condition],
                }),
            }
        }

        // Sorted so the emitted statements are deterministic despite the HashMap.
        for shard in by_shard.keys().copied().sorted().collect::<Vec<_>>() {
            let mut shard_conditions = by_shard.remove(&shard).unwrap_or_default();
            shard_conditions.sort_by_key(|condition| (condition.lower_mass, condition.upper_mass));

            let mut batch: Vec<PeptideCondition> = Vec::new();
            for condition in shard_conditions {
                let overlaps_other_partition = batch.iter().any(|other| {
                    other.partitions != condition.partitions
                        && other.lower_mass <= condition.upper_mass
                        && condition.lower_mass <= other.upper_mass
                });
                if !batch.is_empty()
                    && (overlaps_other_partition || batch.len() >= MAX_CONDITIONS_PER_STATEMENT)
                {
                    groups.push_back(ConditionGroup {
                        conditions: std::mem::take(&mut batch),
                    });
                }
                batch.push(condition);
            }
            if !batch.is_empty() {
                groups.push_back(ConditionGroup { conditions: batch });
            }
        }

        groups
    }

    pub async fn search<T: IsPeptidoformTransformation + 'static>(
        self,
    ) -> Result<Pin<Box<FallibleMatchingPeptideStream<T>>>, Error> {
        let taxonomy_ids = self.taxonomy_ids.map(Arc::new);

        if !self.ptm_collection.is_empty() {
            let min_mass =
                self.configuration.protease().min_length().get() as i64 * GLYCINE.mono_mass();

            // Calulcate max mass as stated in PeptideCondition::from_ptm_collection() 2.3
            let largest_negative_static_ptm = self
                .ptm_collection
                .get_static_ptms()
                .iter()
                .filter(|ptm| ptm.mass_delta().is_negative())
                .fold(0_i64, |acc, ptm| acc.min(ptm.mass_delta()))
                .abs();

            let largest_negative_variable_ptm = self
                .ptm_collection
                .get_variable_ptms()
                .iter()
                .filter(|ptm| ptm.mass_delta().is_negative())
                .fold(0_i64, |acc, ptm| acc.min(ptm.mass_delta()))
                .abs();

            // Possible peptide length plus 30% "play" to account for errors
            let amino_acid_average = AminoAcid::canonical()
                .iter()
                .map(|aa| aa.mono_mass())
                .sum::<i64>()
                / AminoAcid::canonical().len() as i64;
            let possible_peptide_length = ((self.mass / amino_acid_average) as f64 * 1.3) as i64;

            let max_mass = self.mass
                + (largest_negative_static_ptm * possible_peptide_length)
                + (largest_negative_variable_ptm * possible_peptide_length);

            let sorted_ptm_conditions = Self::split_and_sort_peptide_conditions(
                PeptideConditionBuilder::from_ptm_collection(
                    &self.ptm_collection,
                    self.mass,
                    min_mass,
                    max_mass,
                    self.max_variable_modifications,
                ),
                self.configuration.mass_partitioning(),
                self.lower_mass_tolerance_ppm,
                self.upper_mass_tolerance_ppm,
            )?;
            let groups = Self::group_conditions_by_shard(&self.client, sorted_ptm_conditions).await;

            FallibleMatchingPeptideStream::new(
                self.client,
                self.selection,
                self.is_distinct,
                FilterPipeline::new_for_general_sql_able_peptide_attributes(
                    taxonomy_ids,
                    self.is_reviewed,
                )?,
                groups,
                self.resolve_modifications,
                self.num_threads,
            )
            .await
            .map(Box::pin)
        } else {
            let conditions = PeptideConditionBuilder::new(self.mass).finalize(
                self.configuration.mass_partitioning(),
                self.lower_mass_tolerance_ppm,
                self.upper_mass_tolerance_ppm,
            );
            let groups = Self::group_conditions_by_shard(&self.client, conditions).await;

            FallibleMatchingPeptideStream::new(
                self.client,
                self.selection,
                self.is_distinct,
                FilterPipeline::new_for_general_sql_able_peptide_attributes(
                    taxonomy_ids,
                    self.is_reviewed,
                )?,
                groups,
                self.resolve_modifications,
                self.num_threads,
            )
            .await
            .map(Box::pin)
        }
    }
}

/// Peptide condition which are not querieable and need to be checked "on the fly/demand"
///
#[derive(Clone)]
pub struct PeptideConditionBuilder {
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
    excluded_amino_acids: HashSet<AminoAcidBitCode>,
    /// Lazily built, cached lookup maps for `modify_peptide` — depend only on `static_ptms` /
    /// `variable_ptms`, which are fixed once the builder is finalized, so they're computed once
    /// instead of on every row.
    modification_maps: OnceLock<ModificationMaps>,
}

/// Per-amino-acid counts of variable modifications, indexed by bit code. Used both as the
/// exact quota a condition must fill (`required`) and as the running tally during recursion
/// (`applied`); a fixed array keeps both allocation-free on the per-peptide hot path.
type VariableModCounts = [u8; MAX_AMINO_ACID_BIT_CODE];

type ModificationMaps = (
    HashMap<AminoAcidBitCode, Arc<PostTranslationalModification>>,
    HashMap<AminoAcidBitCode, Vec<Arc<PostTranslationalModification>>>,
    VariableModCounts,
    Option<Arc<[(i64, AminoAcidBitCode)]>>,
);

impl PeptideConditionBuilder {
    /// Creates a new PeptideCondition with no PTMs.
    ///
    /// # Arguments
    /// * `targeted_mass` - Mass of peptides to search for
    /// * `minimum_mass` - Minimum mass of peptides in the database. Usually 6 times Glycine
    /// * `max_variable_modifications` - Max. variable modification to apply simultaneously
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
            modification_maps: OnceLock::new(),
        }
    }

    /// Adds a static PTM to the PeptideCondition.
    ///
    pub fn add_static_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = ptm.mass_delta();
        if mass_delta_int > self.query_mass {
            return false;
        }

        self.static_ptms.push(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    /// Adds a variable PTM to the PeptideCondition.
    pub fn add_variable_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = ptm.mass_delta();
        if mass_delta_int > self.query_mass {
            return false;
        }

        self.variable_ptms.push(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    /// Sets the N-terminal PTM. Fails (returns false) if one is already set or the PTM's
    /// mass delta exceeds the remaining query mass.
    pub fn set_n_terminal_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = ptm.mass_delta();
        if self.n_terminal_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.n_terminal_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    /// Sets the C-terminal PTM. Fails (returns false) if one is already set or the PTM's
    /// mass delta exceeds the remaining query mass.
    pub fn set_c_terminal_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = ptm.mass_delta();
        if self.c_terminal_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.c_terminal_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    /// Sets the N-terminal bond PTM. Fails (returns false) if one is already set or the
    /// PTM's mass delta exceeds the remaining query mass.
    pub fn set_n_bond_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = ptm.mass_delta();
        if self.n_bond_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.n_bond_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    /// Sets the C-terminal bond PTM. Fails (returns false) if one is already set or the
    /// PTM's mass delta exceeds the remaining query mass.
    pub fn set_c_bond_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = ptm.mass_delta();
        // if ptm is positive but larger than the remaining mass or  smaller than the minimum mass, skip it
        // a negative delta would increase the remaining mass, so we do not check for it
        if self.c_bond_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.c_bond_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    /// Marks an amino acid as excluded, e.g. so peptides containing it can be filtered out.
    pub fn add_excluded_amino_acid(&mut self, amino_acid: &AminoAcid) {
        self.excluded_amino_acids.insert(*amino_acid.bit_code());
    }

    /// Applies the condition to the given amino acid sequence and returns every possible modified version of it
    /// in ProForma format.
    ///
    /// # Arguments
    /// * `sequence` - The amino acid sequence to apply the condition to
    ///
    pub fn modify_peptide(&self, peptide: &Peptide) -> Vec<Peptidoform> {
        let (
            static_modifications_map,
            variable_modifications_map,
            required_variable_mods,
            global_modifications,
        ) = self.modification_maps.get_or_init(|| {
            let static_map: HashMap<AminoAcidBitCode, Arc<PostTranslationalModification>> = self
                .static_ptms
                .iter()
                .map(|ptm| (*ptm.amino_acid().bit_code(), ptm.clone()))
                .collect();

            // Map for fast access to variable modifications by amino acid. Two distinct
            // PTMs on the same amino acid with the same mass_delta would otherwise make
            // the recursion below enumerate two branches that serialize to the same
            // Peptidoform (the per-position modification only stores mass_delta + bit
            // code, never PTM identity) — dedup by mass_delta here, once per condition, so
            // those branches never get generated instead of hashing them away per peptide.
            let mut variable_map: HashMap<
                AminoAcidBitCode,
                Vec<Arc<PostTranslationalModification>>,
            > = HashMap::new();
            for ptm in self.variable_ptms.iter() {
                let mods = variable_map
                    .entry(*ptm.amino_acid().bit_code())
                    .or_default();
                if !mods
                    .iter()
                    .any(|existing: &Arc<PostTranslationalModification>| {
                        existing.mass_delta() == ptm.mass_delta()
                    })
                {
                    mods.push(ptm.clone());
                }
            }

            // Exact per-residue quota this condition must fill. `variable_ptms` is a
            // multiset, and its *multiplicities* — not just its size and key set — are
            // what distinguishes one condition from another: `{S,S,T}` and `{S,T,T}`
            // have the same length and the same keys but are different hypotheses at
            // the same query mass. The recursion enforces this quota per residue so the
            // two stop generating each other's peptidoforms.
            let mut required: VariableModCounts = [0; MAX_AMINO_ACID_BIT_CODE];
            for ptm in self.variable_ptms.iter() {
                required[*ptm.amino_acid().bit_code() as usize] += 1;
            }

            // Static/fixed PTMs are identical for every peptidoform this condition ever
            // produces, so compute the deduped list once here (per condition) and share it
            // via `Arc` — `modify_peptide` used to rebuild this `Vec` on every call (once
            // per matching peptide) and every peptidoform clone deep-cloned it again.
            let global_modifications = if self.static_ptms.is_empty() {
                None
            } else {
                Some(
                    self.static_ptms
                        .iter()
                        .collect::<HashSet<_>>()
                        .iter()
                        .map(|ptm| (ptm.mass_delta(), *ptm.amino_acid().bit_code()))
                        .collect::<Arc<[(i64, AminoAcidBitCode)]>>(),
                )
            };

            (static_map, variable_map, required, global_modifications)
        });

        // Results vector to store the modified sequences. No local dedup here: the
        // variable-modification map built above is already deduped by mass_delta per amino
        // acid, and `required_variable_mods` keeps each recursion branch inside its own
        // per-residue quota, so no two branches of *this* condition can produce the same
        // Peptidoform. The global distinct filter remains the backstop across conditions.
        let mut peptidoforms: Vec<Peptidoform> = Vec::new();
        let mut applied_variable_mods: VariableModCounts = [0; MAX_AMINO_ACID_BIT_CODE];

        let mut modified_sequence = ModifiedSequence::with_capacity(peptide.len());
        let mut mass: i64 = WATER_MONO_MASS;

        if let Some(global_modifications) = global_modifications {
            modified_sequence.set_global_modifications(global_modifications.clone());
        }

        // Add n-bond if present
        if let Some(n_bond_ptm) = &self.n_bond_ptm {
            modified_sequence.set_n_terminal_bond(n_bond_ptm.mass_delta());
            mass += n_bond_ptm.mass_delta();
        }

        // The c-bond, like the n-bond and the static/global PTMs above, is constant for
        // every peptidoform this condition produces — set it once here instead of pushing
        // and truncating it on every leaf inside `end_modify_peptide`.
        if let Some(c_bond_ptm) = &self.c_bond_ptm {
            modified_sequence.set_c_terminal_bond(c_bond_ptm.mass_delta());
        }

        self.inner_modify_peptide(
            peptide,
            &mut modified_sequence,
            mass,
            static_modifications_map,
            variable_modifications_map,
            required_variable_mods,
            &mut applied_variable_mods,
            0,
            0,
            &mut peptidoforms,
        );

        // return results
        peptidoforms
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
    fn inner_modify_peptide(
        &self,
        peptide: &Peptide,
        modified_sequence: &mut ModifiedSequence,
        mut mass: i64,
        static_modifications_map: &HashMap<AminoAcidBitCode, Arc<PostTranslationalModification>>,
        variable_modifications_map: &HashMap<
            AminoAcidBitCode,
            Vec<Arc<PostTranslationalModification>>,
        >,
        required_variable_mods: &VariableModCounts,
        applied_variable_mods: &mut VariableModCounts,
        position: usize,
        applied_vmods: usize,
        peptidoforms: &mut Vec<Peptidoform>,
    ) {
        if position >= peptide.len() {
            self.end_modify_peptide(
                peptide,
                modified_sequence,
                mass,
                applied_vmods,
                peptidoforms,
            );
            return;
        }

        let residue_start = modified_sequence.residue_len();
        let mod_start = modified_sequence.position_modification_len();
        let mut is_statically_modified = false;
        modified_sequence.push_residue(peptide.sequence()[position]);
        mass += AminoAcid::by_bit_code(&peptide.sequence()[position]).mono_mass();
        if let Some(static_mod) = static_modifications_map.get(&peptide.sequence()[position]) {
            mass += static_mod.mass_delta();
            is_statically_modified = true;
        }

        // First check for n-terminal and c-terminal modifications which must be applied when present.
        if position == 0
            && !is_statically_modified
            && let Some(ptm) = self.n_terminal_ptm.as_ref()
        {
            modified_sequence.push_position_modification(position as u32, ptm.mass_delta());
            mass += ptm.mass_delta();
            self.inner_modify_peptide(
                peptide,
                modified_sequence,
                mass,
                static_modifications_map,
                variable_modifications_map,
                required_variable_mods,
                applied_variable_mods,
                position + 1,
                applied_vmods,
                peptidoforms,
            );
        } else if position == peptide.len() - 1
            && !is_statically_modified
            && let Some(ptm) = self.c_terminal_ptm.as_ref()
        {
            modified_sequence.push_position_modification(position as u32, ptm.mass_delta());
            mass += ptm.mass_delta();
            self.inner_modify_peptide(
                peptide,
                modified_sequence,
                mass,
                static_modifications_map,
                variable_modifications_map,
                required_variable_mods,
                applied_variable_mods,
                position + 1,
                applied_vmods,
                peptidoforms,
            );
        } else {
            // # Next with unmodified amino acid
            self.inner_modify_peptide(
                peptide,
                modified_sequence,
                mass,
                static_modifications_map,
                variable_modifications_map,
                required_variable_mods,
                applied_variable_mods,
                position + 1,
                applied_vmods,
                peptidoforms,
            );

            // Only branch into a modified residue while *this residue's* quota is unfilled.
            // Without the per-residue check a condition places its whole budget anywhere its
            // map allows, so `{S,S,T}` and `{S,T,T}` — same length, same keys, same query
            // mass — each emit every 3-subset of the S∪T positions and duplicate one another
            // wholesale. The quota sums to `variable_ptms.len()`, so honouring it per residue
            // also turns the `applied_vmods == variable_ptms.len()` gate in
            // `end_modify_peptide` into an exact per-residue match.
            let amino_acid = peptide.sequence()[position] as usize;
            let quota_left = applied_variable_mods[amino_acid] < required_variable_mods[amino_acid];

            if !is_statically_modified && applied_vmods < self.variable_ptms.len() && quota_left {
                // # Next with modified amino acid
                if let Some(modifications) =
                    variable_modifications_map.get(&peptide.sequence()[position])
                {
                    applied_variable_mods[amino_acid] += 1;
                    for modification in modifications.iter() {
                        modified_sequence
                            .push_position_modification(position as u32, modification.mass_delta());
                        let next_mass = mass + modification.mass_delta();
                        self.inner_modify_peptide(
                            peptide,
                            modified_sequence,
                            next_mass,
                            static_modifications_map,
                            variable_modifications_map,
                            required_variable_mods,
                            applied_variable_mods,
                            position + 1,
                            applied_vmods + 1,
                            peptidoforms,
                        );
                        modified_sequence.truncate_position_modifications(mod_start);
                    }
                    // Backtrack the tally the way `modified_sequence` is truncated, so
                    // sibling branches see this residue's quota unspent.
                    applied_variable_mods[amino_acid] -= 1;
                }
            }
        }

        modified_sequence.truncate_position_modifications(mod_start);
        modified_sequence.truncate_residues(residue_start);
    }

    /// Modifies the peptide sequence at the end by adding c-terminal to the proforma sequences.
    ///
    /// # Arguments
    /// * `modified_peptide` - The modified peptide sequence to add
    /// * `applied_vmods` - The number of variable modifications applied to the peptide
    /// * `proforma_sequences` - The vector of proforma sequences to add the modified peptide to
    ///
    fn end_modify_peptide(
        &self,
        peptide: &Peptide,
        modified_sequence: &mut ModifiedSequence,
        mut mass: i64,
        applied_vmods: usize,
        peptidoforms: &mut Vec<Peptidoform>,
    ) {
        if let Some(c_bond_ptm) = &self.c_bond_ptm {
            mass += c_bond_ptm.mass_delta();
        }
        // If the number of applied variable modifications not equals the number of variable PTMs,
        // this condition is not fully applied
        if applied_vmods == self.variable_ptms.len() {
            peptidoforms.push(Peptidoform::new(
                modified_sequence.clone(),
                mass,
                peptide.protein_ids_arc(),
                peptide.unique_taxonomy_ids_arc(),
                peptide.non_unique_taxonomy_ids_arc(),
                peptide.is_swiss_prot(),
                peptide.is_trembl(),
            ));
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
    ) -> Vec<PeptideConditionBuilder> {
        if ptm_collection.is_empty() {
            return Vec::new();
        }

        let mut resulting_conditions: Vec<PeptideConditionBuilder> = Vec::new();

        // Handle no modifications (which excludes all static modifications)
        let mut condition = PeptideConditionBuilder::new(targeted_mass);
        for static_ptm in ptm_collection.get_static_ptms() {
            condition.add_excluded_amino_acid(static_ptm.amino_acid());
        }
        resulting_conditions.push(condition);

        // static modifications
        let condition = PeptideConditionBuilder::new(targeted_mass);
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
        mut condition: PeptideConditionBuilder,
        modification_position: usize,
        resulting_conditions: &mut Vec<PeptideConditionBuilder>,
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
        mut condition: PeptideConditionBuilder,
        modification_position: usize,
        resulting_conditions: &mut Vec<PeptideConditionBuilder>,
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

    /// Finalizes the PeptideCondition by calculating the filter functions based on the given modifications.
    ///
    /// # Arguments
    /// * `peptide_condition` - The PeptideCondition to finalize
    ///
    fn filter_pipeline(&self) -> FilterPipeline<Peptide> {
        let mut filter_functions: Vec<Box<dyn FilterFunction<Peptide>>> = Vec::with_capacity(
            self.static_ptms.len() + self.variable_ptms.len() + self.excluded_amino_acids.len() + 2, // N-terminal and C-terminal PTM
        );

        for excluded_aa in self.excluded_amino_acids.iter().sorted() {
            filter_functions.push(Box::new(NoOccurrencesFilterFunction {
                amino_acid: *excluded_aa,
            }));
        }

        let mut statically_modified_amino_acid_counts: HashMap<AminoAcidBitCode, u8> =
            HashMap::new();
        for ptm in self.static_ptms.iter() {
            statically_modified_amino_acid_counts
                .entry(*ptm.amino_acid().bit_code())
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

        let mut variable_modified_amino_acid_counts: HashMap<AminoAcidBitCode, u8> = HashMap::new();
        for ptm in self.variable_ptms.iter() {
            variable_modified_amino_acid_counts
                .entry(*ptm.amino_acid().bit_code())
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }

        if let Some(ptm) = &self.n_terminal_ptm {
            // N-terminal PTM is treated as variable modification
            variable_modified_amino_acid_counts
                .entry(*ptm.amino_acid().bit_code())
                .and_modify(|count| *count += 1)
                .or_insert(1);

            filter_functions.push(Box::new(StartsWithFilterFunction {
                amino_acid: *ptm.amino_acid().bit_code(),
            }));
        }

        if let Some(ptm) = &self.c_terminal_ptm {
            // N-terminal PTM is treated as variable modification
            variable_modified_amino_acid_counts
                .entry(*ptm.amino_acid().bit_code())
                .and_modify(|count| *count += 1)
                .or_insert(1);

            filter_functions.push(Box::new(EndsWithFilterFunction {
                amino_acid: *ptm.amino_acid().bit_code(),
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

        FilterPipeline::new(filter_functions)
    }

    /// Computes the ppm tolerance window around the (PTM-reduced) query mass and emits one
    /// [`PeptideCondition`] per distinct partition overlapping that window, each scoped to
    /// a single partition so its DB query stays a single-shard range scan.
    ///
    /// # Arguments
    /// * `partitioning` - Mass-to-partition map from the stored [`crate::configuration::RuntimeConfiguration`]
    /// * `lower_tolerance_ppm` - Lower mass tolerance in ppm
    /// * `upper_tolerance_ppm` - Upper mass tolerance in ppm
    ///
    pub fn finalize(
        &self,
        partitioning: &MassPartitionMap,
        lower_tolerance_ppm: i64,
        upper_tolerance_ppm: i64,
    ) -> Vec<PeptideCondition> {
        let lower_mass = self.query_mass - (self.query_mass / 1_000_000 * lower_tolerance_ppm);
        let upper_mass = self.query_mass + (self.query_mass / 1_000_000 * upper_tolerance_ppm);

        // Emit ONE range condition per distinct partition overlapping the window.
        //
        // History: the original code emitted one condition (one DB query) per distinct
        // stored *mass* in the window — for a relative ppm tolerance that count grows with
        // mass and blew up into 100k+ point-queries. Collapsing the whole window into a
        // single `partition = ANY([all partitions])` query fixed the count but fanned each
        // query out to up to ~86 shards, exhausting worker connections under load.
        //
        // One query per partition keeps the query as the validated, single-shard
        // `partition = $p AND mass BETWEEN lo AND hi` shape (Task Count 1, columnar
        // chunk-group pruned) while still collapsing the per-mass fan-out: the count is
        // the number of distinct partitions in the window, not distinct masses.
        //
        // The map stores one mass range per partition, so this already yields each overlapping
        // partition exactly once — no dedup needed.
        partitioning
            .partitions_by_mass_range(lower_mass, upper_mass)
            .map(|partition| PeptideCondition {
                partitions: vec![partition],
                lower_mass,
                upper_mass,
                inner: self.clone(),
                filter_pipeline: Self::filter_pipeline(self),
                members: Vec::new(),
            })
            .collect()
    }
}

impl Display for PeptideConditionBuilder {
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
            .map(|ptm| format!("[{}]@{}", ptm.mass_delta(), ptm.amino_acid().code()))
            .join(", ");
        let variable_mods = self
            .variable_ptms
            .iter()
            .map(|ptm| format!("v[{}]@{}", ptm.mass_delta(), ptm.amino_acid().code()))
            .join(", ");
        let n_bind_mod = match &self.n_bond_ptm {
            Some(ptm) => format!("[{}]-", ptm.mass_delta()),
            None => String::new(),
        };
        let c_bind_mod = match &self.c_bond_ptm {
            Some(ptm) => format!("-[{}]", ptm.mass_delta()),
            None => String::new(),
        };
        let n_terminal_mod = match &self.n_bond_ptm {
            Some(ptm) => format!("cterm{}@{}", ptm.mass_delta(), ptm.amino_acid().code()),
            None => String::new(),
        };
        let c_terminal_mod = match &self.c_bond_ptm {
            Some(ptm) => format!("nterm{}@{}", ptm.mass_delta(), ptm.amino_acid().code()),
            None => String::new(),
        };

        write!(
            f,
            "PeptideCondition: '<{static_mods}>{n_bind_mod}{n_terminal_mod}{variable_mods}{c_terminal_mod}{c_bind_mod}' @ {} Da",
            mass_to_float(self.query_mass),
        )
    }
}

impl Drop for PeptideConditionBuilder {
    fn drop(&mut self) {
        while let Some(ptm) = self.static_ptms.pop() {
            drop(ptm);
        }
        while let Some(ptm) = self.variable_ptms.pop() {
            drop(ptm);
        }
        if let Some(ptm) = self.n_terminal_ptm.take() {
            drop(ptm);
        }
        if let Some(ptm) = self.c_terminal_ptm.take() {
            drop(ptm);
        }
        if let Some(ptm) = self.n_bond_ptm.take() {
            drop(ptm);
        }
        if let Some(ptm) = self.c_bond_ptm.take() {
            drop(ptm);
        }
        self.excluded_amino_acids.clear();
        self.excluded_amino_acids.shrink_to(0);
    }
}

/// A single, DB-queryable condition scoped to one partition: a mass range plus the
/// filter functions produced from its originating [`PeptideConditionBuilder`]'s PTMs.
pub struct PeptideCondition {
    /// All distinct partitions overlapping `[lower_mass, upper_mass]`.
    partitions: Vec<i64>,
    /// Inclusive lower/upper mass bounds of the ppm window for this PTM combination.
    /// The DB filter is a single range scan (`mass >= lower AND mass <= upper`) over
    /// `partitions`, replacing the previous one-equality-query-per-distinct-mass fan-out.
    lower_mass: i64,
    upper_mass: i64,
    inner: PeptideConditionBuilder,
    /// Filter functions the peptide has to pass before it is returned
    filter_pipeline: FilterPipeline<Peptide>,
    /// Populated only when this condition absorbed one or more other same-partition,
    /// same-filter-signature conditions whose ppm windows overlapped this one's (see
    /// [`Search::split_and_sort_peptide_conditions`]). Each entry is an absorbed
    /// condition's own (narrower) `(lower_mass, upper_mass)` window plus its own PTM
    /// composition — the merge only widens the shared SQL fetch, it never conflates which
    /// PTM hypothesis actually explains a given row, so `modify_peptide` still picks the
    /// member whose window contains the row's real mass. Empty for an unmerged condition.
    members: Vec<(i64, i64, PeptideConditionBuilder)>,
}

impl PeptideCondition {
    /// Returns true if the peptide passes this condition's filter pipeline (any error is
    /// treated as a non-match). The filter pipeline is shared across all absorbed
    /// `members` by construction (they were only merged because it's identical), so this
    /// never needs to pick a specific member.
    pub fn is_match(&self, peptide: &Peptide) -> bool {
        self.filter_pipeline.is_match(peptide).unwrap_or(false)
    }

    /// Whether `peptide` satisfies this condition: its ppm mass window and its filter
    /// pipeline.
    ///
    /// What the pipeline still contains depends on how the condition was queried. Alone in
    /// a statement, its SQL-able filters were dropped after being inlined into the `WHERE`
    /// clause and only the leftovers remain to be checked. OR-ed together with others, the
    /// SQL guarantees only that *some* disjunct matched, so the pipeline was left intact and
    /// this re-derives whether this particular condition is one of them.
    pub fn accepts(&self, peptide: &Peptide) -> bool {
        let mass = peptide.mass();
        mass >= self.lower_mass && mass <= self.upper_mass && self.is_match(peptide)
    }

    /// Applies this condition's PTMs to `peptide`, returning every resulting [`Peptidoform`].
    /// When this condition absorbed other conditions during merging, picks the member whose
    /// own `(lower_mass, upper_mass)` window contains `peptide`'s mass, so the correct PTM
    /// composition is used for annotation even though the SQL fetch used the widened union
    /// range. Falls back to `inner` (this condition's own PTMs) when unmerged.
    pub fn modify_peptide(&self, peptide: &Peptide) -> Vec<Peptidoform> {
        if self.members.is_empty() {
            return self.inner.modify_peptide(peptide);
        }
        let mass = peptide.mass();
        self.members
            .iter()
            .filter(|(lower, upper, _)| mass >= *lower && mass <= *upper)
            .flat_map(|(_, _, inner)| inner.modify_peptide(peptide))
            .collect()
    }

    /// Partitions this condition's mass range overlaps (populated by `finalize` with
    /// exactly one partition).
    pub fn partitions(&self) -> &Vec<i64> {
        &self.partitions
    }

    /// Inclusive lower bound of the ppm tolerance mass window (integer-scaled mass).
    pub fn lower_mass(&self) -> i64 {
        self.lower_mass
    }

    /// Inclusive upper bound of the ppm tolerance mass window (integer-scaled mass).
    pub fn upper_mass(&self) -> i64 {
        self.upper_mass
    }

    /// Drops the filters already covered by SQL `WHERE` clauses, leaving only the ones
    /// that must still be checked in-process on returned rows.
    pub fn remove_sqlable_filters(&mut self) {
        self.filter_pipeline.remove_sqlable_filters()
    }

    /// Absorbs `other` into `self`: widens `self`'s mass window to the union of both, and
    /// records both conditions' own original windows + PTM compositions in `members` so
    /// `modify_peptide` can still pick the right one per row. Only valid when `self` and
    /// `other` share the same partitions and filter signature (checked by the caller via
    /// grouping, not re-verified here).
    fn absorb(mut self, other: PeptideCondition) -> Self {
        if self.members.is_empty() {
            self.members
                .push((self.lower_mass, self.upper_mass, self.inner.clone()));
        }
        self.lower_mass = self.lower_mass.min(other.lower_mass);
        self.upper_mass = self.upper_mass.max(other.upper_mass);
        self.members
            .push((other.lower_mass, other.upper_mass, other.inner));
        self
    }
}

// Make the inner peptide condition readable
impl Deref for PeptideCondition {
    type Target = PeptideConditionBuilder;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Display for PeptideCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PeptideCondition: {} @ {}",
            self.filter_pipeline,
            mass_to_float(self.query_mass)
        )
    }
}

#[cfg(test)]
mod tests {
    use dihardts_omicstools::proteomics::post_translational_modifications::{
        ModificationType, Position,
    };

    use crate::sequence::PeptideSequence;

    use super::*;

    #[tokio::test]
    async fn test_peptide_condition_from_ptm_collection() {
        let ptms = vec![
            Arc::new(PostTranslationalModification::new(
                "carba of C",
                AminoAcid::by_code('C').unwrap(),
                mass_to_int!(57.021464),
                ModificationType::Static,
                Position::Anywhere,
            )),
            Arc::new(PostTranslationalModification::new(
                "oxi of M",
                AminoAcid::by_code('M').unwrap(),
                mass_to_int!(15.99491),
                ModificationType::Variable,
                Position::Anywhere,
            )),
            Arc::new(PostTranslationalModification::new(
                "oxi of term M",
                AminoAcid::by_code('M').unwrap(),
                mass_to_int!(16.99491),
                ModificationType::Variable,
                Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::N),
            )),
            Arc::new(PostTranslationalModification::new(
                "oxi of term K",
                AminoAcid::by_code('K').unwrap(),
                mass_to_int!(20.3),
                ModificationType::Variable,
                Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::C),
            )),
            Arc::new(PostTranslationalModification::new(
                "something on N-bond",
                AminoAcid::by_code('X').unwrap(),
                mass_to_int!(10.0),
                ModificationType::Variable,
                Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::N),
            )),
            Arc::new(PostTranslationalModification::new(
                "something on N-bond",
                AminoAcid::by_code('X').unwrap(),
                mass_to_int!(40.3),
                ModificationType::Variable,
                Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::C),
            )),
        ];
        let ptm_collection = PTMCollection::new(ptms).unwrap();
        let mass: f64 = 839.403366202; // MFCQLAK
        let mass_int = mass_to_int!(mass);

        let conditions = PeptideConditionBuilder::from_ptm_collection(
            &ptm_collection,
            mass_int,
            AminoAcid::by_code('G').unwrap().mono_mass() * 6,
            mass_int,
            2,
        );

        // Easiest way is to check the string representation of the conditions which gives basically a unique representation of the condition
        let stringyfied_conditions = conditions
            .into_iter()
            .map(|condition| {
                // Partititoning needs to include exact queried masses
                let partitioning = MassPartitionMap::from(HashMap::<i64, Vec<i64>>::from_iter(
                    vec![(condition.query_mass, vec![1_i64])].into_iter(),
                ));
                format!(
                    "{}",
                    condition.finalize(&partitioning, 0, 0).first().unwrap()
                )
            })
            .collect::<HashSet<_>>();

        let test_file_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data")
            .join("finalized_peptide_condition.txt");

        let expected_conditions = std::fs::read_to_string(test_file_path)
            .unwrap()
            .split("\n")
            .map(|line| line.to_string())
            .collect::<HashSet<_>>();

        // assert_eq!(stringyfied_conditions.len(), expected_conditions.len());

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
        let mass_int = mass_to_int!(mass);

        let peptide = Peptide::new(
            PeptideSequence::try_from(sequence).unwrap(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            false,
            false,
        );

        let carbamidomethylation_c = Arc::new(PostTranslationalModification::new(
            "carba of C",
            AminoAcid::by_code('C').unwrap(),
            mass_to_int!(57.021464),
            ModificationType::Static,
            Position::Anywhere,
        ));

        let oxidation_m = Arc::new(PostTranslationalModification::new(
            "oxi of M",
            AminoAcid::by_code('M').unwrap(),
            mass_to_int!(15.99491),
            ModificationType::Variable,
            Position::Anywhere,
        ));

        let something_terminal_m = Arc::new(PostTranslationalModification::new(
            "oxi of term M",
            AminoAcid::by_code('M').unwrap(),
            mass_to_int!(16.99491),
            ModificationType::Variable,
            Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::N),
        ));

        let something_terminal_r = Arc::new(PostTranslationalModification::new(
            "oxi of term R",
            AminoAcid::by_code('R').unwrap(),
            mass_to_int!(20.3),
            ModificationType::Variable,
            Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::C),
        ));

        let something_bond_n = Arc::new(PostTranslationalModification::new(
            "something on N-bond",
            AminoAcid::by_code('X').unwrap(),
            mass_to_int!(10.0),
            ModificationType::Variable,
            Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::N),
        ));

        let something_bond_c = Arc::new(PostTranslationalModification::new(
            "something on N-bond",
            AminoAcid::by_code('X').unwrap(),
            mass_to_int!(40.3),
            ModificationType::Variable,
            Position::Terminus(dihardts_omicstools::proteomics::peptide::Terminus::C),
        ));

        let mut condition = PeptideConditionBuilder::new(mass_int);
        condition.add_static_ptm(carbamidomethylation_c.clone());
        condition.add_static_ptm(carbamidomethylation_c.clone());
        condition.add_variable_ptm(oxidation_m.clone());

        // Partititoning needs to include exact queried masses
        let partitioning = MassPartitionMap::from(HashMap::from_iter(vec![(
            condition.query_mass,
            vec![1_i64],
        )]));
        let mut finalized_condition = condition.finalize(&partitioning, 0, 0).pop().unwrap();

        assert!(finalized_condition.is_match(&peptide));

        let mut modified_sequences = condition
            .modify_peptide(&peptide)
            .into_iter()
            .map(|pep| pep.sequence().to_string())
            .collect::<Vec<_>>();
        modified_sequences.sort();
        assert_eq!(
            modified_sequences.as_slice(),
            [
                "<[+57.021464]@C>MFCQLAKTCPVQLWVDM[+15.99491]STPPPGTRVR",
                "<[+57.021464]@C>M[+15.99491]FCQLAKTCPVQLWVDMSTPPPGTRVR"
            ]
        );

        condition.set_n_terminal_ptm(something_terminal_m.clone());
        // Partititoning needs to include exact queried masses
        let partitioning = MassPartitionMap::from(HashMap::from_iter(vec![(
            condition.query_mass,
            vec![1_i64],
        )]));
        finalized_condition = condition
            .clone()
            .finalize(&partitioning, 0, 0)
            .pop()
            .unwrap();
        assert!(finalized_condition.is_match(&peptide));

        let mut modified_sequences = condition
            .modify_peptide(&peptide)
            .into_iter()
            .map(|pep| pep.sequence().to_string())
            .collect::<Vec<_>>();
        modified_sequences.sort();
        assert_eq!(
            modified_sequences.as_slice(),
            ["<[+57.021464]@C>M[+16.99491]FCQLAKTCPVQLWVDM[+15.99491]STPPPGTRVR",]
        );

        condition.set_c_terminal_ptm(something_terminal_r.clone());
        // Partititoning needs to include exact queried masses
        let partitioning = MassPartitionMap::from(HashMap::from_iter(vec![(
            condition.query_mass,
            vec![1_i64],
        )]));
        finalized_condition = condition
            .clone()
            .finalize(&partitioning, 0, 0)
            .pop()
            .unwrap();
        assert!(finalized_condition.is_match(&peptide));

        let mut modified_sequences = condition
            .modify_peptide(&peptide)
            .into_iter()
            .map(|pep| pep.sequence().to_string())
            .collect::<Vec<_>>();
        modified_sequences.sort();
        assert_eq!(
            modified_sequences.as_slice(),
            ["<[+57.021464]@C>M[+16.99491]FCQLAKTCPVQLWVDM[+15.99491]STPPPGTRVR[+20.3]",]
        );

        condition.set_n_bond_ptm(something_bond_n.clone());
        // Partititoning needs to include exact queried masses
        let partitioning = MassPartitionMap::from(HashMap::from_iter(vec![(
            condition.query_mass,
            vec![1_i64],
        )]));
        finalized_condition = condition
            .clone()
            .finalize(&partitioning, 0, 0)
            .pop()
            .unwrap();
        assert!(finalized_condition.is_match(&peptide));

        let mut modified_sequences = condition
            .modify_peptide(&peptide)
            .into_iter()
            .map(|pep| pep.sequence().to_string())
            .collect::<Vec<_>>();
        modified_sequences.sort();
        assert_eq!(
            modified_sequences.as_slice(),
            ["<[+57.021464]@C>[+10]-M[+16.99491]FCQLAKTCPVQLWVDM[+15.99491]STPPPGTRVR[+20.3]",]
        );

        condition.set_c_bond_ptm(something_bond_c.clone());
        // Partititoning needs to include exact queried masses
        let partitioning = MassPartitionMap::from(HashMap::from_iter(vec![(
            condition.query_mass,
            vec![1_i64],
        )]));
        finalized_condition = condition
            .clone()
            .finalize(&partitioning, 0, 0)
            .pop()
            .unwrap();
        assert!(finalized_condition.is_match(&peptide));

        let mut modified_sequences = condition
            .modify_peptide(&peptide)
            .into_iter()
            .map(|pep| pep.sequence().to_string())
            .collect::<Vec<_>>();
        modified_sequences.sort();
        assert_eq!(
            modified_sequences.as_slice(),
            [
                "<[+57.021464]@C>[+10]-M[+16.99491]FCQLAKTCPVQLWVDM[+15.99491]STPPPGTRVR[+20.3]-[+40.3]",
            ]
        );
    }

    /// `split_and_sort_peptide_conditions` (the choke point both `Search` impls funnel
    /// through) must: (1) drop conditions that reach the exact same `(query_mass, filter
    /// signature)` via different `PeptideConditionBuilder`s, and (2) merge conditions whose
    /// filter signature matches and whose ppm windows overlap on the same partition, while
    /// still keeping each merged member's own window/PTM composition around.
    #[test]
    fn test_split_and_sort_peptide_conditions_dedup_and_merge() {
        let target_mass = mass_to_int!(1000.0);

        // (1) Exact duplicate: two builders applying the identical PTM reach the identical
        // (query_mass, filter signature) pair and must collapse to one condition.
        let carbamidomethylation_c = Arc::new(PostTranslationalModification::new(
            "carba of C",
            AminoAcid::by_code('C').unwrap(),
            mass_to_int!(57.021464),
            ModificationType::Static,
            Position::Anywhere,
        ));

        let mut dup_a = PeptideConditionBuilder::new(target_mass);
        assert!(dup_a.add_static_ptm(carbamidomethylation_c.clone()));
        let mut dup_b = PeptideConditionBuilder::new(target_mass);
        assert!(dup_b.add_static_ptm(carbamidomethylation_c.clone()));
        assert_eq!(dup_a.query_mass, dup_b.query_mass);

        let dedup_partitioning =
            MassPartitionMap::from(HashMap::from_iter(vec![(dup_a.query_mass, vec![1_i64])]));
        let deduped = PeptideSearch::split_and_sort_peptide_conditions(
            vec![dup_a, dup_b],
            &dedup_partitioning,
            0,
            0,
        )
        .unwrap();
        assert_eq!(
            deduped.len(),
            1,
            "identical (query_mass, filter signature) conditions must dedup to one"
        );
        assert!(deduped[0].members.is_empty());

        // (2) Two DIFFERENT static PTMs on the same residue with the same occurrence count
        // (same filter signature) but near-isobaric masses (different query_mass). With a
        // generous ppm tolerance their windows overlap on the same partition and must merge
        // into one condition, without collapsing to a single arbitrary PTM composition.
        let ptm_a = Arc::new(PostTranslationalModification::new(
            "near-isobaric a",
            AminoAcid::by_code('M').unwrap(),
            mass_to_int!(10.0001),
            ModificationType::Static,
            Position::Anywhere,
        ));
        let ptm_b = Arc::new(PostTranslationalModification::new(
            "near-isobaric b",
            AminoAcid::by_code('M').unwrap(),
            mass_to_int!(10.0002),
            ModificationType::Static,
            Position::Anywhere,
        ));

        let mut builder_a = PeptideConditionBuilder::new(target_mass);
        assert!(builder_a.add_static_ptm(ptm_a.clone()));
        let mut builder_b = PeptideConditionBuilder::new(target_mass);
        assert!(builder_b.add_static_ptm(ptm_b.clone()));
        assert_ne!(builder_a.query_mass, builder_b.query_mass);

        let merge_partitioning = MassPartitionMap::from(HashMap::from_iter(vec![
            (builder_a.query_mass, vec![1_i64]),
            (builder_b.query_mass, vec![1_i64]),
        ]));
        let merged = PeptideSearch::split_and_sort_peptide_conditions(
            vec![builder_a, builder_b],
            &merge_partitioning,
            50,
            50,
        )
        .unwrap();

        assert_eq!(
            merged.len(),
            1,
            "same-signature, overlapping-window conditions on the same partition must merge"
        );
        assert_eq!(merged[0].members.len(), 2);
    }

    /// A finalized, unmodified condition on `partition`, with a ppm window around `mass`.
    fn condition_at(mass: i64, partition: i64, ppm: i64) -> PeptideCondition {
        let partitioning =
            MassPartitionMap::from(HashMap::from_iter(vec![(mass, vec![partition])]));
        PeptideConditionBuilder::new(mass)
            .finalize(&partitioning, ppm, ppm)
            .pop()
            .unwrap()
    }

    /// Same, but carrying one static PTM so the condition has a SQL-able filter to inline.
    fn carba_condition_at(mass: i64, partition: i64) -> PeptideCondition {
        let carba = Arc::new(PostTranslationalModification::new(
            "carba of C",
            AminoAcid::by_code('C').unwrap(),
            mass_to_int!(57.021464),
            ModificationType::Static,
            Position::Anywhere,
        ));
        let mut builder = PeptideConditionBuilder::new(mass);
        assert!(builder.add_static_ptm(carba));
        let partitioning = MassPartitionMap::from(HashMap::from_iter(vec![(
            builder.query_mass,
            vec![partition],
        )]));
        builder.finalize(&partitioning, 0, 0).pop().unwrap()
    }

    /// Conditions whose partitions share a shard collapse into one statement; conditions on
    /// different shards, and conditions whose partition has no known shard, keep their own.
    #[test]
    fn test_pack_conditions_batches_by_shard() {
        let conditions = vec![
            condition_at(mass_to_int!(1000.0), 10, 0),
            condition_at(mass_to_int!(2000.0), 11, 0),
            condition_at(mass_to_int!(3000.0), 12, 0),
            condition_at(mass_to_int!(4000.0), 13, 0),
        ];
        // 10 + 11 -> shard 1, 12 -> shard 2, 13 -> unmapped.
        let shards = HashMap::from_iter(vec![(10_i64, 1_i64), (11, 1), (12, 2)]);

        let groups = PeptideSearch::pack_conditions(conditions, &shards);
        let mut sizes: Vec<usize> = groups.iter().map(ConditionGroup::len).collect();
        sizes.sort_unstable();
        assert_eq!(
            sizes,
            vec![1, 1, 2],
            "the two same-shard conditions must share a statement, the others must not"
        );
        assert_eq!(
            groups.iter().map(ConditionGroup::len).sum::<usize>(),
            4,
            "packing must not drop or duplicate conditions"
        );
    }

    /// Two conditions on the same shard but different partitions must not share a statement
    /// when their mass windows overlap: a returned row is attributed to a disjunct by mass,
    /// so an overlap would credit it to a partition it does not live in.
    #[test]
    fn test_pack_conditions_keeps_overlapping_partitions_apart() {
        let mass = mass_to_int!(1000.0);
        // Wide ppm windows around the same mass, on two partitions of the same shard.
        let conditions = vec![condition_at(mass, 10, 100), condition_at(mass, 11, 100)];
        assert!(
            conditions[0].lower_mass() <= conditions[1].upper_mass()
                && conditions[1].lower_mass() <= conditions[0].upper_mass(),
            "fixture must actually overlap"
        );
        let shards = HashMap::from_iter(vec![(10_i64, 1_i64), (11, 1)]);

        let groups = PeptideSearch::pack_conditions(conditions, &shards);
        assert_eq!(groups.len(), 2, "overlapping windows must not be OR-ed");
        assert!(groups.iter().all(|group| group.len() == 1));
    }

    /// A shard collecting more conditions than a statement may carry splits into several.
    #[test]
    fn test_pack_conditions_respects_statement_cap() {
        let count = MAX_CONDITIONS_PER_STATEMENT * 2 + 3;
        let conditions: Vec<PeptideCondition> = (0..count)
            .map(|i| condition_at(mass_to_int!(1000.0) + i as i64 * mass_to_int!(1.0), 10, 0))
            .collect();
        let shards = HashMap::from_iter(vec![(10_i64, 1_i64)]);

        let groups = PeptideSearch::pack_conditions(conditions, &shards);
        assert_eq!(groups.len(), 3);
        assert!(
            groups
                .iter()
                .all(|group| group.len() <= MAX_CONDITIONS_PER_STATEMENT)
        );
        assert_eq!(
            groups.iter().map(ConditionGroup::len).sum::<usize>(),
            count,
            "splitting must not drop conditions"
        );
    }

    /// A single-condition statement inlines its filters and drops them from the in-process
    /// pipeline; a batched one keeps them, because its SQL only proves that *some* disjunct
    /// matched and each condition still has to re-derive whether the row is its own.
    #[test]
    fn test_where_clause_shape_and_filter_retention() {
        let no_global_filters: FilterPipeline<Peptide> = FilterPipeline::new(Vec::new());

        let mut single = ConditionGroup {
            conditions: vec![carba_condition_at(mass_to_int!(1000.0), 10)],
        };
        let single_clause = single.where_clause(&no_global_filters);
        assert!(
            !single_clause.contains(" OR "),
            "an unbatched statement must stay a plain conjunction: {single_clause}"
        );
        assert!(single_clause.contains(&format!("{PARTITION_COL} = 10")));
        assert!(
            single.conditions[0].filter_pipeline.is_empty(),
            "SQL-able filters must be dropped once SQL fully determines the result set"
        );

        let mut batched = ConditionGroup {
            conditions: vec![
                carba_condition_at(mass_to_int!(1000.0), 10),
                carba_condition_at(mass_to_int!(2000.0), 11),
            ],
        };
        let batched_clause = batched.where_clause(&no_global_filters);
        assert!(batched_clause.contains(") OR ("), "expected a disjunction");
        assert!(batched_clause.contains(&format!("{PARTITION_COL} = 10")));
        assert!(batched_clause.contains(&format!("{PARTITION_COL} = 11")));
        assert!(
            batched
                .conditions
                .iter()
                .all(|condition| !condition.filter_pipeline.is_empty()),
            "a batched condition must keep its filters to re-check per row"
        );
    }

    /// Row attribution inside a batched statement: each condition claims only the rows its
    /// own mass window and filters accept, so the disjunction cannot leak a row from one
    /// disjunct into another's PTM expansion.
    #[test]
    fn test_batched_group_attributes_rows_by_condition() {
        let sequence = "MFCQLAKTCPVQLWVDMSTPPPGTRVR";
        let peptide = Peptide::new(
            PeptideSequence::try_from(sequence).unwrap(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            false,
            false,
        );
        let peptide_mass = peptide.mass();

        let mut group = ConditionGroup {
            conditions: vec![
                condition_at(peptide_mass, 10, 0),
                // Far outside the peptide's mass, so it must not claim the row.
                condition_at(peptide_mass + mass_to_int!(500.0), 11, 0),
            ],
        };
        group.where_clause(&FilterPipeline::new(Vec::new()));

        let mut matching = Vec::new();
        group.accepting(&peptide, &mut matching);
        assert_eq!(
            matching,
            vec![0],
            "only the condition whose window contains the row may claim it"
        );
    }
}
