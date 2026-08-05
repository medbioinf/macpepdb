use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Display;
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
    conditions: AtomicU64,
    /// Max number of partitions in a single condition's `partition = ANY(...)` list,
    /// i.e. the worst-case shard fan-out per range query.
    partitions_max: AtomicU64,
}

impl SearchTimingAgg {
    fn record_setup(&self, us: u64, partitions: u64) {
        self.setup_us_sum.fetch_add(us, Ordering::Relaxed);
        fetch_max(&self.setup_us_max, us);
        fetch_max(&self.partitions_max, partitions);
    }

    fn record_scan(&self, us: u64, rows: u64) {
        self.scan_us_sum.fetch_add(us, Ordering::Relaxed);
        fetch_max(&self.scan_us_max, us);
        self.rows.fetch_add(rows, Ordering::Relaxed);
        self.conditions.fetch_add(1, Ordering::Relaxed);
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

use crate::amino_acid::{AminoAcid, AminoAcidBitCode, GLYCINE};
use crate::configuration::RuntimeConfiguration;
use crate::database_build::MassPartitionMap;
use crate::molecules::WATER_MONO_MASS;
use crate::peptide::{IS_SWISS_PROT_BIT, IS_TREMBL_BIT, IsPeptide, Peptidoform};
use crate::peptide_table::{FLAGS_COLUMN, MASS_COL, PARTITION_COL, PeptideTable, TABLE_NAME};
use crate::post_translational_modification::{PTMCollection, PostTranslationalModification};
use crate::sequence::{IsSimpleSequence, ModifiedSequence, ModifiedSequencePart};
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

/// Makes sure that no peptide is returned twice
///
pub struct ThreadSafeDistinctFilterFunction<T: IsPeptide> {
    // TODO: This could be change to store ByteSequence instead to save up some memory in exchange for computational overhead for the conversion
    sequences: DashSet<T::Sequence>,
}

impl<T> FilterFunction<T> for ThreadSafeDistinctFilterFunction<T>
where
    T: IsPeptide,
{
    // Returns true if the peptide is distinct (not seen before), false otherwise.
    fn is_match(&self, peptide: &T) -> Result<bool, Error> {
        Ok(self.sequences.insert(peptide.sequence().clone()))
    }

    fn to_sql(&self, _filters: &mut Vec<String>, _params: &mut Vec<Box<dyn ToSql + Sync + Send>>) {}

    fn to_sql_literal(&self, _filters: &mut Vec<String>) {}

    fn is_sqlable(&self) -> bool {
        false
    }
}

impl<T> Display for ThreadSafeDistinctFilterFunction<T>
where
    T: IsPeptide,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "distinct")
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
    /// * `_proteome_ids` - Optional list of proteome IDs to filter by (currently not used)
    /// * `is_reviewed` - Optional boolean to filter by review status (true for SwissProt, false for TrEMBL)
    ///
    pub fn new_for_general_sql_able_peptide_attributes(
        taxonomy_ids: Option<Arc<Vec<i32>>>,
        _proteome_ids: Option<Arc<Vec<String>>>,
        is_reviewed: Option<bool>,
    ) -> Result<Self, Error> {
        let mut filter_function: Vec<Box<dyn FilterFunction<T>>> = Vec::new();
        if let Some(taxonomy_ids) = taxonomy_ids {
            filter_function.push(Box::new(TaxonomyFilterFunction { taxonomy_ids }));
        }
        // if let Some(proteome_ids) = proteome_ids {
        //     filter_function.push(Box::new(ProteomeFilterFunction { proteome_ids }));
        // }
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

/// A stream of batches of matching [`Peptidoform`]s produced by a running search, common
/// to both the `MultiTask` and `UnionAll` strategies so callers can handle them uniformly.
pub trait IsFallibleMatchingPeptideStream:
    Stream<Item = Result<Vec<Peptidoform>, Error>> + Send
{
    /// Name of the `metrics` counter this stream's matching-peptide count is reported under.
    fn matching_peptide_metric(&self) -> &str;
}

type BoxedPeptideRowStream =
    Pin<Box<dyn Stream<Item = Result<Peptide, crate::peptide_table::Error>> + Send>>;

struct ConditionalPeptideStream {
    condition: Pin<Box<PeptideCondition>>,
    inner: BoxedPeptideRowStream,
    resolve_modification: bool,
    // ── timing diagnostics ──
    agg: Arc<SearchTimingAgg>,
    opened_at: std::time::Instant,
    rows: u64,
}

impl ConditionalPeptideStream {
    /// Opens the DB query for a single [`PeptideCondition`] and wraps the resulting row
    /// stream, recording open/scan timings into `agg` as the stream is consumed.
    pub async fn new(
        client: Arc<Client>,
        mut condition: PeptideCondition,
        sql_filters: Arc<FilterPipeline<Peptide>>,
        resolve_modification: bool,
        agg: Arc<SearchTimingAgg>,
    ) -> Result<Self, Error> {
        for filter in sql_filters.iter() {
            if !filter.is_sqlable() {
                return Err(Error::NonSqlAbleFilter(format!("{filter}")));
            }
        }

        // Inline literals (no bind params): Citus prunes shards + columnar chunk groups
        // at plan time only for an inlined query — a parameterized distributed query
        // re-plans every execute (~11 ms) and cannot use a cached generic plan.
        let num_partitions = condition.partitions().len() as u64;

        let mut filters = vec![
            format!(
                "{PARTITION_COL} = ANY(ARRAY[{}]::bigint[])",
                condition.partitions().iter().join(",")
            ),
            format!("{MASS_COL} >= {}", condition.lower_mass()),
            format!("{MASS_COL} <= {}", condition.upper_mass()),
        ];
        // add peptide condition specific where clauses (this will precisly locate the peptides and reduce them most)
        for filter_fn in condition.filter_pipeline().iter() {
            filter_fn.to_sql_literal(&mut filters);
        }
        // add global where clauses
        condition.remove_sqlable_filters();

        for filter_fn in sql_filters.iter() {
            filter_fn.to_sql_literal(&mut filters);
        }

        let where_clause = format!("WHERE {}", filters.join(" AND "));

        let setup_start = std::time::Instant::now();
        let inner: BoxedPeptideRowStream = Box::pin(
            PeptideTable::new(client)
                .select_inline(&where_clause)
                .await?,
        );
        agg.record_setup(setup_start.elapsed().as_micros() as u64, num_partitions);
        Ok(Self {
            resolve_modification,
            inner,
            condition: Box::pin(condition),
            agg,
            opened_at: std::time::Instant::now(),
            rows: 0,
        })
    }
}

impl Stream for ConditionalPeptideStream {
    type Item = Result<Vec<Peptidoform>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        'polling_loop: loop {
            return match this.inner.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(peptide))) => {
                    this.rows += 1;
                    if this.condition.is_match(&peptide) {
                        let peptidoforms = if this.resolve_modification {
                            this.condition.modify_peptide(&peptide)
                        } else {
                            vec![Peptidoform::from(peptide)]
                        };

                        Poll::Ready(Some(Ok(peptidoforms)))
                    } else {
                        continue 'polling_loop;
                    }
                }
                Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
                Poll::Ready(None) => {
                    // Condition exhausted: fold its scan time + row count into the
                    // per-search aggregate (the search logs one summary at the end).
                    this.agg
                        .record_scan(this.opened_at.elapsed().as_micros() as u64, this.rows);
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

/// `MultiTask` search strategy: runs one concurrent DB query per [`PeptideCondition`]
/// (bounded by `concurrent_selects`), each driven by its own spawned task so the
/// per-row CPU work (decode, condition matching, PTM expansion) is parallelized across
/// OS threads rather than polled cooperatively on a single task, and merges their rows
/// into a single stream, applying the non-SQL-able filters (e.g. distinctness) as
/// results arrive.
pub struct FallibleMatchingPeptideStream {
    // Should only contain non sql able filters
    filter_pipeline: Pin<Box<FilterPipeline<Peptidoform>>>,
    /// Receives batches produced by the spawned per-condition tasks below.
    rx: mpsc::UnboundedReceiver<Result<Vec<Peptidoform>, Error>>,
    /// Owns the per-condition tasks; dropping it (e.g. the caller abandoning the
    /// search) aborts any still-running tasks. Never read directly — held only for
    /// this cancel-on-drop side effect.
    _tasks: JoinSet<()>,
    matching_peptide_metric: String,
    matching_peptide_counter: Counter,
    // ── timing diagnostics ──
    started_at: std::time::Instant,
    total_conditions: usize,
    done_logged: bool,
    agg: Arc<SearchTimingAgg>,
}

impl FallibleMatchingPeptideStream {
    /// Builds the stream and spawns one task per [`PeptideCondition`], each gated by a
    /// shared [`Semaphore`] (bound = `concurrent_selects`) so at most that many DB
    /// queries/scans run at once — the same cap enforced today, but each task now runs
    /// its row decode / condition matching / PTM expansion on its own OS thread instead
    /// of all conditions being polled cooperatively on one task.
    pub async fn new(
        client: Arc<Client>,
        is_distinct: bool,
        // Global SQL filters, e.g. review or taxonomy condition
        sql_filters: FilterPipeline<Peptide>,
        conditions: VecDeque<PeptideCondition>,
        resolve_modifications: bool,
        concurrent_selects: NonZeroUsize,
    ) -> Result<Self, Error> {
        for filter in sql_filters.iter() {
            if !filter.is_sqlable() {
                return Err(Error::NonSqlAbleFilter(format!("{filter}")));
            }
        }
        let sql_filters = Arc::new(sql_filters);

        // Build filter pipeline for non sqlable conditions
        let mut filters: Vec<Box<dyn FilterFunction<Peptidoform>>> = Vec::new();
        if is_distinct {
            filters.push(Box::new(ThreadSafeDistinctFilterFunction {
                sequences: DashSet::with_capacity(300_000), // With an average length of 30 amino acids this should grow to about 72MB in memory
            }));
        }
        let filter_pipeline = FilterPipeline::new(filters);

        let total_conditions = conditions.len();
        let concurrent_selects = concurrent_selects.get();
        tracing::info!(
            target: "search_timing",
            total_conditions,
            concurrent_selects,
            "search started (MultiTask)"
        );
        let agg = Arc::new(SearchTimingAgg::default());
        let semaphore = Arc::new(Semaphore::new(concurrent_selects));
        let (tx, rx) = mpsc::unbounded_channel::<Result<Vec<Peptidoform>, Error>>();

        let mut tasks = JoinSet::new();
        for condition in conditions {
            let client = client.clone();
            let sql_filters = sql_filters.clone();
            let agg = agg.clone();
            let semaphore = semaphore.clone();
            let tx = tx.clone();
            tasks.spawn(async move {
                let _permit = match semaphore.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => return, // semaphore never closed in practice
                };
                match ConditionalPeptideStream::new(
                    client,
                    condition,
                    sql_filters,
                    resolve_modifications,
                    agg,
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
                filter_pipeline.len() as u8,
                resolve_modifications as u8,
                total_conditions as u8,
                filter_pipeline.len() as u8,
            ])
        );
        let matching_peptide_counter = counter!(matching_peptide_metric.clone());

        Ok(Self {
            rx,
            _tasks: tasks,
            matching_peptide_metric,
            matching_peptide_counter,
            filter_pipeline: Box::pin(filter_pipeline),
            started_at: std::time::Instant::now(),
            total_conditions,
            done_logged: false,
            agg,
        })
    }
}

impl IsFallibleMatchingPeptideStream for FallibleMatchingPeptideStream {
    fn matching_peptide_metric(&self) -> &str {
        &self.matching_peptide_metric
    }
}

impl Stream for FallibleMatchingPeptideStream {
    type Item = Result<Vec<Peptidoform>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Every condition's spawned task streams its batches into `rx`; once all tasks
        // finish, all sender clones drop and `rx` yields `None`.
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(peptides))) => {
                let matching_peptides = peptides
                    .into_iter()
                    .filter_map(|peptide| match this.filter_pipeline.is_match(&peptide) {
                        Ok(true) => Some(Ok(peptide)),
                        Ok(false) => None, // skip non-matching peptide
                        Err(err) => Some(Err(err)),
                    })
                    .collect::<Result<Vec<_>, Error>>();

                match matching_peptides {
                    Ok(peptides) => {
                        this.matching_peptide_counter
                            .increment(peptides.len() as u64);
                        Poll::Ready(Some(Ok(peptides)))
                    }
                    Err(err) => Poll::Ready(Some(Err(err))),
                }
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => {
                if !this.done_logged {
                    this.done_logged = true;
                    let conds = this.agg.conditions.load(Ordering::Relaxed).max(1);
                    tracing::info!(
                        target: "search_timing",
                        total_us = this.started_at.elapsed().as_micros(),
                        total_conditions = this.total_conditions,
                        setup_us_mean = this.agg.setup_us_sum.load(Ordering::Relaxed) / conds,
                        setup_us_max = this.agg.setup_us_max.load(Ordering::Relaxed),
                        scan_us_mean = this.agg.scan_us_sum.load(Ordering::Relaxed) / conds,
                        scan_us_max = this.agg.scan_us_max.load(Ordering::Relaxed),
                        partitions_max = this.agg.partitions_max.load(Ordering::Relaxed),
                        rows = this.agg.rows.load(Ordering::Relaxed),
                        "search finished (MultiTask)"
                    );
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for FallibleMatchingPeptideStream {
    fn drop(&mut self) {
        // If the stream is dropped before reaching completion (Step 3), the search was
        // abandoned — almost always the HTTP client disconnecting / timing out. These
        // never hit the "search finished" log, which would otherwise bias measurements
        // toward only the searches fast enough to complete. Log them as abandoned.
        if !self.done_logged {
            let conds = self.agg.conditions.load(Ordering::Relaxed).max(1);
            tracing::warn!(
                target: "search_timing",
                total_us = self.started_at.elapsed().as_micros(),
                total_conditions = self.total_conditions,
                completed_conditions = self.agg.conditions.load(Ordering::Relaxed),
                setup_us_mean = self.agg.setup_us_sum.load(Ordering::Relaxed) / conds,
                setup_us_max = self.agg.setup_us_max.load(Ordering::Relaxed),
                partitions_max = self.agg.partitions_max.load(Ordering::Relaxed),
                rows = self.agg.rows.load(Ordering::Relaxed),
                "search abandoned before completion (client disconnect/timeout?)"
            );
        }
    }
}

/// Asynchronous filter where one task is spawned for each PTM condition.
///
pub struct PeptideSearch;

impl PeptideSearch {
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
    /// * `resolve_modifications` - Whether to resolve modifications and return the modified sequences as ProForma compliant strings
    /// * `num_threads` - The number of concurrent searches
    ///
    pub async fn search(
        client: Arc<Client>,
        configuration: Arc<RuntimeConfiguration>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        is_distinct: bool,
        taxonomy_ids: Option<Vec<i32>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptm_collection: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
        resolve_modifications: bool,
        num_threads: NonZeroUsize,
    ) -> Result<Pin<Box<dyn IsFallibleMatchingPeptideStream>>, Error> {
        let taxonomy_ids = taxonomy_ids.map(Arc::new);
        let proteome_ids = proteome_ids.map(Arc::new);

        if !ptm_collection.is_empty() {
            let min_mass = configuration.protease().min_length().get() as i64 * GLYCINE.mono_mass();

            // Calulcate max mass as stated in PeptideCondition::from_ptm_collection() 2.3
            let largest_negative_static_ptm = ptm_collection
                .get_static_ptms()
                .iter()
                .filter(|ptm| ptm.mass_delta().is_negative())
                .fold(0_i64, |acc, ptm| acc.min(ptm.mass_delta()))
                .abs();

            let largest_negative_variable_ptm = ptm_collection
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
            let possible_peptide_length = ((mass / amino_acid_average) as f64 * 1.3) as i64;

            let max_mass = mass
                + (largest_negative_static_ptm * possible_peptide_length)
                + (largest_negative_variable_ptm * possible_peptide_length);

            let sorted_ptm_conditions = VecDeque::from(Self::split_and_sort_peptide_conditions(
                PeptideConditionBuilder::from_ptm_collection(
                    &ptm_collection,
                    mass,
                    min_mass,
                    max_mass,
                    max_variable_modifications,
                ),
                configuration.mass_partitioning(),
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
            )?);

            FallibleMatchingPeptideStream::new(
                client,
                is_distinct,
                FilterPipeline::new_for_general_sql_able_peptide_attributes(
                    taxonomy_ids,
                    proteome_ids,
                    is_reviewed,
                )?,
                sorted_ptm_conditions,
                resolve_modifications,
                num_threads,
            )
            .await
            .map(|stream| Box::pin(stream) as Pin<Box<dyn IsFallibleMatchingPeptideStream>>)
        } else {
            let conditions = VecDeque::from(PeptideConditionBuilder::new(mass).finalize(
                configuration.mass_partitioning(),
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
            ));

            FallibleMatchingPeptideStream::new(
                client,
                is_distinct,
                FilterPipeline::new_for_general_sql_able_peptide_attributes(
                    taxonomy_ids,
                    proteome_ids,
                    is_reviewed,
                )?,
                conditions,
                resolve_modifications,
                num_threads,
            )
            .await
            .map(|stream| Box::pin(stream) as Pin<Box<dyn IsFallibleMatchingPeptideStream>>)
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

type ModificationMaps = (
    HashMap<AminoAcidBitCode, Arc<PostTranslationalModification>>,
    HashMap<AminoAcidBitCode, Vec<Arc<PostTranslationalModification>>>,
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
        let (static_modifications_map, variable_modifications_map) =
            self.modification_maps.get_or_init(|| {
                let static_map: HashMap<AminoAcidBitCode, Arc<PostTranslationalModification>> =
                    self.static_ptms
                        .iter()
                        .map(|ptm| (*ptm.amino_acid().bit_code(), ptm.clone()))
                        .collect();

                // Map for fast access to variable modifications by amino acid. Two distinct
                // PTMs on the same amino acid with the same mass_delta would otherwise make
                // the recursion below enumerate two branches that serialize to the same
                // Peptidoform (ModifiedSequencePart only stores mass_delta + bit_code, never
                // PTM identity) — dedup by mass_delta here, once per condition, so those
                // branches never get generated instead of hashing them away per peptide.
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

                (static_map, variable_map)
            });

        // Results vector to store the modified sequences. No local dedup here: the
        // variable-modification map built above is already deduped by mass_delta per amino
        // acid, so no two recursion branches can ever produce the same Peptidoform: the
        // global distinct filter (`ThreadSafeDistinctFilterFunction`) is the sole dedup
        // point across conditions/partitions.
        let mut peptidoforms: Vec<Peptidoform> = Vec::new();

        let mut modified_sequence = ModifiedSequence::with_capacity(peptide.len());
        let mut mass: i64 = WATER_MONO_MASS;

        if !self.static_ptms.is_empty() {
            modified_sequence.push(ModifiedSequencePart::GlobalModifications(
                self.static_ptms
                    .iter()
                    .collect::<HashSet<_>>()
                    .iter()
                    .map(|ptm| (ptm.mass_delta(), *ptm.amino_acid().bit_code()))
                    .collect(),
            ))
        }

        // Add n-bond if present
        if let Some(n_bond_ptm) = &self.n_bond_ptm {
            modified_sequence.push(ModifiedSequencePart::NTerminalModification(
                n_bond_ptm.mass_delta(),
            ));
            mass += n_bond_ptm.mass_delta();
        }

        self.inner_modify_peptide(
            peptide,
            &mut modified_sequence,
            mass,
            static_modifications_map,
            variable_modifications_map,
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

        let start_len = modified_sequence.len();
        let mut is_statically_modified = false;
        modified_sequence.push(ModifiedSequencePart::AminoAcid(
            peptide.sequence()[position],
        ));
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
            modified_sequence.push(ModifiedSequencePart::PositionModification(ptm.mass_delta()));
            mass += ptm.mass_delta();
            self.inner_modify_peptide(
                peptide,
                modified_sequence,
                mass,
                static_modifications_map,
                variable_modifications_map,
                position + 1,
                applied_vmods,
                peptidoforms,
            );
        } else if position == peptide.len() - 1
            && !is_statically_modified
            && let Some(ptm) = self.c_terminal_ptm.as_ref()
        {
            modified_sequence.push(ModifiedSequencePart::PositionModification(ptm.mass_delta()));
            mass += ptm.mass_delta();
            self.inner_modify_peptide(
                peptide,
                modified_sequence,
                mass,
                static_modifications_map,
                variable_modifications_map,
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
                position + 1,
                applied_vmods,
                peptidoforms,
            );

            if !is_statically_modified && applied_vmods < self.variable_ptms.len() {
                // # Next with modified amino acid
                if let Some(modifications) =
                    variable_modifications_map.get(&peptide.sequence()[position])
                {
                    for modification in modifications.iter() {
                        modified_sequence.push(ModifiedSequencePart::PositionModification(
                            modification.mass_delta(),
                        ));
                        let next_mass = mass + modification.mass_delta();
                        self.inner_modify_peptide(
                            peptide,
                            modified_sequence,
                            next_mass,
                            static_modifications_map,
                            variable_modifications_map,
                            position + 1,
                            applied_vmods + 1,
                            peptidoforms,
                        );
                        modified_sequence.truncate(start_len + 1);
                    }
                }
            }
        }

        modified_sequence.truncate(start_len);
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
        let start_len = modified_sequence.len();
        if let Some(c_bond_ptm) = &self.c_bond_ptm {
            modified_sequence.push(ModifiedSequencePart::CTerminalModification(
                c_bond_ptm.mass_delta(),
            ));
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
        modified_sequence.truncate(start_len);
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
    pub fn is_match(&mut self, peptide: &Peptide) -> bool {
        self.filter_pipeline.is_match(peptide).unwrap_or(false)
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
            .find(|(lower, upper, _)| mass >= *lower && mass <= *upper)
            .map(|(_, _, inner)| inner.modify_peptide(peptide))
            .unwrap_or_default()
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
}
