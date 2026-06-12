use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashSet;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, SelectAll, Stream, StreamExt};
use itertools::Itertools;
use metrics::{Counter, counter};
use postgres_types::ToSql;
use thiserror::Error;
use tokio_postgres::Row;

use crate::amino_acid::{AminoAcid, AminoAcidBitCode, GLYCINE};
use crate::client::OwnedRowStream;
use crate::configuration::RuntimeConfiguration;
use crate::molecules::WATER_MONO_MASS;
use crate::peptide::{IsPeptide, Peptidoform};
use crate::peptide_table::{COLUMNS, MASS_COL, PARTITION_COL, PeptideTable, TABLE_NAME};
// use crate::entities::configuration::Configuration;
// use crate::entities::peptide::MatchingPeptide;
use crate::post_translational_modification::{PTMCollection, PostTranslationalModification};
use crate::sequence::{IsSimpleSequence, ModifiedSequence, ModifiedSequencePart};
use crate::{mass::to_float as mass_to_float, peptide::Peptide};

use super::client::Client;

pub static MATCHING_PEPTIDE_METRIC: &str = "peptide_search:matching_peptides";

const CONDITION_REF_COL: &str = "condition_ref";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in peptide search: {0}")]
    Client(Box<crate::client::Error>),
    #[error("Missing condition for condition reference index {0} of {1}")]
    MissingCondition(usize, usize),
    #[error("Database error in peptide search: {0}")]
    NextPeptide(Box<tokio_postgres::Error>),
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
}

// /// Filters peptides which not are in SwissProt
// ///
// struct IsSwissProtFilterFunction;

// impl FilterFunction for IsSwissProtFilterFunction {
//     fn is_match(&self, peptide: &Peptide) -> Result<bool, Error> {
//         Ok(peptide.is_swiss_prot())
//     }
// }

// impl Display for IsSwissProtFilterFunction {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "is SwissProt")
//     }
// }

// /// Filter peptides which are not in TrEMBL
// ///
// struct IsTrEMBLFilterFunction;

// impl FilterFunction for IsTrEMBLFilterFunction {
//     fn is_match(&self, peptide: &Peptide) -> Result<bool, Error> {
//         Ok(peptide.is_trembl())
//     }
// }

// impl Display for IsTrEMBLFilterFunction {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "is TrEMBL")
//     }
// }

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
}

impl<T> Display for ThreadSafeDistinctFilterFunction<T>
where
    T: IsPeptide,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "distinct")
    }
}

// /// Filters peptides which are not in the given taxonomy IDs
// ///
// struct TaxonomyFilterFunction {
//     taxonomy_ids: Arc<Vec<i64>>,
// }

// impl FilterFunction for TaxonomyFilterFunction {
//     fn is_match(&self, peptide: &Peptide) -> Result<bool, Error> {
//         for taxonomy_id in self.taxonomy_ids.iter() {
//             if peptide.taxonomy_ids().contains(taxonomy_id) {
//                 return Ok(true);
//             }
//         }
//         Ok(false)
//     }
// }

// impl Display for TaxonomyFilterFunction {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "taxonomy in [{}]", self.taxonomy_ids.iter().join(", "))
//     }
// }

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

pub struct FilterPipeline<T: IsPeptide> {
    filter_functions: Vec<Box<dyn FilterFunction<T>>>,
}

impl<T> FilterPipeline<T>
where
    T: IsPeptide + 'static,
{
    pub fn new(filter_functions: Vec<Box<dyn FilterFunction<T>>>) -> Self {
        Self { filter_functions }
    }

    pub fn new_for_general_peptide_attributes(
        distinct: bool,
        _taxonomy_ids: Option<Arc<Vec<i32>>>,
        _proteome_ids: Option<Arc<Vec<String>>>,
        _is_reviewed: Option<bool>,
    ) -> Result<Self, Error> {
        let mut filter_function: Vec<Box<dyn FilterFunction<T>>> = Vec::new();
        if distinct {
            filter_function.push(Box::new(ThreadSafeDistinctFilterFunction {
                sequences: DashSet::with_capacity(300_000), // With an average length of 30 amino acids this should grow to about 72MB in memory
            }));
        }
        // if let Some(taxonomy_ids) = taxonomy_ids {
        //     filter_function.push(Box::new(TaxonomyFilterFunction { taxonomy_ids }));
        // }
        // if let Some(proteome_ids) = proteome_ids {
        //     filter_function.push(Box::new(ProteomeFilterFunction { proteome_ids }));
        // }
        // if let Some(is_reviewed) = is_reviewed {
        //     if is_reviewed {
        //         filter_function.push(Box::new(IsSwissProtFilterFunction {}));
        //     } else {
        //         filter_function.push(Box::new(IsTrEMBLFilterFunction {}));
        //     }
        // }
        Ok(Self::new(filter_function))
    }

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

    pub fn len(&self) -> usize {
        self.filter_functions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.filter_functions.is_empty()
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

pub trait IsFallibleMatchingPeptideStream:
    Stream<Item = Result<Vec<Peptidoform>, Error>> + Send
{
    fn matching_peptide_metric(&self) -> &str;
}

type BoxedPeptideRowStream =
    Pin<Box<dyn Stream<Item = Result<Peptide, crate::peptide_table::Error>> + Send>>;

struct ConditionalPeptideStream {
    condition: Pin<Box<PeptideCondition>>,
    inner: BoxedPeptideRowStream,
    resolve_modification: bool,
}

impl ConditionalPeptideStream {
    pub async fn new(
        client: Arc<Client>,
        condition: PeptideCondition,
        resolve_modification: bool,
    ) -> Result<Self, Error> {
        let params: Vec<Box<dyn ToSql + Sync + Send>> = vec![
            Box::new(condition.partitions().clone()),
            Box::new(condition.mass()),
        ];
        let inner: BoxedPeptideRowStream = Box::pin(
            PeptideTable::new(client)
                .select("WHERE partition = ANY($1) AND mass = $2", params)
                .await?,
        );
        Ok(Self {
            resolve_modification,
            inner,
            condition: Box::pin(condition),
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
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

pub struct FallibleMatchingPeptideStream {
    client: Arc<Client>,
    filter_pipeline: Pin<Box<FilterPipeline<Peptidoform>>>,
    #[allow(clippy::box_collection)]
    conditions: Pin<Box<VecDeque<PeptideCondition>>>,
    resolve_modifications: bool,
    concurrent_selects: usize,
    /// Active streams being polled in parallel.
    streams: SelectAll<ConditionalPeptideStream>,
    /// Futures that are currently opening new `ConditionalPeptideStream`s.
    /// Completed futures are drained into `streams` inside `poll_next`.
    pending: FuturesUnordered<BoxFuture<'static, Result<ConditionalPeptideStream, Error>>>,
    matching_peptide_metric: String,
    matching_peptide_counter: Counter,
}

impl FallibleMatchingPeptideStream {
    pub async fn new(
        client: Arc<Client>,
        filter_pipeline: FilterPipeline<Peptidoform>,
        mut conditions: VecDeque<PeptideCondition>,
        resolve_modifications: bool,
        concurrent_selects: NonZeroUsize,
    ) -> Result<Self, Error> {
        let concurrent_selects = concurrent_selects.get();
        let streams = SelectAll::<ConditionalPeptideStream>::new();
        let pending: FuturesUnordered<BoxFuture<'static, Result<ConditionalPeptideStream, Error>>> =
            FuturesUnordered::new();

        // Kick off the initial batch of stream-creation futures without blocking.
        let initial = concurrent_selects.min(conditions.len());
        for condition in conditions.drain(..initial) {
            let client_clone = client.clone();
            pending.push(Box::pin(async move {
                ConditionalPeptideStream::new(client_clone, condition, resolve_modifications).await
            }));
        }

        let matching_peptide_metric = format!(
            "{}:{}",
            MATCHING_PEPTIDE_METRIC,
            // TODO: Think of a better way to generate the node ID
            uuid::Uuid::now_v1(&[
                resolve_modifications as u8,
                conditions.len() as u8,
                filter_pipeline.len() as u8,
                resolve_modifications as u8,
                conditions.len() as u8,
                filter_pipeline.len() as u8,
            ])
        );
        let matching_peptide_counter = counter!(matching_peptide_metric.clone());

        Ok(Self {
            client,
            resolve_modifications,
            concurrent_selects,
            pending,
            streams,
            matching_peptide_metric,
            matching_peptide_counter,
            filter_pipeline: Box::pin(filter_pipeline),
            conditions: Box::pin(conditions),
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
        loop {
            // ── Step 1 ────────────────────────────────────────────────────────────
            // Drain all pending stream-creation futures that are already resolved.
            // This moves freshly-opened ConditionalPeptideStreams into the active
            // SelectAll pool.  We stop as soon as the queue is either empty or
            // still waiting (the waker is registered in both cases).
            loop {
                match this.pending.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(stream))) => this.streams.push(stream),
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                    // Empty or waiting — waker already registered by FuturesUnordered.
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }

            // ── Step 2 ────────────────────────────────────────────────────────────
            // Refill the pending queue so that
            //   active streams  +  in-flight creations  ==  concurrent_selects
            // (as long as there are remaining conditions to open).
            let mut added = false;
            while this.streams.len() + this.pending.len() < this.concurrent_selects
                && !this.conditions.is_empty()
            {
                let condition = this.conditions.pop_front().unwrap();
                let client = this.client.clone();
                let resolve = this.resolve_modifications;
                this.pending.push(Box::pin(async move {
                    ConditionalPeptideStream::new(client, condition, resolve).await
                }));
                added = true;
            }

            // If we just enqueued new futures, loop back so they get polled
            // immediately (and their wakers registered) before we decide whether
            // to wait.
            if added {
                continue;
            }

            // ── Step 3 ────────────────────────────────────────────────────────────
            // All conditions consumed and everything drained — we are done.
            if this.streams.is_empty() && this.pending.is_empty() && this.conditions.is_empty() {
                return Poll::Ready(None);
            }

            // ── Step 4 ────────────────────────────────────────────────────────────
            // No active streams yet; we are still waiting for the first batch of
            // creation futures.  The waker was registered in step 1.
            if this.streams.is_empty() {
                return Poll::Pending;
            }

            // ── Step 5 ────────────────────────────────────────────────────────────
            // Poll all active streams via SelectAll.
            match this.streams.poll_next_unpin(cx) {
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
                            return Poll::Ready(Some(Ok(peptides)));
                        }
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    }
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                // All active streams depleted — loop back to replenish from pending.
                Poll::Ready(None) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

struct PeptideWithConditionRef {
    condition_ref: usize,
    inner_peptide: Peptide,
}

impl PeptideWithConditionRef {
    pub fn condition_ref(&self) -> usize {
        self.condition_ref
    }

    pub fn inner_peptide(&self) -> &Peptide {
        &self.inner_peptide
    }
}

impl TryFrom<Row> for PeptideWithConditionRef {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let condition_ref =
            row.try_get::<_, i64>(CONDITION_REF_COL)
                .map_err(|err| Error::RowPeptideConversion(Box::new(err)))? as usize;
        Ok(Self {
            condition_ref,
            inner_peptide: Peptide::try_from(row)?,
        })
    }
}

impl From<PeptideWithConditionRef> for Peptide {
    fn from(value: PeptideWithConditionRef) -> Self {
        value.inner_peptide
    }
}

pub struct UnionAllFallibleMatchingPeptideStream {
    filter_pipeline: Pin<Box<FilterPipeline<Peptidoform>>>,
    conditions: Pin<Vec<PeptideCondition>>,
    resolve_modifications: bool,
    #[allow(clippy::box_collection)]
    row_stream: Pin<Box<OwnedRowStream>>,
    matching_peptide_metric: String,
    matching_peptide_counter: Counter,
}

impl UnionAllFallibleMatchingPeptideStream {
    pub async fn new(
        client: Arc<Client>,
        filter_pipeline: FilterPipeline<Peptidoform>,
        conditions: Vec<PeptideCondition>,
        resolve_modifications: bool,
    ) -> Result<Self, Error> {
        let params_len = conditions
            .iter()
            .fold(0, |acc, cond| acc + cond.partitions().len())
            * 2;
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::with_capacity(params_len);
        let mut placeholders: Vec<(usize, usize, usize)> = Vec::with_capacity(params_len);

        for (condition_idx, condition) in conditions.iter().enumerate() {
            for partition in condition.partitions() {
                let params_offset = params.len();
                params.push(Box::new(*partition));
                params.push(Box::new(condition.mass()));
                placeholders.push((condition_idx, params_offset + 1, params_offset + 2));
            }
        }

        let statement = placeholders.into_iter().map(|(condition_idx, parititon_placeholder, mass_placeholder)| {
            format!(
                "SELECT {condition_idx}::bigint as {CONDITION_REF_COL}, {COLUMNS} FROM {TABLE_NAME} WHERE {PARTITION_COL} = ${} AND {MASS_COL} = ${}",
                parititon_placeholder,
                mass_placeholder,
            )
        })
        .join(" UNION ALL ");

        let matching_peptide_metric = format!(
            "{}:{}",
            MATCHING_PEPTIDE_METRIC,
            // TODO: Think of a better way to generate the node ID
            uuid::Uuid::now_v1(&[
                resolve_modifications as u8,
                conditions.len() as u8,
                filter_pipeline.len() as u8,
                resolve_modifications as u8,
                conditions.len() as u8,
                filter_pipeline.len() as u8,
            ])
        );
        let matching_peptide_counter = counter!(matching_peptide_metric.clone());

        let row_stream = client.query_stream(&statement, params).await?;

        Ok(Self {
            resolve_modifications,
            matching_peptide_metric,
            matching_peptide_counter,
            filter_pipeline: Box::pin(filter_pipeline),
            conditions: Pin::new(conditions),
            row_stream: Box::pin(row_stream),
        })
    }
}

impl IsFallibleMatchingPeptideStream for UnionAllFallibleMatchingPeptideStream {
    fn matching_peptide_metric(&self) -> &str {
        &self.matching_peptide_metric
    }
}

impl Stream for UnionAllFallibleMatchingPeptideStream {
    type Item = Result<Vec<Peptidoform>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        'polling_loop: loop {
            return match Pin::new(&mut this.row_stream).poll_next(cx) {
                Poll::Ready(Some(Ok(row))) => {
                    let peptide = match PeptideWithConditionRef::try_from(row) {
                        Ok(peptide) => peptide,
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    };

                    let condition = match this.conditions.get_mut(peptide.condition_ref()) {
                        Some(condition) => condition,
                        None => {
                            return Poll::Ready(Some(Err(Error::MissingCondition(
                                peptide.condition_ref(),
                                this.conditions.len(),
                            ))));
                        }
                    };

                    if !condition.is_match(peptide.inner_peptide()) {
                        continue 'polling_loop;
                    }

                    let peptidoforms = if this.resolve_modifications {
                        condition.modify_peptide(peptide.inner_peptide())
                    } else {
                        vec![Peptidoform::from(Peptide::from(peptide))]
                    };

                    let matching_peptidoforms = peptidoforms
                        .into_iter()
                        .filter_map(|peptide| match this.filter_pipeline.is_match(&peptide) {
                            Ok(true) => Some(Ok(peptide)),
                            Ok(false) => None, // skip non-matching peptide
                            Err(err) => Some(Err(err)),
                        })
                        .collect::<Result<Vec<_>, Error>>();

                    return match matching_peptidoforms {
                        Ok(peptidoforms) => {
                            this.matching_peptide_counter
                                .increment(peptidoforms.len() as u64);
                            Poll::Ready(Some(Ok(peptidoforms)))
                        }
                        Err(err) => Poll::Ready(Some(Err(err))),
                    };
                }
                Poll::Ready(Some(Err(err))) => {
                    Poll::Ready(Some(Err(Error::NextPeptide(Box::new(err)))))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

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
    /// * `resolve_modifications` - Whether to resolve modifications and return the modified sequences as ProForma compliant strings
    /// * `num_threads` - The number of concurrent searches
    ///
    fn search(
        client: Arc<Client>,
        configuration: Arc<RuntimeConfiguration>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        distinct: bool,
        taxonomy_ids: Option<Vec<i32>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptm_collection: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
        resolve_modifications: bool,
        num_threads: NonZeroUsize,
    ) -> impl Future<Output = Result<Pin<Box<dyn IsFallibleMatchingPeptideStream>>, Error>> + Send;

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
        mass_partitioning: &HashMap<i64, Vec<i64>>,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
    ) -> Result<Vec<PeptideCondition>, Error> {
        Ok(peptide_conditions
            .into_iter()
            .flat_map(|conditions| {
                conditions.finalize(
                    mass_partitioning,
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
                )
            })
            .collect())
    }
}

/// Asynchronous filter where one task is spawned for each PTM condition.
///
pub struct MultiTaskSearch;

impl Search for MultiTaskSearch {
    async fn search(
        client: Arc<Client>,
        configuration: Arc<RuntimeConfiguration>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        distinct: bool,
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
                FilterPipeline::new_for_general_peptide_attributes(
                    distinct,
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
                FilterPipeline::new_for_general_peptide_attributes(
                    distinct,
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

/// Use of PostgreSQL's UNION ALL to concatenate all conditions
///
pub struct UnionAllSearch;

impl Search for UnionAllSearch {
    async fn search(
        client: Arc<Client>,
        configuration: Arc<RuntimeConfiguration>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        distinct: bool,
        taxonomy_ids: Option<Vec<i32>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptm_collection: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
        resolve_modifications: bool,
        _num_threads: NonZeroUsize,
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

            let sorted_ptm_conditions = Self::split_and_sort_peptide_conditions(
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
            )?;

            UnionAllFallibleMatchingPeptideStream::new(
                client,
                FilterPipeline::new_for_general_peptide_attributes(
                    distinct,
                    taxonomy_ids,
                    proteome_ids,
                    is_reviewed,
                )?,
                sorted_ptm_conditions,
                resolve_modifications,
            )
            .await
            .map(|stream| Box::pin(stream) as Pin<Box<dyn IsFallibleMatchingPeptideStream>>)
        } else {
            let conditions = PeptideConditionBuilder::new(mass).finalize(
                configuration.mass_partitioning(),
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
            );

            UnionAllFallibleMatchingPeptideStream::new(
                client,
                FilterPipeline::new_for_general_peptide_attributes(
                    distinct,
                    taxonomy_ids,
                    proteome_ids,
                    is_reviewed,
                )?,
                conditions,
                resolve_modifications,
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
}

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

    pub fn add_variable_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = ptm.mass_delta();
        if mass_delta_int > self.query_mass {
            return false;
        }

        self.variable_ptms.push(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    pub fn set_n_terminal_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = ptm.mass_delta();
        if self.n_terminal_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.n_terminal_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    pub fn set_c_terminal_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = ptm.mass_delta();
        if self.c_terminal_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.c_terminal_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

    pub fn set_n_bond_ptm(&mut self, ptm: Arc<PostTranslationalModification>) -> bool {
        let mass_delta_int = ptm.mass_delta();
        if self.n_bond_ptm.is_some() || mass_delta_int > self.query_mass {
            return false;
        }

        self.n_bond_ptm = Some(ptm);
        self.query_mass -= mass_delta_int;
        true
    }

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
        let static_modifications_map: HashMap<AminoAcidBitCode, &PostTranslationalModification> =
            self.static_ptms
                .iter()
                .map(|ptm| (*ptm.amino_acid().bit_code(), ptm.as_ref()))
                .collect();

        // Map for fast access to variable modifications by amino acid
        let mut variable_modifications_map: HashMap<
            AminoAcidBitCode,
            Vec<&PostTranslationalModification>,
        > = HashMap::new();
        for ptm in self.variable_ptms.iter() {
            variable_modifications_map
                .entry(*ptm.amino_acid().bit_code())
                .and_modify(|mods| mods.push(ptm))
                .or_insert(vec![ptm]);
        }

        // Results vector to store the modified sequences
        #[allow(clippy::mutable_key_type)]
        let mut peptidoforms: HashSet<Peptidoform> = HashSet::new();

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
            modified_sequence.clone(),
            mass,
            &static_modifications_map,
            &variable_modifications_map,
            0,
            0,
            &mut peptidoforms,
        );

        // return results
        peptidoforms.into_iter().collect::<Vec<_>>()
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
    #[allow(clippy::too_many_arguments, clippy::mutable_key_type)]
    fn inner_modify_peptide(
        &self,
        peptide: &Peptide,
        mut modified_sequence: ModifiedSequence,
        mut mass: i64,
        static_modifications_map: &HashMap<AminoAcidBitCode, &PostTranslationalModification>,
        variable_modifications_map: &HashMap<AminoAcidBitCode, Vec<&PostTranslationalModification>>,
        position: usize,
        applied_vmods: usize,
        peptidoforms: &mut HashSet<Peptidoform>,
    ) {
        if position >= peptide.len() {
            self.end_modify_peptide(modified_sequence, mass, applied_vmods, peptidoforms);
            return;
        }

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
                modified_sequence.clone(),
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
                        let mut next_modified_sequence = modified_sequence.clone();
                        next_modified_sequence.push(ModifiedSequencePart::PositionModification(
                            modification.mass_delta(),
                        ));
                        let next_mass = mass + modification.mass_delta();
                        self.inner_modify_peptide(
                            peptide,
                            next_modified_sequence,
                            next_mass,
                            static_modifications_map,
                            variable_modifications_map,
                            position + 1,
                            applied_vmods + 1,
                            peptidoforms,
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
    #[allow(clippy::mutable_key_type)]
    fn end_modify_peptide(
        &self,
        mut modified_sequence: ModifiedSequence,
        mut mass: i64,
        applied_vmods: usize,
        peptidoforms: &mut HashSet<Peptidoform>,
    ) {
        if let Some(c_bond_ptm) = &self.c_bond_ptm {
            modified_sequence.push(ModifiedSequencePart::CTerminalModification(
                c_bond_ptm.mass_delta(),
            ));
            mass += c_bond_ptm.mass_delta();
        }
        // If the number of applied variable modifications not equals the number of variable PTMs,
        // this condition is not fully applied
        if applied_vmods == self.variable_ptms.len() {
            peptidoforms.insert(Peptidoform::new(modified_sequence, mass));
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

        for excluded_aa in self.excluded_amino_acids.iter() {
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

    pub fn finalize(
        &self,
        partitioning: &HashMap<i64, Vec<i64>>,
        lower_tolerance_ppm: i64,
        upper_tolerance_ppm: i64,
    ) -> Vec<PeptideCondition> {
        let lower_mass = self.query_mass - (self.query_mass / 1_000_000 * lower_tolerance_ppm);
        let upper_mass = self.query_mass + (self.query_mass / 1_000_000 * upper_tolerance_ppm);

        partitioning
            .keys()
            .filter_map(|mass| {
                if lower_mass <= *mass && *mass <= upper_mass {
                    Some(PeptideCondition {
                        partitions: partitioning.get(mass).unwrap().clone(), // TODO catch error, pass ref
                        mass: *mass,
                        inner: self.clone(),
                        filter_pipeline: Self::filter_pipeline(self),
                    })
                } else {
                    None
                }
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

pub struct PeptideCondition {
    partitions: Vec<i64>,
    mass: i64,
    inner: PeptideConditionBuilder,
    /// Filter functions the peptide has to pass before it is returned
    filter_pipeline: FilterPipeline<Peptide>,
}

impl PeptideCondition {
    pub fn is_match(&mut self, peptide: &Peptide) -> bool {
        self.filter_pipeline.is_match(peptide).unwrap_or(false)
    }

    pub fn partitions(&self) -> &Vec<i64> {
        &self.partitions
    }

    pub fn mass(&self) -> i64 {
        self.mass
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
                let partitioning = HashMap::<i64, Vec<i64>>::from_iter(
                    vec![(condition.query_mass, vec![1_i64])].into_iter(),
                );
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
        let partitioning = HashMap::from_iter(vec![(condition.query_mass, vec![1_i64])]);
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
        let partitioning = HashMap::from_iter(vec![(condition.query_mass, vec![1_i64])]);
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
        let partitioning = HashMap::from_iter(vec![(condition.query_mass, vec![1_i64])]);
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
        let partitioning = HashMap::from_iter(vec![(condition.query_mass, vec![1_i64])]);
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
        let partitioning = HashMap::from_iter(vec![(condition.query_mass, vec![1_i64])]);
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
}
