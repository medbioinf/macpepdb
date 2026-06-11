use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    ops::AddAssign,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicI64, AtomicUsize},
    },
    time::Duration,
};

use crossbeam::queue::ArrayQueue;
use fallible_iterator::FallibleIterator;
use futures::{Stream, StreamExt};
use postgres_types::{ToSql, Type};
use thiserror::Error;

use crate::{
    client::Client,
    database_build::IsProteinAccess,
    mass_index::MassIndex,
    peptide::{IsPeptide, Peptide},
    protease::Protease,
    sequence::{CompactSequence, PeptideSequence},
    stats_table::StatsTable,
};

pub const TABLE_NAME: &str = "peptides";

/// Rows per columnar stripe. The build COPYs exactly this many rows per partition so
/// each partition becomes one full columnar stripe — this MUST match
/// `columnar.stripe_row_limit` in `db.sql`.
pub const STRIPE_ROW_LIMIT: usize = 150_000;

/// Memory guard: flush a partition early if its buffered rows exceed this many bytes
/// (estimated via `Peptide::cql_size`), even before `STRIPE_ROW_LIMIT`. Bounds worker
/// memory when peptides map to very large protein-id lists.
const MAX_PARTITION_BYTES: usize = 256 * 1024 * 1024;

pub const PARTITION_COL: &str = "partition";

pub const MASS_COL: &str = "mass";

pub const COLUMNS: &str =
    "partition, mass, sequence, protein_ids, unique_taxonomy_ids, non_unique_taxonomy_ids";

static COPY_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("COPY {TABLE_NAME} ({COLUMNS}) FROM STDIN (FORMAT binary)"));

/// Column types for the binary COPY into `peptides`, in column order.
static COPY_TYPES: LazyLock<[Type; 6]> = LazyLock::new(|| {
    [
        Type::INT8,       // partition
        Type::INT8,       // mass
        Type::BYTEA,      // sequence (CompactSequence bytes)
        Type::BYTEA,      // protein_ids (delta+varint bytes)
        Type::INT4_ARRAY, // unique_taxonomy_ids
        Type::INT4_ARRAY, // non_unique_taxonomy_ids
    ]
});

static SELECT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT {COLUMNS} FROM {TABLE_NAME}"));

pub static PROGRESS_METRIC: &str = "peptides_table::build::progress";
pub static INSERTED_PEPTIDES_METRIC: &str = "peptides_table::build::inserted_peptides";
pub static QUEUE_METRIC: &str = "peptides_table::queue";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Client error in peptide table: {0}")]
    Client(#[from] crate::client::Error),
    #[error(
        "Peptide count not found. It should be stored in the `{}` table. Are you sure the database was build correctly?",
        StatsTable::table_name()
    )]
    CountNotFound,
    #[error("Row decoding error in peptide table: {0}")]
    Row(#[from] tokio_postgres::Error),
    #[error("IO error in peptide table: {0}")]
    Io(#[from] std::io::Error),
    #[error("Unable to join insertion task: {0}")]
    Join(String),
    #[error("Mass index error in peptide table: {0}")]
    MassIndex(Box<crate::mass_index::Error>),
    #[error("No errored thread found in peptide table, but one finished early.")]
    NoErroredThread,
    #[error("Protease error in peptide table: {0}")]
    Protease(#[from] crate::protease::Error),
    #[error("Protein access error in peptide table: {0}")]
    ProteinAccess(Box<crate::database_build::Error>),
    #[error("Peptide error in peptide table: {0}")]
    Peptide(#[from] crate::peptide::Error),
    #[error("Sequence error in peptide table: {0}")]
    Sequence(#[from] crate::sequence::Error),
    #[error("Stats table error in peptide table: {0}")]
    StatsTable(Box<crate::stats_table::Error>),
    #[error("UnipotReader error in peptide table: {0}")]
    UnprotReader(#[from] uniprot_reader::reader::Error),
}

into_thiserror_boxed!(crate::mass_index::Error, Error, MassIndex);
into_thiserror_boxed!(crate::database_build::Error, Error, ProteinAccess);
into_thiserror_boxed!(crate::stats_table::Error, Error, StatsTable);

type ConcurrentlyBuildQueue = Arc<ArrayQueue<Option<(i64, Vec<i32>)>>>;

struct NextPartitionGuard {
    next_partition: AtomicI64,
}

impl NextPartitionGuard {
    fn new() -> Self {
        Self {
            next_partition: AtomicI64::new(0),
        }
    }

    fn next_partition(&self) -> i64 {
        self.next_partition
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

pub struct PeptideTable {
    client: Arc<Client>,
}

impl PeptideTable {
    pub fn new(client: Arc<Client>) -> Self {
        Self { client }
    }

    /// Bulk-loads a partition's peptides via one binary COPY transaction. Columnar
    /// turns each transaction into a single stripe, so the whole partition buffer must
    /// go in one COPY (see the migration plan). COPY is transactional, so the whole
    /// thing is retried atomically on transient errors.
    async fn insert_batch(&self, peptides: &[Peptide]) -> Result<usize, Error> {
        if peptides.is_empty() {
            return Ok(0);
        }

        self.client
            .run_congested(|| async {
                let mut copy = self
                    .client
                    .copy_in_binary(COPY_STATEMENT.as_str(), COPY_TYPES.as_slice())
                    .await?;
                for peptide in peptides {
                    let partition = peptide
                        .partition()
                        .expect("peptide partition must be set before COPY");
                    let mass = peptide.mass();
                    let unique = peptide.unique_taxonomy_ids();
                    let non_unique = peptide.non_unique_taxonomy_ids();
                    copy.write(&[
                        &partition,
                        &mass,
                        peptide.sequence(),
                        peptide.protein_ids(),
                        &unique,
                        &non_unique,
                    ])
                    .await?;
                }
                copy.finish().await?;
                Ok::<(), crate::client::Error>(())
            })
            .await?;

        Ok(peptides.len())
    }

    /// Streams peptides matching `where_clause` (e.g. `WHERE partition = ANY($1) AND
    /// mass = $2`), binding `params` positionally.
    pub async fn select(
        &self,
        where_clause: &str,
        params: Vec<Box<dyn ToSql + Sync + Send>>,
    ) -> Result<impl Stream<Item = Result<Peptide, Error>> + Send + use<>, Error> {
        let statement = format!("{} {where_clause}", SELECT_STATEMENT.as_str());
        let stream = self.client.query_stream(&statement, params).await?;
        Ok(stream.map(|row_res| {
            row_res
                .map_err(Error::Row)
                .and_then(|row| Peptide::try_from(row).map_err(Error::from))
        }))
    }

    pub async fn count(&self) -> Result<usize, Error> {
        StatsTable::new(self.client.clone())
            .select_peptide_count()
            .await
            .map_err(Error::from)?
            .ok_or(Error::CountNotFound)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn build_concurrently(
        &self,
        protein_access: Arc<Box<dyn IsProteinAccess>>,
        skip_protein_associations: bool,
        skip_taxonomies: bool,
        protease: Arc<Protease>,
        _batch_size_limit: NonZeroUsize,
        num_threads: NonZeroUsize,
        mass_index: MassIndex,
    ) -> Result<(usize, HashMap<i64, Vec<i64>>), Error> {
        let queue: ConcurrentlyBuildQueue = Arc::new(ArrayQueue::new(num_threads.get() * 3));
        let progress_metric = Arc::new(metrics::gauge!(PROGRESS_METRIC));
        let inserted_peptides_metric = Arc::new(metrics::counter!(INSERTED_PEPTIDES_METRIC));
        let queue_metric = metrics::gauge!(QUEUE_METRIC);
        let next_partition_guard = Arc::new(NextPartitionGuard::new());
        let peptide_ctr = Arc::new(AtomicUsize::new(0));

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let protein_access = protein_access.clone();
                let protease = protease.clone();
                let queue = queue.clone();
                let client = self.client.clone();
                let progress_metric = progress_metric.clone();
                let inserted_peptides_metric = inserted_peptides_metric.clone();
                let next_partition_guard = next_partition_guard.clone();
                let peptide_ctr = peptide_ctr.clone();

                tokio::spawn(async move {
                    let peptide_table = PeptideTable::new(client.clone());
                    let mut mass_partition_map: HashMap<i64, Vec<i64>> = HashMap::new();
                    let mut peptide_buffer: Vec<Peptide> = Vec::new();
                    let mut partition_bytes: usize = 0;
                    let mut partition = next_partition_guard.next_partition();

                    // Track which masses have peptides in the current buffer.
                    let mut buffer_masses: HashSet<i64> = HashSet::new();

                    loop {
                        let (mass, protein_ids) = match queue.pop() {
                            Some(Some(entry)) => entry,
                            Some(None) => {
                                if !peptide_buffer.is_empty() {
                                    for &m in &buffer_masses {
                                        mass_partition_map.entry(m).or_default().push(partition);
                                    }
                                    tracing::debug!(
                                        "Flushing {} peptides into partition {partition} (final)",
                                        peptide_buffer.len(),
                                    );
                                    let inserted =
                                        peptide_table.insert_batch(&peptide_buffer).await?;
                                    peptide_buffer.clear();
                                    peptide_ctr
                                        .fetch_add(inserted, std::sync::atomic::Ordering::SeqCst);
                                    inserted_peptides_metric.increment(inserted as u64);
                                }
                                break;
                            }
                            None => {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        };

                        let protein_ids = Vec::from_iter(protein_ids);

                        let protein_ids_len = protein_ids.len();

                        let mut proteins = protein_access.by_ids(&protein_ids).await?;

                        let protein_ids = if skip_protein_associations {
                            Vec::new()
                        } else {
                            protein_ids.clone()
                        };

                        // Using the more compact form of the sequence to keep the peptide in memory as small as possible, mass is not important now.
                        let mut peptide_sequences: HashMap<CompactSequence, HashMap<i32, usize>> =
                            HashMap::with_capacity(2 * protein_ids_len);

                        while let Some(protein) = proteins.next().await.transpose()? {
                            #[allow(clippy::mutable_key_type)]
                            protease
                                .cleave(protein.sequence().as_ref(), Some(mass..=mass))
                                .for_each(|peptide| {
                                    peptide_sequences
                                        .entry(CompactSequence::try_from(peptide.into_sequence())?)
                                        .or_default()
                                        .entry(protein.taxonomy_id())
                                        .or_insert(0)
                                        .add_assign(1);
                                    Ok(())
                                })?;
                        }

                        let peptides: Vec<Peptide> = peptide_sequences
                            .into_iter()
                            .map(|(seq, taxonomies)| {
                                let unique_taxonomy_ids = taxonomies
                                    .iter()
                                    .filter(|(_, count)| **count == 1 && !skip_taxonomies)
                                    .map(|(taxonomy_id, _)| *taxonomy_id)
                                    .collect::<Vec<_>>();

                                let non_unique_taxonomy_ids = taxonomies
                                    .into_iter()
                                    .filter(|(_, count)| *count > 1 && !skip_taxonomies)
                                    .map(|(taxonomy_id, _)| taxonomy_id)
                                    .collect::<Vec<_>>();

                                Ok::<_, Error>(Peptide::new(
                                    PeptideSequence::try_from(seq).map_err(Error::Sequence)?,
                                    protein_ids.clone(),
                                    unique_taxonomy_ids,
                                    non_unique_taxonomy_ids,
                                ))
                            })
                            .collect::<Result<_, _>>()?;

                        for mut peptide in peptides {
                            // Flush when the partition has filled one columnar stripe
                            // (row count is the primary trigger; the byte ceiling is a
                            // memory guard for peptides in very many proteins).
                            if !peptide_buffer.is_empty()
                                && (peptide_buffer.len() >= STRIPE_ROW_LIMIT
                                    || partition_bytes + peptide.cql_size() >= MAX_PARTITION_BYTES)
                            {
                                for &m in &buffer_masses {
                                    mass_partition_map.entry(m).or_default().push(partition);
                                }
                                tracing::debug!(
                                    "Partition {partition} full: flushing {} peptides ({} bytes)",
                                    peptide_buffer.len(),
                                    partition_bytes,
                                );
                                let inserted = peptide_table.insert_batch(&peptide_buffer).await?;
                                peptide_buffer.clear();
                                peptide_ctr
                                    .fetch_add(inserted, std::sync::atomic::Ordering::SeqCst);
                                inserted_peptides_metric.increment(inserted as u64);
                                partition_bytes = 0;
                                buffer_masses.clear();
                                partition = next_partition_guard.next_partition();
                            }

                            partition_bytes += peptide.cql_size();
                            peptide.set_partition(partition);
                            peptide_buffer.push(peptide);
                            buffer_masses.insert(mass);
                        }

                        progress_metric.increment(protein_ids_len as f64);
                    }
                    Ok::<_, Error>(mass_partition_map)
                })
            })
            .collect::<Vec<_>>();

        for mass_entry in mass_index.into_iter() {
            let mut mass_index_entry = Some(mass_entry);
            loop {
                mass_index_entry = match queue.push(mass_index_entry) {
                    Ok(()) => break,
                    Err(entry) => {
                        // check if all threads still running
                        if digest_and_insertion_threads
                            .iter()
                            .any(|thread| thread.is_finished())
                        {
                            // find errored_thread and return error
                            return Err(
                                Self::find_errored_thread(digest_and_insertion_threads).await
                            );
                        }
                        entry
                    }
                };
            }
            queue_metric.set(queue.len() as f64);
        }

        // Send none to signal stop
        for _ in 0..num_threads.get() {
            loop {
                if queue.push(None).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        let mut final_mass_to_partitions_map: HashMap<i64, Vec<i64>> = HashMap::new();

        for thread in digest_and_insertion_threads {
            match thread.await.map_err(|err| Error::Join(err.to_string()))? {
                Ok(map) => final_mass_to_partitions_map.extend(map),
                Err(err) => return Err(err),
            }
        }

        Ok((
            peptide_ctr.load(std::sync::atomic::Ordering::SeqCst),
            final_mass_to_partitions_map,
        ))
    }

    #[allow(clippy::type_complexity)]
    async fn find_errored_thread(
        threads: Vec<tokio::task::JoinHandle<Result<HashMap<i64, Vec<i64>>, Error>>>,
    ) -> Error {
        for thread in threads {
            if thread.is_finished() {
                match thread.await {
                    Ok(Ok(_)) => continue,
                    Ok(Err(err)) => return err,
                    Err(_err) => continue,
                }
            }
        }

        Error::NoErroredThread
    }
}
