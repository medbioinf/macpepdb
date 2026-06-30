use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    ops::{AddAssign, RangeInclusive},
    sync::{
        Arc, LazyLock,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
};

use dashmap::{DashMap, mapref::entry::Entry};
use fallible_iterator::FallibleIterator;
use futures::{Stream, StreamExt};
use postgres_types::{ToSql, Type};
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_128;

use crate::{
    client::Client,
    database_build::IsProteinAccess,
    mass_index::MassIndex,
    peptide::{IsPeptide, Peptide},
    peptide_metadata_table::PeptideMetadataTable,
    peptide_search::SEARCH_SELECT_STATEMENT,
    protease::Protease,
    protein::Protein,
    protein_ids::ProteinIds,
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

/// Newly-interned `peptide_metadata` rows are COPYed in batches of this size per worker.
const METADATA_FLUSH_ROWS: usize = 8192;

/// Protein associations a build worker claims per contiguous mass chunk. Sizing claims by
/// associations (not mass count) makes each chunk ~equal *work* — digestion cost is roughly
/// proportional to associations — which keeps worker run lengths even and avoids the
/// heavy-chunk tail a fixed mass-count claim suffers under skewed per-mass work.
///
/// Each chunk flushes its (possibly partial) trailing partition at the chunk boundary, so
/// the chunk count (`total_associations / this`) bounds the extra underfilled partitions.
/// Larger ⇒ fuller partitions / lower search fan-out but a longer tail; smaller ⇒ shorter
/// tail but more partitions. A few × `STRIPE_ROW_LIMIT` keeps both comfortably in range.
const TARGET_ASSOCIATIONS_PER_CLAIM: u64 = 16 * STRIPE_ROW_LIMIT as u64;

pub const PARTITION_COL: &str = "partition";

pub const MASS_COL: &str = "mass";

pub const FLAGS_COLUMN: &str = "flags";

pub const COLUMNS: &str = "partition, mass, sequence, amino_acid_counts, metadata_id, unique_taxonomy_ids, non_unique_taxonomy_ids, flags";

static COPY_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("COPY {TABLE_NAME} ({COLUMNS}) FROM STDIN (FORMAT binary)"));

/// Column types for the binary COPY into `peptides`, in column order.
// TODO: No need for a lazy lock
static COPY_TYPES: LazyLock<[Type; 8]> = LazyLock::new(|| {
    [
        Type::INT8,       // partition
        Type::INT8,       // mass
        Type::BYTEA,      // sequence (CompactSequence bytes)
        Type::BYTEA,      // amino acid counts
        Type::INT8,       // metadata_id (reference into peptide_metadata)
        Type::INT4_ARRAY, // unique_taxonomy_ids
        Type::INT4_ARRAY, // non_unique_taxonomy_ids
        Type::CHAR,       // flags
    ]
});

static SELECT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT {COLUMNS} FROM {TABLE_NAME}"));

pub static PROGRESS_METRIC: &str = "peptides_table::build::progress";
pub static INSERTED_PEPTIDES_METRIC: &str = "peptides_table::build::inserted_peptides";

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
    #[error("Protease error in peptide table: {0}")]
    Protease(#[from] crate::protease::Error),
    #[error("Protein access error in peptide table: {0}")]
    ProteinAccess(Box<crate::database_build::Error>),
    #[error("Peptide error in peptide table: {0}")]
    Peptide(#[from] crate::peptide::Error),
    #[error("Peptide metadata table error in peptide table: {0}")]
    PeptideMetadata(Box<crate::peptide_metadata_table::Error>),
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
into_thiserror_boxed!(crate::peptide_metadata_table::Error, Error, PeptideMetadata);

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
                    let metadata_id = peptide
                        .metadata_id()
                        .expect("peptide metadata_id must be set before COPY");
                    let unique = peptide.unique_taxonomy_ids();
                    let non_unique = peptide.non_unique_taxonomy_ids();
                    copy.write(&[
                        &partition,
                        &mass,
                        peptide.sequence(),
                        peptide.amino_acid_counts(),
                        &metadata_id,
                        &unique,
                        &non_unique,
                        peptide.flags_as_ref(),
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

    /// Like [`PeptideTable::select`] but for a `where_clause` with all values inlined as
    /// literals (no bind parameters). Uses the non-caching inlined path so Citus can
    /// prune shards/chunk groups at plan time. See [`Client::query_stream_inline`].
    pub async fn select_inline(
        &self,
        where_clause: &str,
    ) -> Result<impl Stream<Item = Result<Peptide, Error>> + Send + use<>, Error> {
        let statement = format!("{} {where_clause}", SEARCH_SELECT_STATEMENT.as_str());
        let stream = self.client.query_stream_inline(&statement).await?;
        Ok(stream.map(|row_res| {
            row_res
                .map_err(Error::Row)
                .and_then(|row| Peptide::try_from_search_row(&row).map_err(Error::from))
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
        mass_index: Arc<MassIndex>,
    ) -> Result<(usize, HashMap<i64, Vec<i64>>), Error> {
        let progress_metric = Arc::new(metrics::gauge!(PROGRESS_METRIC));
        let inserted_peptides_metric = Arc::new(metrics::counter!(INSERTED_PEPTIDES_METRIC));
        let next_partition_guard = Arc::new(NextPartitionGuard::new());
        let peptide_ctr = Arc::new(AtomicUsize::new(0));

        // Shared interner: xxh3_128(canonical protein_ids blob) -> metadata_id. Deduplicates
        // protein-id sets across all threads/partitions. ~16-byte key + i64 value per distinct
        // set (~55M expected => ~3 GB) — budget this against the in-memory proteins.
        let metadata_interner: Arc<DashMap<u128, i64>> = Arc::new(DashMap::new());
        let metadata_id_counter = Arc::new(AtomicI64::new(0));

        // Workers claim disjoint contiguous chunks of the globally mass-sorted index via a
        // shared cursor (instead of interleaving a shared per-mass queue). Each chunk is
        // digested ascending into one or more partitions and the buffer is flushed at the
        // chunk boundary, so every partition covers a disjoint contiguous mass range — which
        // collapses search-time partition fan-out. Chunks are sized by protein associations
        // (~equal work per chunk) rather than mass count, so per-mass work skew can't strand
        // a worker on one heavy chunk while the rest idle.
        let total_masses = mass_index.len();
        let cursor = Arc::new(AtomicUsize::new(0));

        let digest_and_insertion_threads = (0..num_threads.get())
            .map(|_| {
                let protein_access = protein_access.clone();
                let protease = protease.clone();
                let client = self.client.clone();
                let progress_metric = progress_metric.clone();
                let inserted_peptides_metric = inserted_peptides_metric.clone();
                let next_partition_guard = next_partition_guard.clone();
                let peptide_ctr = peptide_ctr.clone();
                let metadata_interner = metadata_interner.clone();
                let metadata_id_counter = metadata_id_counter.clone();
                let mass_index = mass_index.clone();
                let cursor = cursor.clone();

                tokio::spawn(async move {
                    let peptide_table = PeptideTable::new(client.clone());
                    let metadata_table = PeptideMetadataTable::new(client.clone());
                    let mut metadata_buffer: Vec<(i64, ProteinIds)> = Vec::new();
                    let mut mass_partition_map: HashMap<i64, Vec<i64>> = HashMap::new();
                    let mut peptide_buffer: Vec<Peptide> = Vec::new();
                    let mut partition_bytes: usize = 0;
                    // Acquired lazily on the first push into an empty buffer so workers that
                    // draw short/empty claims never burn (and never reuse) a partition id.
                    let mut partition: Option<i64> = None;
                    // Track which masses have peptides in the current buffer.
                    let mut buffer_masses: HashSet<i64> = HashSet::new();

                    loop {
                        // Claim a work-balanced contiguous chunk: the mass range starting at
                        // `start` that carries ~TARGET_ASSOCIATIONS_PER_CLAIM associations.
                        // `claim_end` is a pure function of `start`, so the CAS only decides
                        // which worker wins this chunk; losers retry with the advanced cursor.
                        let start = cursor.load(Ordering::Relaxed);
                        if start >= total_masses {
                            break;
                        }
                        let end = mass_index.claim_end(start, TARGET_ASSOCIATIONS_PER_CLAIM);
                        if cursor
                            .compare_exchange_weak(start, end, Ordering::Relaxed, Ordering::Relaxed)
                            .is_err()
                        {
                            continue;
                        }

                        // Digest the claimed mass range in ascending order so each stripe is
                        // mass-sorted (tight columnar chunk-group pruning at search time). The
                        // reader streams the claim's protein_ids from disk on demand.
                        let mut claim_reader = mass_index.claim_reader(start, end)?;
                        while let Some((mass, protein_ids)) = claim_reader.next_entry()? {
                            let protein_ids_len = protein_ids.len();

                            let mut proteins = protein_access.by_ids(&protein_ids).await?;

                            let protein_ids = if skip_protein_associations {
                                Vec::new()
                            } else {
                                protein_ids.clone()
                            };

                            let metadata = ProteinIds::from(protein_ids);
                            let hash = xxh3_128(&metadata.encode());
                            let (metadata_id, is_new_metadata) = match metadata_interner.entry(hash)
                            {
                                Entry::Occupied(existing) => (*existing.get(), false),
                                Entry::Vacant(slot) => {
                                    let id = metadata_id_counter.fetch_add(1, Ordering::SeqCst);
                                    slot.insert(id);
                                    (id, true)
                                }
                            };

                            if is_new_metadata {
                                metadata_buffer.push((metadata_id, metadata));
                                if metadata_buffer.len() >= METADATA_FLUSH_ROWS {
                                    metadata_table.insert_batch(&metadata_buffer).await?;
                                    metadata_buffer.clear();
                                }
                            }

                            // Using the more compact form of the sequence to keep the peptide in memory as small as possible, mass is not important now.
                            let mut peptide_sequences: HashMap<
                                CompactSequence,
                                HashMap<i32, usize>,
                            > = HashMap::with_capacity(2 * protein_ids_len);

                            let mut is_swiss_prot = false;
                            let mut is_trembl: bool = false;

                            while let Some(protein) = proteins.next().await.transpose()? {
                                is_swiss_prot |= protein.is_reviewed();
                                is_trembl |= !protein.is_reviewed();

                                Self::digest_protein(
                                    protease.as_ref(),
                                    protein.as_ref(),
                                    mass..=mass,
                                    &mut peptide_sequences,
                                )?;
                            }

                            let peptides: Vec<Peptide> = Self::finalize_peptides(
                                peptide_sequences,
                                metadata_id,
                                is_swiss_prot,
                                is_trembl,
                                skip_taxonomies,
                            )?;

                            for mut peptide in peptides {
                                // Flush when the partition has filled one columnar stripe
                                // (row count is the primary trigger; the byte ceiling is a
                                // memory guard for peptides in very many proteins).
                                if !peptide_buffer.is_empty()
                                    && (peptide_buffer.len() >= STRIPE_ROW_LIMIT
                                        || partition_bytes + peptide.cql_size()
                                            >= MAX_PARTITION_BYTES)
                                {
                                    let p =
                                        partition.expect("partition id set with non-empty buffer");
                                    for &m in &buffer_masses {
                                        mass_partition_map.entry(m).or_default().push(p);
                                    }
                                    let inserted =
                                        peptide_table.insert_batch(&peptide_buffer).await?;
                                    peptide_buffer.clear();
                                    peptide_ctr.fetch_add(inserted, Ordering::SeqCst);
                                    inserted_peptides_metric.increment(inserted as u64);
                                    partition_bytes = 0;
                                    buffer_masses.clear();
                                    partition = None;
                                }

                                let p = *partition
                                    .get_or_insert_with(|| next_partition_guard.next_partition());
                                partition_bytes += peptide.cql_size();
                                peptide.set_partition(p);
                                peptide_buffer.push(peptide);
                                buffer_masses.insert(mass);
                            }

                            progress_metric.increment(protein_ids_len as f64);
                        }

                        // Boundary flush: never carry a partial buffer across claims. The next
                        // claim is a non-adjacent mass range (another worker took the chunk in
                        // between), so carrying over would merge non-contiguous masses into one
                        // partition and reintroduce overlap. Mirrors the mid-loop flush above
                        // (resets bytes/masses/partition), unlike the old final-exit flush.
                        if !peptide_buffer.is_empty() {
                            let p = partition.expect("partition id set with non-empty buffer");
                            for &m in &buffer_masses {
                                mass_partition_map.entry(m).or_default().push(p);
                            }
                            let inserted = peptide_table.insert_batch(&peptide_buffer).await?;
                            peptide_buffer.clear();
                            peptide_ctr.fetch_add(inserted, Ordering::SeqCst);
                            inserted_peptides_metric.increment(inserted as u64);
                            partition_bytes = 0;
                            buffer_masses.clear();
                            partition = None;
                        }
                    }

                    if !metadata_buffer.is_empty() {
                        metadata_table.insert_batch(&metadata_buffer).await?;
                        metadata_buffer.clear();
                    }

                    Ok::<_, Error>(mass_partition_map)
                })
            })
            .collect::<Vec<_>>();

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

    fn digest_protein(
        protease: &Protease,
        protein: &Protein,
        mass_range: RangeInclusive<i64>,
        peptide_sequences: &mut HashMap<CompactSequence, HashMap<i32, usize>>,
    ) -> Result<(), Error> {
        #[allow(clippy::mutable_key_type)]
        protease
            .cleave(protein.sequence().as_ref(), Some(mass_range))
            .for_each(|peptide| {
                peptide_sequences
                    .entry(CompactSequence::try_from(peptide.into_sequence())?)
                    .or_default()
                    .entry(protein.taxonomy_id())
                    .or_insert(0)
                    .add_assign(1);
                Ok(())
            })?;

        Ok(())
    }

    fn finalize_peptides(
        peptide_sequences: HashMap<CompactSequence, HashMap<i32, usize>>,
        metadata_id: i64,
        is_swiss_prot: bool,
        is_trembl: bool,
        skip_taxonomies: bool,
    ) -> Result<Vec<Peptide>, Error> {
        peptide_sequences
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

                Ok::<_, Error>(Peptide::new_with_metadata(
                    PeptideSequence::try_from(seq).map_err(Error::Sequence)?,
                    metadata_id,
                    unique_taxonomy_ids,
                    non_unique_taxonomy_ids,
                    is_swiss_prot,
                    is_trembl,
                ))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, num::NonZeroUsize};

    use crate::{
        peptide::IsPeptide,
        peptide_table::PeptideTable,
        protease::Protease,
        protein::Protein,
        sequence::{CompactSequence, PeptideSequence, ProteinSequence},
    };

    #[test]
    fn test_taxonomy_assignment() {
        let leptin0 = Protein::new(
            "Q257X2".to_string(),
            Some(0),
            ProteinSequence::try_from(
                "MHWGTLCGFLWLWPYLFYVQAVPIQKVQDDTKTLIKTIVTRINDISHTQSVSSKQKVTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSRNVIQISNDLENLRDLLHVLAFSKSCHLPWASGLETLDSLGGVLEASGYSTEVVALSRLQGSLQDMLWQLDLSPGC",
            ).unwrap(),
            0,
            true,
            Vec::new()
        );
        let leptin1 = Protein::new(
            "O42164".to_string(),
            Some(1),
            ProteinSequence::try_from(
                "MHWGTLCGFLWLWPYLFYVQAVPIQKVQDDTKTLIKTIVTRINDISHTQSVSSKQKVTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSRNVIQISNDLENLRDLLHVLAFSKSCHLPWASGLETLDSLGGVLEASGYSTEVVALSRLQGSLQDMLWQLDLSPGC",
            ).unwrap(),
            0,
            true,
            Vec::new()
        );
        // missing M at the beginning makes first peptide unique among this three proteins
        let leptin2 = Protein::new(
            "custom".to_string(),
            Some(2),
            ProteinSequence::try_from(
                "HWGTLCGFLWLWPYLFYVQAVPIQKVQDDTKTLIKTIVTRINDISHTQSVSSKQKVTGLDFIPGLHPILTLSKMDQTLAVYQQILTSMPSRNVIQISNDLENLRDLLHVLAFSKSCHLPWASGLETLDSLGGVLEASGYSTEVVALSRLQGSLQDMLWQLDLSPGC",
            ).unwrap(),
            0,
            false,
            Vec::new()
        );

        let trypsin = Protease::by_name(
            "trypsin",
            Some(NonZeroUsize::new(6).unwrap()),
            Some(NonZeroUsize::new(50).unwrap()),
            Some(0),
            false,
        )
        .unwrap();

        let mut peptide_sequences: HashMap<CompactSequence, HashMap<i32, usize>> = HashMap::new();
        PeptideTable::digest_protein(
            &trypsin,
            &leptin0,
            i64::MIN..=i64::MAX,
            &mut peptide_sequences,
        )
        .unwrap();
        PeptideTable::digest_protein(
            &trypsin,
            &leptin1,
            i64::MIN..=i64::MAX,
            &mut peptide_sequences,
        )
        .unwrap();
        PeptideTable::digest_protein(
            &trypsin,
            &leptin2,
            i64::MIN..=i64::MAX,
            &mut peptide_sequences,
        )
        .unwrap();

        let peptides =
            PeptideTable::finalize_peptides(peptide_sequences, 0, true, true, false).unwrap();

        let only_in2 = PeptideSequence::try_from("HWGTLCGFLWLWPYLFYVQAVPIQK").unwrap();
        for pep in peptides {
            if pep.sequence() == &only_in2 {
                assert_eq!(pep.unique_taxonomy_ids(), vec![0]);
                assert!(pep.non_unique_taxonomy_ids().is_empty());
            } else {
                assert!(pep.unique_taxonomy_ids().is_empty());
                assert_eq!(pep.non_unique_taxonomy_ids(), vec![0]);
            }
        }
    }
}
