use std::{collections::HashMap, num::NonZeroUsize, path::PathBuf, sync::Arc, time::Instant};

use dihardts_omicstools::biology::io::taxonomy_reader::TaxonomyReader;
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream::BoxStream};
use macpepdb_tui::{MetricConfig, TuiHandle};
use metrics::counter;
use serde::{Deserialize, Serialize};
use sysinfo::System;
use thiserror::Error;

use crate::{
    blob_table::BlobTable, client::Client, configuration::RuntimeConfiguration,
    mass_index::MassIndex, peptide_table::PeptideTable, protease::Protease, protein::Protein,
    protein_table::ProteinTable, stats_table::StatsTable, taxonomy::Taxonomy,
    taxonomy_rank::TaxonomyRank, taxonomy_rank_table::TaxonomyRankTable,
    taxonomy_table::TaxonomyTable,
};

const PROGRESS_REPORT_EVERY: u64 = 8192;

/// Progress of loading all proteins into memory ahead of the mass-index/peptide build stages.
pub static IN_MEMORY_PROTEIN_ACCESS_BUILD_PROGRESS: &str =
    "database::build::load_proteins_into_memory";

/// Errors occurring while running the protein/mass-index/peptide build pipeline.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Blob table error in database build: {0}")]
    BlobTable(Box<crate::blob_table::Error>),
    #[error("Default, should not occure anywhere")]
    Default,
    #[error("Mass index error in database build: {0}")]
    MassIndex(Box<crate::mass_index::Error>),
    #[error("Missing protein ID for `{0}`, means it does not come from the database")]
    MissingProteinId(String),
    #[error(
        "Missing protein count in stats table, cannot decide whether to keep proteins in memory or not"
    )]
    MissingProteinCount,
    #[error(
        "Missing proteins size in stats table, cannot decide whether to keep proteins in memory or not"
    )]
    MissingProteinsSize,
    #[error("Peptide table error in database build: {0}")]
    PeptideTable(Box<crate::peptide_table::Error>),
    #[error("Protein table error in database build: {0}")]
    ProteinTable(Box<crate::protein_table::Error>),
    #[error("Stats table error in database build: {0}")]
    StatsTable(Box<crate::stats_table::Error>),
    #[error("Taxonomy error in taxonomy tree build: {0}")]
    Taxonomy(Box<crate::taxonomy::Error>),
    #[error("Taxonomy table error in taxonomy tree build: {0}")]
    TaxonomyTable(Box<crate::taxonomy_table::Error>),
    #[error("Taxonomy error in taxonomy tree build: {0}")]
    TaxonomyRank(Box<crate::taxonomy_rank::Error>),
    #[error("Taxonomy rank table error in taxonomy tree build: {0}")]
    TaxonomyRankTable(Box<crate::taxonomy_rank_table::Error>),
    #[error("Taxonomy tree read error in taxonomy tree build: {0}")]
    TaxonomyTreeReadError(String),
    #[error("Unable to join protein-loading task: {0}")]
    Join(String),
}

into_thiserror_boxed!(crate::blob_table::Error, Error, BlobTable);
into_thiserror_boxed!(crate::mass_index::Error, Error, MassIndex);
into_thiserror_boxed!(crate::peptide_table::Error, Error, PeptideTable);
into_thiserror_boxed!(crate::protein_table::Error, Error, ProteinTable);
into_thiserror_boxed!(crate::stats_table::Error, Error, StatsTable);
into_thiserror_boxed!(crate::taxonomy::Error, Error, Taxonomy);
into_thiserror_boxed!(crate::taxonomy_table::Error, Error, TaxonomyTable);
into_thiserror_boxed!(crate::taxonomy_rank::Error, Error, TaxonomyRank);
into_thiserror_boxed!(crate::taxonomy_rank_table::Error, Error, TaxonomyRankTable);

/// The closed mass range `[lo, hi]` covered by one partition.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct PartitionRange {
    lo: i64,
    hi: i64,
    partition: i64,
}

impl PartitionRange {
    /// Creates a range for `partition` spanning the masses `[lo, hi]` inclusive.
    pub fn new(lo: i64, hi: i64, partition: i64) -> Self {
        Self { lo, hi, partition }
    }

    pub fn lo(&self) -> i64 {
        self.lo
    }

    pub fn hi(&self) -> i64 {
        self.hi
    }

    pub fn partition(&self) -> i64 {
        self.partition
    }
}

/// Maps a peptide mass to the partition(s) that hold peptides of that mass, stored as one closed
/// mass range per partition rather than one entry per mass.
///
/// Stage 3 digests contiguous, globally-ascending mass claims and flushes its buffer at every claim
/// boundary ([`PeptideTable::build_concurrently`]), so each partition covers exactly one contiguous
/// mass range and two partitions overlap only at a shared boundary mass — a mass whose peptides
/// spilled past one columnar stripe. Recording ranges instead of masses collapses the map from one
/// entry per distinct mass (hundreds of millions at full TrEMBL scale) to one per partition
/// (`peptide_count / STRIPE_ROW_LIMIT`), which is what keeps it cheap to accumulate during the build
/// and cheap to load on every `api` / `search` start.
///
/// That contiguous-tiling invariant is therefore **load-bearing**: if stage 3 stopped flushing at
/// claim boundaries, one partition would cover non-adjacent masses and the lookups below would
/// silently return the wrong partitions — losing search hits rather than erroring. `config verify`
/// checks it against a stored per-mass map.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MassPartitionMap {
    /// One closed mass range per partition, sorted by `(lo, hi)`.
    ranges: Vec<PartitionRange>,
}

impl MassPartitionMap {
    /// Builds the map from the per-partition ranges collected by stage 3.
    pub fn new(mut ranges: Vec<PartitionRange>) -> Self {
        ranges.sort_unstable_by_key(|range| (range.lo, range.hi));
        // Ranges are disjoint apart from shared endpoints, so sorting by `lo` also leaves `hi`
        // non-decreasing — which is what makes the binary search in the lookups below valid.
        debug_assert!(
            ranges.windows(2).all(|pair| pair[0].hi <= pair[1].hi),
            "partition mass ranges must tile the mass line ascending and overlap only at a shared \
             boundary mass; stage 3 must keep flushing its buffer at every claim boundary"
        );
        Self { ranges }
    }

    /// Returns the partitions holding any mass in `[lower_mass, upper_mass]`, without duplicates.
    ///
    /// Ordered by ascending mass range, which is not the same as ascending partition id — ids are
    /// handed out by a shared counter as workers flush, so they carry no mass ordering.
    ///
    /// # Arguments
    /// * `lower_mass` - Lower bound of the mass range (inclusive)
    /// * `upper_mass` - Upper bound of the mass range (inclusive)
    pub fn partitions_by_mass_range(
        &self,
        lower_mass: i64,
        upper_mass: i64,
    ) -> impl Iterator<Item = i64> {
        let start = self.ranges.partition_point(|range| range.hi < lower_mass);

        self.ranges[start..]
            .iter()
            .take_while(move |range| range.lo <= upper_mass)
            .map(|range| range.partition)
    }

    /// Returns the partitions that may hold peptides of exactly `mass`.
    ///
    /// Because ranges span the gaps between stored masses, this can name a partition for a mass
    /// that was never stored. Callers already qualify their query with `mass = ...`, so the result
    /// is still correct — it just costs one pruned query that returns nothing.
    ///
    /// # Arguments
    /// * `mass` - The exact mass to look up partitions for
    pub fn partition_by_mass(&self, mass: i64) -> impl Iterator<Item = i64> {
        self.partitions_by_mass_range(mass, mass)
    }

    /// Number of partitions in the map.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// The stored ranges, ascending by `(lo, hi)`.
    pub fn ranges(&self) -> &[PartitionRange] {
        &self.ranges
    }

    /// Folds `(mass, partition)` pairs into one range per partition by taking each partition's
    /// minimum and maximum mass.
    ///
    /// This reconstructs the range form from the historical per-mass shape — used by
    /// `config migrate` to convert a stored v1 configuration without re-running the build, and by
    /// tests that find it easier to spell out a map mass-by-mass.
    pub fn from_mass_partition_pairs(pairs: impl Iterator<Item = (i64, i64)>) -> Self {
        let mut bounds: HashMap<i64, (i64, i64)> = HashMap::new();

        for (mass, partition) in pairs {
            bounds
                .entry(partition)
                .and_modify(|(lo, hi)| {
                    *lo = (*lo).min(mass);
                    *hi = (*hi).max(mass);
                })
                .or_insert((mass, mass));
        }

        Self::new(
            bounds
                .into_iter()
                .map(|(partition, (lo, hi))| PartitionRange { lo, hi, partition })
                .collect(),
        )
    }
}

#[cfg(test)]
impl From<HashMap<i64, Vec<i64>>> for MassPartitionMap {
    fn from(map: HashMap<i64, Vec<i64>>) -> Self {
        Self::from_mass_partition_pairs(
            map.into_iter()
                .flat_map(|(mass, partitions)| partitions.into_iter().map(move |p| (mass, p))),
        )
    }
}

type BoxedPeptideStreamFuture<'a> =
    BoxFuture<'a, Result<BoxStream<'a, Result<Arc<Protein>, Error>>, Error>>;

/// Abstracts how the build pipeline reads back proteins for digestion, so the mass-index and
/// peptide stages don't need to care whether proteins were kept in memory (`InMemoryProteinAccess`)
/// or are re-read from the database (`DatabaseProteinAccess`).
pub trait IsProteinAccess: Send + Sync {
    /// Streams the proteins matching the given `ids`, in no particular order.
    ///
    /// # Arguments
    /// * `ids` - Protein IDs to fetch
    fn by_ids<'a>(&'a self, ids: &'a [i32]) -> BoxedPeptideStreamFuture<'a>;
    /// Streams every protein.
    fn all<'a>(&'a self) -> BoxedPeptideStreamFuture<'a>;
    /// Returns the total number of proteins.
    fn count<'a>(&'a self) -> BoxFuture<'a, Result<usize, Error>>;
    /// Returns the IDs of every protein.
    fn ids<'a>(&'a self) -> BoxFuture<'a, Result<Vec<i32>, Error>>;
}

/// Reads proteins back from the `proteins` table on demand. Used when the protein set is too
/// large to fit within `--proteins-memory-limit`.
pub struct DatabaseProteinAccess {
    protein_table: ProteinTable,
}

impl DatabaseProteinAccess {
    /// Creates a new instance backed by the given database client.
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            protein_table: ProteinTable::new(client),
        }
    }
}

impl IsProteinAccess for DatabaseProteinAccess {
    fn by_ids<'a>(&'a self, ids: &'a [i32]) -> BoxedPeptideStreamFuture<'a> {
        async move {
            Ok(self
                .protein_table
                .select_by_ids(ids)
                .await?
                .map(|protein_res| protein_res.map(Arc::new).map_err(Error::from))
                .boxed())
        }
        .boxed()
    }

    fn all<'a>(&'a self) -> BoxedPeptideStreamFuture<'a> {
        async move {
            Ok(self
                .protein_table
                .select_all()
                .await?
                .map(|protein_res| protein_res.map(Arc::new).map_err(Error::from))
                .boxed())
        }
        .boxed()
    }

    fn count<'a>(&'a self) -> BoxFuture<'a, Result<usize, Error>> {
        async move { self.protein_table.count().await.map_err(Error::from) }.boxed()
    }

    fn ids<'a>(&'a self) -> BoxFuture<'a, Result<Vec<i32>, Error>> {
        async move {
            self.protein_table
                .select_ids()
                .await?
                .try_collect::<Vec<i32>>()
                .await
                .map_err(Error::from)
        }
        .boxed()
    }
}

/// Holds every protein in memory, keyed by ID, behind an `Arc` for cheap cloning. Used when
/// the whole protein set fits within `--proteins-memory-limit`; greatly speeds up digestion but
/// competes with the mass index for RAM.
pub struct InMemoryProteinAccess {
    proteins: HashMap<i32, Arc<Protein>>,
}

impl InMemoryProteinAccess {
    /// Loads all proteins from the database into memory, in parallel chunks of the ID range.
    ///
    /// # Arguments
    /// * `client` - Database client
    ///
    pub async fn new(client: Arc<Client>) -> Result<Self, Error> {
        let progress_counter_metric = Arc::new(counter!(IN_MEMORY_PROTEIN_ACCESS_BUILD_PROGRESS));
        let protein_count = ProteinTable::new(client.clone()).count().await?;
        tracing::info!("Load all {protein_count} proteins in memory...");

        let now = Instant::now();
        let count = ProteinTable::new(client.clone()).count().await?;
        let parallelism = 8; // temporary workaround
        let id_lo = i32::MIN as i64;
        let span = count as i64;

        let handles = (0..parallelism)
            .map(|chunk| {
                let client = client.clone();
                let progress_counter_metric = progress_counter_metric.clone();
                let start = (id_lo + span * chunk as i64 / parallelism as i64) as i32;
                let end = (id_lo + span * (chunk as i64 + 1) / parallelism as i64) as i32;
                tokio::spawn(async move {
                    let mut stream = ProteinTable::new(client)
                        .select_by_id_range(start, end)
                        .await?;
                    let mut local: Vec<(i32, Arc<Protein>)> = Vec::new();
                    let mut since_report: u64 = 0;
                    while let Some(protein) = stream.next().await {
                        let protein = protein?;
                        local.push((protein.id().unwrap(), Arc::new(protein)));
                        since_report += 1;
                        if since_report == PROGRESS_REPORT_EVERY {
                            progress_counter_metric.increment(since_report);
                            since_report = 0;
                        }
                    }
                    if since_report > 0 {
                        progress_counter_metric.increment(since_report);
                    }
                    Ok::<Vec<(i32, Arc<Protein>)>, Error>(local)
                })
            })
            .collect::<Vec<_>>();

        let mut proteins: HashMap<i32, Arc<Protein>> = HashMap::with_capacity(count);
        for handle in handles {
            let chunk = handle.await.map_err(|e| Error::Join(e.to_string()))??;
            proteins.extend(chunk);
        }

        tracing::info!(
            "load proteins in memory: time = {:.1} s",
            now.elapsed().as_secs_f64(),
        );

        Ok(Self { proteins })
    }

    #[cfg(test)]
    pub(crate) fn with_proteins(proteins: impl Iterator<Item = Protein>) -> Self {
        Self {
            proteins: proteins
                .map(|protein| (protein.id().unwrap(), Arc::new(protein)))
                .collect(),
        }
    }
}

impl IsProteinAccess for InMemoryProteinAccess {
    fn by_ids<'a>(&'a self, ids: &'a [i32]) -> BoxedPeptideStreamFuture<'a> {
        async move {
            Ok(futures::stream::iter(
                ids.iter()
                    .filter_map(|id| self.proteins.get(id).map(|protein| Ok(protein.clone()))),
            )
            .boxed())
        }
        .boxed()
    }

    fn all<'a>(&'a self) -> BoxedPeptideStreamFuture<'a> {
        async move {
            Ok(
                futures::stream::iter(self.proteins.values().map(|protein| Ok(protein.clone())))
                    .boxed(),
            )
        }
        .boxed()
    }

    fn count(&self) -> BoxFuture<'_, Result<usize, Error>> {
        async move { Ok(self.proteins.len()) }.boxed()
    }

    fn ids<'a>(&'a self) -> BoxFuture<'a, Result<Vec<i32>, Error>> {
        async move { Ok(self.proteins.keys().cloned().collect::<Vec<i32>>()) }.boxed()
    }
}

/// Drives the three-stage build pipeline (proteins, mass index, peptides) plus the optional
/// taxonomy tree, and produces the `RuntimeConfiguration` to persist afterwards.
pub struct DatabaseBuild<'a> {
    client: Arc<Client>,
    comment: Option<String>,
    protein_file_paths: &'a [PathBuf],
    protease: Arc<Protease>,
    batch_size_limit: NonZeroUsize,
    concurrent_batch_size: NonZeroUsize,
    proteins_memory_limit: f64,
    skip_proteins: bool,
    skip_protein_associations: bool,
    skip_taxonomies: bool,
    num_threads: NonZeroUsize,
    taxonomy_dump_file_path: Option<PathBuf>,
    resolve_isoforms: bool,
    scratch_dir: PathBuf,
    tui: Option<&'a TuiHandle>,
}

impl<'a> DatabaseBuild<'a> {
    /// Creates a new build, ready to run via [`DatabaseBuild::start`].
    ///
    /// # Arguments
    /// * `client` - Database client
    /// * `comment` - Optional comment stored in the resulting configuration
    /// * `protein_file_paths` - UniProt protein files to digest
    /// * `protease` - Protease to digest proteins with
    /// * `batch_size_limit` - Max size (KB) of a batch insert
    /// * `concurrent_batch_size` - Number of concurrent inserts for non-partitioned batches
    /// * `proteins_memory_limit` - Fraction of free RAM allowed for keeping proteins in memory;
    ///   above this, proteins are re-read from the database instead
    /// * `skip_proteins` - Skip inserting proteins (assumes they already exist in the database)
    /// * `skip_protein_associations` - Skip collecting protein associations per peptide
    /// * `skip_taxonomies` - Skip collecting taxonomies per peptide
    /// * `num_threads` - Number of concurrent worker threads
    /// * `taxonomy_dump_file_path` - Optional NCBI taxonomy dump to build the taxonomy tree from
    /// * `resolve_isoforms` - Whether to resolve isoforms from alternate products
    /// * `scratch_dir` - Directory for the mass-index build's scratch files
    /// * `tui` - Optional TUI handle to report progress metrics to
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<Client>,
        comment: Option<String>,
        protein_file_paths: &'a [PathBuf],
        protease: Protease,
        batch_size_limit: NonZeroUsize,
        concurrent_batch_size: NonZeroUsize,
        proteins_memory_limit: f64,
        skip_proteins: bool,
        skip_protein_associations: bool,
        skip_taxonomies: bool,
        num_threads: NonZeroUsize,
        taxonomy_dump_file_path: Option<PathBuf>,
        resolve_isoforms: bool,
        scratch_dir: PathBuf,
        tui: Option<&'a TuiHandle>,
    ) -> Self {
        Self {
            client,
            comment,
            protein_file_paths,
            batch_size_limit,
            concurrent_batch_size,
            proteins_memory_limit,
            skip_proteins,
            skip_protein_associations,
            skip_taxonomies,
            num_threads,
            tui,
            taxonomy_dump_file_path,
            resolve_isoforms,
            scratch_dir,
            protease: Arc::new(protease),
        }
    }

    /// Runs the full build: inserts (or reuses) proteins, builds the mass index, digests
    /// peptides and upserts them, persists the resulting configuration as a blob, and builds
    /// the taxonomy tree if a dump file was given.
    pub async fn start(&self) -> Result<RuntimeConfiguration, Error> {
        // 1. set insert proteins or get access to them
        let (protein_ctr, proteins_size) = if !self.skip_proteins {
            if let Some(tui) = &self.tui {
                tui.add_metric(MetricConfig::rate(
                    crate::protein_table::INSERTED_PROTEINS_METRIC,
                    "Inserted proteins",
                ));
            }

            let (protein_ctr, proteins_size) = self.build_db_proteins().await?;
            if let Some(tui) = &self.tui {
                tui.remove_metric(crate::protein_table::INSERTED_PROTEINS_METRIC);
            }

            (protein_ctr, proteins_size)
        } else {
            let stats_table = StatsTable::new(self.client.clone());
            (
                stats_table
                    .select_protein_count()
                    .await?
                    .ok_or(Error::MissingProteinCount)?,
                stats_table
                    .select_proteins_size()
                    .await?
                    .ok_or(Error::MissingProteinsSize)?,
            )
        };

        let allowed_usable_memory = self.allowed_usable_memory();
        // needed memory is proteins size + an Arc per protein for cheap cloning
        let needed_memory = proteins_size
            + (std::mem::size_of::<Arc<Protein>>() + std::mem::size_of::<i32>()) * protein_ctr;

        let protein_access: Arc<Box<dyn IsProteinAccess>> = if needed_memory
            <= allowed_usable_memory
        {
            tracing::info!(
                "Keeping proteins in memory. Needed memory: {} MB, allowed free memory limit: {} MB",
                needed_memory / (1024 * 1024),
                allowed_usable_memory / (1024 * 1024)
            );
            if let Some(tui) = &self.tui {
                tui.add_metric(MetricConfig::progress(
                    IN_MEMORY_PROTEIN_ACCESS_BUILD_PROGRESS,
                    "Load proteins into memory",
                    protein_ctr as f64,
                ));
                tui.add_metric(MetricConfig::rate(
                    IN_MEMORY_PROTEIN_ACCESS_BUILD_PROGRESS,
                    "Loaded proteins /s",
                ));
            }

            let in_memory_access = InMemoryProteinAccess::new(self.client.clone()).await?;

            if let Some(tui) = &self.tui {
                tui.remove_metric(IN_MEMORY_PROTEIN_ACCESS_BUILD_PROGRESS);
            }

            Arc::new(Box::new(in_memory_access))
        } else {
            tracing::info!(
                "Not keeping proteins in memory. Needed memory: {} MB, allowed free memory limit: {} MB",
                needed_memory / (1024 * 1024),
                allowed_usable_memory / (1024 * 1024)
            );
            Arc::new(Box::new(DatabaseProteinAccess::new(self.client.clone())))
        };

        // 2. step create mass to protein index
        if let Some(tui) = &self.tui {
            tui.add_metric(MetricConfig::progress(
                crate::mass_index::SCATTER_PROGRESS_METRIC,
                crate::mass_index::SCATTER_PROGRESS_METRIC,
                protein_ctr as f64,
            ));
            tui.add_metric(MetricConfig::counter(
                crate::mass_index::FINALIZE_PROGRESS_METRIC,
                crate::mass_index::FINALIZE_PROGRESS_METRIC,
            ));
        }
        let mass_index = self.build_db_mass_index(protein_access.clone()).await?;
        if let Some(tui) = &self.tui {
            tui.remove_metric(crate::mass_index::SCATTER_PROGRESS_METRIC);
            tui.remove_metric(crate::mass_index::FINALIZE_PROGRESS_METRIC);
        }

        // 5. go through masses and digest the proteins collect distinct peptides and upsert them with proteins
        if let Some(tui) = &self.tui {
            tui.add_metric(MetricConfig::progress(
                crate::peptide_table::PROGRESS_METRIC,
                crate::peptide_table::PROGRESS_METRIC,
                mass_index.num_protein_associations() as f64,
            ));
            tui.add_metric(MetricConfig::counter(
                crate::peptide_table::INSERTED_PEPTIDES_METRIC,
                crate::peptide_table::INSERTED_PEPTIDES_METRIC,
            ));
        }

        let mass_to_partitions_map = self.build_db_peptides(protein_access, mass_index).await?;
        if let Some(tui) = &self.tui {
            tui.remove_metric(crate::peptide_table::PROGRESS_METRIC);
            tui.remove_metric(crate::peptide_table::INSERTED_PEPTIDES_METRIC);
        }
        let configuration = RuntimeConfiguration::new(
            self.comment.clone(),
            mass_to_partitions_map,
            self.protease.as_ref().clone(),
        );

        BlobTable::insert(
            self.client.as_ref(),
            &configuration,
            self.concurrent_batch_size,
        )
        .await?;

        if let Some(tui) = &self.tui {
            tui.add_metric(MetricConfig::counter(
                crate::taxonomy_rank_table::INSERTED_RANKS_METRIC,
                crate::taxonomy_rank_table::INSERTED_RANKS_METRIC,
            ));
            tui.add_metric(MetricConfig::counter(
                crate::taxonomy_table::INSERTED_TAXONOMIES_METRIC,
                crate::taxonomy_table::INSERTED_TAXONOMIES_METRIC,
            ));
        }

        self.build_taxonomy_tree().await?;

        if let Some(tui) = &self.tui {
            tui.remove_metric(crate::taxonomy_rank_table::INSERTED_RANKS_METRIC);
            tui.remove_metric(crate::taxonomy_table::INSERTED_TAXONOMIES_METRIC);
        }

        Ok(configuration)
    }

    async fn build_db_proteins(&self) -> Result<(usize, usize), Error> {
        let now = std::time::Instant::now();
        let (protein_ctr, proteins_size) = ProteinTable::new(self.client.clone())
            .build(
                self.protein_file_paths.iter(),
                self.concurrent_batch_size,
                self.num_threads,
                self.resolve_isoforms,
            )
            .await?;
        tracing::info!(
            "db proteins: time = {:.2?} s; #proteins = {protein_ctr}",
            now.elapsed().as_secs_f64(),
        );
        let stats_table = StatsTable::new(self.client.clone());

        stats_table.upsert_protein_count(protein_ctr).await?;
        stats_table.upsert_proteins_size(proteins_size).await?;

        Ok((protein_ctr, proteins_size))
    }

    async fn build_db_mass_index(
        &self,
        protein_access: Arc<Box<dyn IsProteinAccess>>,
    ) -> Result<MassIndex, Error> {
        let now = std::time::Instant::now();
        let memory_budget = self.mass_index_sort_budget();
        tracing::info!(
            "db mass index: sort budget = {} MB; scratch dir = {}",
            memory_budget / (1024 * 1024),
            self.scratch_dir.display()
        );
        let index = MassIndex::build(
            protein_access,
            self.protease.clone(),
            self.num_threads,
            self.scratch_dir.clone(),
            memory_budget,
        )
        .await?;
        StatsTable::new(self.client.clone())
            .upsert_mass_count(index.len())
            .await?;
        tracing::info!(
            "db mass index: time = {:.2?} s; #masses = {}; metadata size: {:.2?} MB",
            now.elapsed().as_secs_f64(),
            index.len(),
            index.size() / (1024 * 1024)
        );

        Ok(index)
    }

    /// Auto-derive the per-bucket sort budget for the mass index. Read at stage-2 start, so
    /// `available_memory` already reflects RAM left after protein loading. Capped so a big machine
    /// does not size one giant bucket (which would reproduce the old peak); the available-memory
    /// term only pulls the budget lower on small machines.
    fn mass_index_sort_budget(&self) -> usize {
        const CAP: usize = 4 * 1024 * 1024 * 1024; // 4 GiB
        const SAFETY: f64 = 0.5;
        const FLOOR: usize = 64 * 1024 * 1024; // 64 MiB
        let mut sys = System::new_all();
        sys.refresh_all();
        let available = sys.available_memory() as f64;
        // FLOOR < CAP always, so clamp cannot panic.
        ((available * SAFETY) as usize).clamp(FLOOR, CAP)
    }

    async fn build_db_peptides(
        &self,
        protein_access: Arc<Box<dyn IsProteinAccess>>,
        mass_index: MassIndex,
    ) -> Result<MassPartitionMap, Error> {
        let now = std::time::Instant::now();
        let (peptide_ctr, partition_ranges) = PeptideTable::new(self.client.clone())
            .build_concurrently(
                protein_access,
                self.skip_protein_associations,
                self.skip_taxonomies,
                self.protease.clone(),
                self.batch_size_limit,
                self.num_threads,
                Arc::new(mass_index),
            )
            .await?;
        tracing::info!("db peptides = {:.2?} s;", now.elapsed().as_secs_f64(),);
        StatsTable::new(self.client.clone())
            .upsert_peptide_count(peptide_ctr)
            .await?;

        Ok(MassPartitionMap::new(partition_ranges))
    }

    fn allowed_usable_memory(&self) -> usize {
        let mut sys = System::new_all();
        sys.refresh_all();
        (sys.available_memory() as f64 * self.proteins_memory_limit) as usize
    }

    /// Reads the configured NCBI taxonomy dump (if any) and builds the taxonomy ranks and
    /// taxonomies tables from it; a no-op if no dump file was configured.
    pub async fn build_taxonomy_tree(&self) -> Result<(), Error> {
        if let Some(taxonomy_dump_file_path) = &self.taxonomy_dump_file_path {
            let taxonomy_tree = TaxonomyReader::new(taxonomy_dump_file_path)
                .map_err(|err| Error::TaxonomyTreeReadError(format!("{}", err)))?
                .read()
                .map_err(|err| Error::TaxonomyTreeReadError(format!("{}", err)))?;

            let now = std::time::Instant::now();

            let ranks = taxonomy_tree
                .get_ranks()
                .iter()
                .map(TaxonomyRank::try_from)
                .collect::<Result<Vec<TaxonomyRank>, _>>()
                .map_err(Error::from)?;

            let ranks_len = ranks.len();

            TaxonomyRankTable::new(self.client.clone())
                .build(ranks)
                .await?;

            tracing::info!(
                "db taxonomy ranks: time = {:.2?} s; #ranks = {}",
                now.elapsed().as_secs_f64(),
                ranks_len
            );

            let now = std::time::Instant::now();

            let taxonomies = taxonomy_tree
                .get_taxonomies()
                .iter()
                .map(Taxonomy::try_from)
                .collect::<Result<Vec<Taxonomy>, _>>()
                .map_err(Error::from)?;

            let tax_len = taxonomies.len();

            drop(taxonomy_tree);

            TaxonomyTable::new(self.client.clone())
                .build(taxonomies)
                .await?;

            tracing::info!(
                "db taxonomies: time = {:.2?} s; #taxonomies = {}",
                now.elapsed().as_secs_f64(),
                tax_len
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Four partitions tiling `[100, 300]` and `[400, 500]`, with mass 200 split across three of
    /// them (a mass whose peptides overflowed one columnar stripe) and a gap between 300 and 400.
    fn partitioning() -> MassPartitionMap {
        MassPartitionMap::new(vec![
            // Deliberately unsorted — `new` is responsible for ordering.
            PartitionRange::new(400, 500, 13),
            PartitionRange::new(200, 300, 12),
            PartitionRange::new(100, 200, 10),
            PartitionRange::new(200, 200, 11),
        ])
    }

    fn partitions_for(map: &MassPartitionMap, mass: i64) -> Vec<i64> {
        let mut partitions: Vec<i64> = map.partition_by_mass(mass).collect();
        partitions.sort_unstable();
        partitions
    }

    fn partitions_in(map: &MassPartitionMap, lower: i64, upper: i64) -> Vec<i64> {
        let mut partitions: Vec<i64> = map.partitions_by_mass_range(lower, upper).collect();
        partitions.sort_unstable();
        partitions
    }

    #[test]
    fn test_new_sorts_ranges_ascending() {
        let map = partitioning();
        let los: Vec<i64> = map.ranges().iter().map(|range| range.lo()).collect();
        let his: Vec<i64> = map.ranges().iter().map(|range| range.hi()).collect();

        assert_eq!(los, vec![100, 200, 200, 400]);
        // `hi` must come out non-decreasing — the lookups binary-search on it.
        assert_eq!(his, vec![200, 200, 300, 500]);
    }

    #[test]
    fn test_mass_in_a_single_partition() {
        assert_eq!(partitions_for(&partitioning(), 150), vec![10]);
    }

    #[test]
    fn test_mass_split_across_three_partitions() {
        // 200 is the shared boundary mass of 10, 11 and 12 — all three must be named.
        assert_eq!(partitions_for(&partitioning(), 200), vec![10, 11, 12]);
    }

    #[test]
    fn test_mass_inside_a_range_gap_names_the_spanning_partition() {
        // 250 need never have been stored; it sits inside partition 12's span, so it is named. The
        // caller still qualifies on `mass`, so the extra query just returns nothing.
        assert_eq!(partitions_for(&partitioning(), 250), vec![12]);
    }

    #[test]
    fn test_mass_between_two_partitions_names_nothing() {
        // 350 is outside every span, unlike 250 above.
        assert!(partitions_for(&partitioning(), 350).is_empty());
    }

    #[test]
    fn test_range_query_spanning_everything() {
        assert_eq!(
            partitions_in(&partitioning(), 150, 450),
            vec![10, 11, 12, 13]
        );
    }

    #[test]
    fn test_range_query_straddling_a_shared_boundary_mass() {
        assert_eq!(partitions_in(&partitioning(), 199, 201), vec![10, 11, 12]);
    }

    #[test]
    fn test_range_query_inside_the_gap_between_partitions() {
        assert!(partitions_in(&partitioning(), 310, 390).is_empty());
    }

    #[test]
    fn test_range_queries_outside_the_map() {
        let map = partitioning();
        assert!(partitions_in(&map, 0, 50).is_empty());
        assert!(partitions_in(&map, 600, 700).is_empty());
        // Touching the very first and very last mass still resolves.
        assert_eq!(partitions_in(&map, 0, 100), vec![10]);
        assert_eq!(partitions_in(&map, 500, 700), vec![13]);
    }

    #[test]
    fn test_empty_map() {
        let map = MassPartitionMap::new(Vec::new());
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
        assert!(partitions_for(&map, 200).is_empty());
        assert!(partitions_in(&map, 0, i64::MAX).is_empty());
    }

    #[test]
    fn test_from_mass_partition_pairs_folds_to_one_range_per_partition() {
        // The historical per-mass shape: 200 lives in both partitions.
        let map = MassPartitionMap::from(HashMap::from_iter(vec![
            (100_i64, vec![1_i64]),
            (200, vec![1, 2]),
            (300, vec![2]),
        ]));

        assert_eq!(map.len(), 2, "one range per partition, not one per mass");
        assert_eq!(partitions_for(&map, 100), vec![1]);
        assert_eq!(partitions_for(&map, 200), vec![1, 2]);
        assert_eq!(partitions_for(&map, 300), vec![2]);
    }

    #[test]
    fn test_from_mass_partition_pairs_preserves_every_association() {
        // What `config verify` asserts against a migrated database, in miniature: every stored
        // (mass, partition) pair must still resolve through the folded range form.
        let per_mass: HashMap<i64, Vec<i64>> = HashMap::from_iter(vec![
            (10_i64, vec![1_i64]),
            (20, vec![1]),
            (30, vec![1, 2]),
            (40, vec![2]),
            (90, vec![3]),
        ]);
        let map = MassPartitionMap::from(per_mass.clone());

        for (mass, partitions) in per_mass {
            for partition in partitions {
                assert!(
                    map.partition_by_mass(mass).any(|p| p == partition),
                    "mass {mass} lost its association with partition {partition}"
                );
            }
        }
    }
}
