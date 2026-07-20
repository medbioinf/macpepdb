use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

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

/// Maps a peptide mass to the partition(s) that hold peptides of that mass. Split into a dense,
/// sorted array for the common case of one partition per mass (`single`) and a sparse map for
/// masses that overflow into several partitions (`overflow`), so range queries over `single` can
/// binary-search instead of scanning every mass.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MassPartitionMap {
    /// Masses partition array
    single: Vec<(i64, i64)>,
    /// Remaining
    overflow: BTreeMap<i64, Vec<i64>>,
}

impl MassPartitionMap {
    /// Returns the `(mass, partition)` pairs for every mass in `[lower_mass, upper_mass]`.
    ///
    /// # Arguments
    /// * `lower_mass` - Lower bound of the mass range (inclusive)
    /// * `upper_mass` - Upper bound of the mass range (inclusive)
    pub fn partitions_by_mass_range(
        &self,
        lower_mass: i64,
        upper_mass: i64,
    ) -> impl Iterator<Item = (i64, i64)> {
        let start = self.single.partition_point(|(m, _)| *m < lower_mass);
        let end = self.single.partition_point(|(m, _)| *m <= upper_mass);

        self.single[start..end].iter().cloned().chain(
            self.overflow
                .range(lower_mass..=upper_mass)
                .flat_map(|(&mass, partitions)| {
                    partitions.iter().map(move |partition| (mass, *partition))
                }),
        )
    }

    /// Returns the `(mass, partition)` pairs for a single, exact `mass`.
    ///
    /// # Arguments
    /// * `mass` - The exact mass to look up partitions for
    pub fn partition_by_mass(&self, mass: i64) -> impl Iterator<Item = (i64, i64)> {
        let start = self.single.partition_point(|(m, _)| *m < mass);
        let end = self.single.partition_point(|(m, _)| *m <= mass);

        self.single[start..end].iter().cloned().chain(
            self.overflow
                .get(&mass)
                .map(|partitions| partitions.iter().map(move |partition| (mass, *partition)))
                .into_iter()
                .flatten(),
        )
    }
}

impl From<HashMap<i64, Vec<i64>>> for MassPartitionMap {
    fn from(map: HashMap<i64, Vec<i64>>) -> Self {
        let capacity = map
            .iter()
            .filter(|(_, partitions)| partitions.len() == 1)
            .count();

        let mut single = Vec::with_capacity(capacity);
        let mut overflow = BTreeMap::new();

        for (mass, mut partitions_vec) in map {
            if partitions_vec.len() == 1 {
                single.push((mass, partitions_vec[0]));
            } else {
                partitions_vec.shrink_to_fit();
                overflow.insert(mass, partitions_vec);
            }
        }

        single.sort_unstable_by_key(|(m, _)| *m);

        Self { single, overflow }
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
        let (peptide_ctr, mass_to_partitions_map) = PeptideTable::new(self.client.clone())
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

        Ok(MassPartitionMap::from(mass_to_partitions_map))
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
