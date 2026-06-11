use std::{collections::HashMap, num::NonZeroUsize, path::PathBuf, sync::Arc};

use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, pin_mut, stream::BoxStream};
use macpepdb_tui::{MetricConfig, TuiHandle};
use metrics::counter;
use sysinfo::System;
use thiserror::Error;

use crate::{
    blob_table::BlobTable, client::Client, configuration::RuntimeConfiguration,
    mass_index::MassIndex, peptide_table::PeptideTable, protease::Protease, protein::Protein,
    protein_table::ProteinTable, stats_table::StatsTable,
};

pub static APPROPRIATE_PROTEIN_ACCESS_PROGRESS_METRIC: &str =
    "protein_table::build::appropriate_protein_access";

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
    #[error("Peptide table error in database build: {0}")]
    PeptideTable(Box<crate::peptide_table::Error>),
    #[error("Protein table error in database build: {0}")]
    ProteinTable(Box<crate::protein_table::Error>),
    #[error("Stats table error in database build: {0}")]
    StatsTable(Box<crate::stats_table::Error>),
}

into_thiserror_boxed!(crate::blob_table::Error, Error, BlobTable);
into_thiserror_boxed!(crate::mass_index::Error, Error, MassIndex);
into_thiserror_boxed!(crate::peptide_table::Error, Error, PeptideTable);
into_thiserror_boxed!(crate::protein_table::Error, Error, ProteinTable);
into_thiserror_boxed!(crate::stats_table::Error, Error, StatsTable);

type BoxedPeptideStreamFuture<'a> =
    BoxFuture<'a, Result<BoxStream<'a, Result<Arc<Protein>, Error>>, Error>>;

pub trait IsProteinAccess: Send + Sync {
    fn by_ids<'a>(&'a self, ids: &'a [i32]) -> BoxedPeptideStreamFuture<'a>;
    fn all<'a>(&'a self) -> BoxedPeptideStreamFuture<'a>;
    fn count<'a>(&'a self) -> BoxFuture<'a, Result<usize, Error>>;
    fn ids<'a>(&'a self) -> BoxFuture<'a, Result<Vec<i32>, Error>>;
}

pub struct DatabaseProteinAccess {
    protein_table: ProteinTable,
}

impl DatabaseProteinAccess {
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

pub struct InMemoryProteinAccess {
    proteins: HashMap<i32, Arc<Protein>>,
}

impl InMemoryProteinAccess {
    pub async fn new(client: Arc<Client>) -> Result<Self, Error> {
        let proteins = ProteinTable::new(client)
            .select_all()
            .await?
            .map(|protein_result| {
                protein_result
                    .map(|protein| (protein.id().unwrap(), Arc::new(protein)))
                    .map_err(Error::from)
            })
            .try_collect::<HashMap<i32, Arc<Protein>>>()
            .await?;

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

pub struct DatabaseBuild<'a> {
    client: Arc<Client>,
    protein_file_paths: &'a [PathBuf],
    protease: Arc<Protease>,
    batch_size_limit: NonZeroUsize,
    concurrent_batch_size: NonZeroUsize,
    proteins_memory_limit: f64,
    skip_proteins: bool,
    skip_protein_associations: bool,
    skip_taxonomies: bool,
    num_threads: NonZeroUsize,
    tui: Option<&'a TuiHandle>,
}

impl<'a> DatabaseBuild<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<Client>,
        protein_file_paths: &'a [PathBuf],
        protease: Protease,
        batch_size_limit: NonZeroUsize,
        concurrent_batch_size: NonZeroUsize,
        proteins_memory_limit: f64,
        skip_proteins: bool,
        skip_protein_associations: bool,
        skip_taxonomies: bool,
        num_threads: NonZeroUsize,
        tui: Option<&'a TuiHandle>,
    ) -> Self {
        Self {
            client,
            protein_file_paths,
            batch_size_limit,
            concurrent_batch_size,
            proteins_memory_limit,
            skip_proteins,
            skip_protein_associations,
            skip_taxonomies,
            num_threads,
            tui,
            protease: Arc::new(protease),
        }
    }

    pub async fn start(&self) -> Result<RuntimeConfiguration, Error> {
        // 1. set insert proteins or get access to them
        let (protein_ctr, protein_access) = if !self.skip_proteins {
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

            let mut sys = System::new_all();
            sys.refresh_all();
            let allowed_usable_memory =
                (sys.available_memory() as f64 * self.proteins_memory_limit) as usize;
            // needed memory is proteins size + an Arc per protein for cheap cloning
            let needed_memory = proteins_size
                + (std::mem::size_of::<Arc<Protein>>() + std::mem::size_of::<i32>()) * protein_ctr;

            let protein_access: Box<dyn IsProteinAccess> = if needed_memory <= allowed_usable_memory
            {
                tracing::info!(
                    "Keeping proteins in memory. Needed memory: {} MB, allowed free memory limit: {} MB",
                    needed_memory / (1024 * 1024),
                    allowed_usable_memory / (1024 * 1024)
                );
                Box::new(InMemoryProteinAccess::new(self.client.clone()).await?)
            } else {
                tracing::info!(
                    "Not keeping proteins in memory. Needed memory: {} MB, allowed free memory limit: {} MB",
                    needed_memory / (1024 * 1024),
                    allowed_usable_memory / (1024 * 1024)
                );
                Box::new(DatabaseProteinAccess::new(self.client.clone()))
            };

            (protein_ctr, protein_access)
        } else {
            if let Some(tui) = &self.tui {
                tui.add_metric(MetricConfig::rate(
                    crate::database_build::APPROPRIATE_PROTEIN_ACCESS_PROGRESS_METRIC,
                    "Processed proteins",
                ));
            }

            let (proteins_ctr, protein_access) = self.get_appropriate_protein_access().await?;

            if let Some(tui) = &self.tui {
                tui.remove_metric(
                    crate::database_build::APPROPRIATE_PROTEIN_ACCESS_PROGRESS_METRIC,
                );
            }

            (proteins_ctr, protein_access)
        };

        let protein_access = Arc::new(protein_access);

        // 2. step create mass to protein index
        if let Some(tui) = &self.tui {
            tui.add_metric(MetricConfig::progress(
                crate::mass_index::PROGRESS_METRIC,
                crate::mass_index::PROGRESS_METRIC,
                protein_ctr as f64,
            ));
        }
        let mass_index = self.build_db_mass_index(protein_access.clone()).await?;
        if let Some(tui) = &self.tui {
            tui.remove_metric(crate::mass_index::PROGRESS_METRIC);
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
            tui.add_metric(MetricConfig::gauge(
                crate::peptide_table::QUEUE_METRIC,
                crate::peptide_table::QUEUE_METRIC,
            ));
        }

        let mass_to_partitions_map = self.build_db_peptides(protein_access, mass_index).await?;
        if let Some(tui) = &self.tui {
            tui.remove_metric(crate::peptide_table::PROGRESS_METRIC);
            tui.remove_metric(crate::peptide_table::INSERTED_PEPTIDES_METRIC);
            tui.remove_metric(crate::peptide_table::QUEUE_METRIC);
        }
        let configuration =
            RuntimeConfiguration::new(mass_to_partitions_map, self.protease.as_ref().clone());

        BlobTable::insert(
            self.client.as_ref(),
            &configuration,
            self.concurrent_batch_size,
        )
        .await?;

        Ok(configuration)
    }

    async fn build_db_proteins(&self) -> Result<(usize, usize), Error> {
        let now = std::time::Instant::now();
        let (protein_ctr, proteins_size) = ProteinTable::new(self.client.clone())
            .build(
                self.protein_file_paths.iter(),
                self.concurrent_batch_size,
                self.num_threads,
            )
            .await?;
        tracing::info!(
            "db proteins: time = {:.2?} s; #proteins = {protein_ctr}",
            now.elapsed().as_secs_f64(),
        );
        StatsTable::new(self.client.clone())
            .upsert_protein_count(protein_ctr)
            .await?;

        Ok((protein_ctr, proteins_size))
    }

    async fn build_db_mass_index(
        &self,
        protein_access: Arc<Box<dyn IsProteinAccess>>,
    ) -> Result<MassIndex, Error> {
        let now = std::time::Instant::now();
        let index = MassIndex::build(
            self.client.clone(),
            protein_access,
            self.protease.clone(),
            self.num_threads,
        )
        .await?;
        tracing::info!(
            "db mass index: time = {:.2?} s; #masses = {}; size: {:.2?} MB",
            now.elapsed().as_secs_f64(),
            index.len(),
            index.size() / (1024 * 1024)
        );

        Ok(index)
    }

    async fn build_db_peptides(
        &self,
        protein_access: Arc<Box<dyn IsProteinAccess>>,
        mass_index: MassIndex,
    ) -> Result<HashMap<i64, Vec<i64>>, Error> {
        let now = std::time::Instant::now();
        let (peptide_ctr, mass_to_partitions_map) = PeptideTable::new(self.client.clone())
            .build_concurrently(
                protein_access,
                self.skip_protein_associations,
                self.skip_taxonomies,
                self.protease.clone(),
                self.batch_size_limit,
                self.num_threads,
                mass_index,
            )
            .await?;
        tracing::info!("db peptides = {:.2?} s;", now.elapsed().as_secs_f64(),);
        StatsTable::new(self.client.clone())
            .upsert_peptide_count(peptide_ctr)
            .await?;

        Ok(mass_to_partitions_map)
    }

    fn allowed_usable_memory(&self) -> usize {
        let mut sys = System::new_all();
        sys.refresh_all();
        (sys.available_memory() as f64 * self.proteins_memory_limit) as usize
    }

    pub async fn get_appropriate_protein_access(
        &self,
    ) -> Result<(usize, Box<dyn IsProteinAccess>), Error> {
        let protein_table = ProteinTable::new(self.client.clone());
        tracing::info!("Counting proteins in the database...");
        let proteins_count = protein_table.count().await?;
        tracing::info!("Proteins count: {}", proteins_count);
        let mut proteins: HashMap<i32, Arc<Protein>> = HashMap::with_capacity(proteins_count);

        let allowed_usable_memory = self.allowed_usable_memory();

        tracing::info!(
            "Allowed usable memory for proteins: {} MB",
            allowed_usable_memory as f64 / 1000.0 / 1000.0,
        );

        let processed_proteins_metrics = counter!(APPROPRIATE_PROTEIN_ACCESS_PROGRESS_METRIC);

        let proteins_stream = protein_table.select_all().await?.peekable();

        pin_mut!(proteins_stream);

        let mut proteins_size = 0_usize;
        while let Some(protein_result) = proteins_stream.as_mut().next().await {
            let protein = protein_result?;
            proteins_size += protein.size() + std::mem::size_of::<i32>();
            proteins.insert(protein.id().unwrap(), Arc::new(protein));
            processed_proteins_metrics.increment(1);

            if let Some(Ok(protein)) = proteins_stream.as_mut().peek().await
                && proteins_size
                    + protein.size()
                    + std::mem::size_of::<i32>()
                    + std::mem::size_of::<Arc<Protein>>()
                    > allowed_usable_memory
            {
                tracing::warn!(
                    "Allowed usable memory exceeded, falling back to database based protein access.",
                );
                return Ok((
                    proteins_count,
                    Box::new(DatabaseProteinAccess::new(self.client.clone()))
                        as Box<dyn IsProteinAccess>,
                ));
            }
        }

        tracing::info!("All proteins loaded into memory.",);
        Ok((
            proteins_count,
            Box::new(InMemoryProteinAccess { proteins }) as Box<dyn IsProteinAccess>,
        ))
    }
}
