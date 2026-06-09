use std::{collections::HashMap, sync::Arc};

use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, pin_mut, stream::BoxStream};
use metrics::counter;
use sysinfo::System;
use thiserror::Error;

use crate::{client::Client, protein::Protein, protein_table::ProteinTable};

pub static APPROPRIATE_PROTEIN_ACCESS_PROGRESS_METRIC: &str =
    "protein_table::build::appropriate_protein_access";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Default, should not occure anywhere")]
    Default,
    #[error("Missing protein ID for `{0}`, means it does not come from the database")]
    MissingProteinId(String),
    #[error("Unable not load next protein from stream: {0}")]
    NextProtein(Box<scylla::errors::NextRowError>),
    #[error("Protein access error: {0}")]
    ProteinTable(Box<crate::protein_table::Error>),
}

into_thiserror_boxed!(scylla::errors::NextRowError, Error, NextProtein);
into_thiserror_boxed!(crate::protein_table::Error, Error, ProteinTable);

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
                .select(Some("WHERE id IN ?"), (ids,))
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
                .select(None, ())
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
            .select(None, ())
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

fn get_allowed_usable_memory(proteins_memory_limit: f64) -> usize {
    let mut sys = System::new_all();
    sys.refresh_all();
    (sys.available_memory() as f64 * proteins_memory_limit) as usize
}

pub async fn get_appropriate_protein_access(
    client: Arc<Client>,
    proteins_memory_limit: f64,
) -> Result<(usize, Box<dyn IsProteinAccess>), Error> {
    let protein_table = ProteinTable::new(client.clone());
    tracing::info!("Counting proteins in the database...");
    let proteins_count = protein_table.count().await?;
    tracing::info!("Proteins count: {}", proteins_count);
    let mut proteins: HashMap<i32, Arc<Protein>> = HashMap::with_capacity(proteins_count);

    let allowed_usable_memory = get_allowed_usable_memory(proteins_memory_limit);

    tracing::info!(
        "Allowed usable memory for proteins: {} MB",
        allowed_usable_memory as f64 / 1000.0 / 1000.0,
    );

    let processed_proteins_metrics = counter!(APPROPRIATE_PROTEIN_ACCESS_PROGRESS_METRIC);

    let proteins_stream = ProteinTable::new(client.clone())
        .select(None, ())
        .await?
        .peekable();

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
                Box::new(DatabaseProteinAccess::new(client)) as Box<dyn IsProteinAccess>,
            ));
        }
    }

    tracing::info!("All proteins loaded into memory.",);
    Ok((
        proteins_count,
        Box::new(InMemoryProteinAccess { proteins }) as Box<dyn IsProteinAccess>,
    ))
}
