use std::{collections::HashMap, sync::Arc};

use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream::BoxStream};
use thiserror::Error;

use crate::{client::Client, protein::Protein, protein_table::ProteinTable};

#[derive(Debug, Error)]
pub enum Error {
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
}
