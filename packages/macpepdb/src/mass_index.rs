use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
    fs::File,
    hash::{Hash, Hasher},
    io::BufReader,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use fallible_iterator::FallibleIterator;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uniprot_reader::indexer::Offset;

use crate::protease::Protease;

#[derive(Debug, Error)]
pub enum Error {
    #[error("UnipotReader: {0}")]
    UnprotReader(#[from] uniprot_reader::reader::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Protease error: {0}")]
    Protease(String),
    #[error("Protein reader thread error: {0}")]
    ProteinReaderThread(String),
    #[error("Indexing stopped unexpectedly before finishing the protein processing ")]
    EarlyIndexThreadStop,
    #[error("Unable to insert entry: {0}")]
    InsertEntry(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Entry {
    file_idx: usize,
    offset: Offset,
}

impl Entry {
    pub fn file_idx(&self) -> usize {
        self.file_idx
    }

    pub fn offset(&self) -> &Offset {
        &self.offset
    }
}

impl Hash for Entry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_idx.hash(state);
        self.offset.start().hash(state);
        self.offset.end().hash(state);
    }
}

impl From<Entry> for Vec<u8> {
    fn from(entry: Entry) -> Self {
        let mut vec =
            Vec::with_capacity(std::mem::size_of::<usize>() + 2 * std::mem::size_of::<u64>());
        vec.extend_from_slice(&entry.file_idx.to_le_bytes());
        vec.extend_from_slice(&entry.offset.start().to_le_bytes());
        vec.extend_from_slice(&entry.offset.end().to_le_bytes());
        vec
    }
}

impl From<Vec<u8>> for Entry {
    fn from(bytes: Vec<u8>) -> Self {
        let file_idx =
            usize::from_le_bytes(bytes[0..std::mem::size_of::<usize>()].try_into().unwrap());
        let offset_start = u64::from_le_bytes(
            bytes[std::mem::size_of::<usize>()
                ..std::mem::size_of::<usize>() + std::mem::size_of::<u64>()]
                .try_into()
                .unwrap(),
        );
        let offset_end = u64::from_le_bytes(
            bytes[std::mem::size_of::<usize>() + std::mem::size_of::<u64>()..]
                .try_into()
                .unwrap(),
        );
        Entry {
            file_idx,
            offset: Offset::new(offset_start, offset_end),
        }
    }
}

pub trait IsMassIndexMap: Debug + Eq + PartialEq {
    type Error: std::error::Error + Sync + Send;

    fn len(&self) -> impl Future<Output = Result<usize, Self::Error>>;
    fn is_empty(&self) -> impl Future<Output = Result<bool, Self::Error>>;

    fn masses<'a>(
        &'a self,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<Cow<'a, i64>, Self::Error>>, Self::Error>>;

    #[allow(clippy::type_complexity)]
    fn entries<'a>(
        &'a self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<(Cow<'a, i64>, Cow<'a, HashSet<Entry>>), Self::Error>>,
            Self::Error,
        >,
    >;

    fn entries_len(&self) -> impl Future<Output = Result<usize, Self::Error>>;

    fn insert(&mut self, mass: i64, entry: Entry) -> impl Future<Output = Result<(), Self::Error>>;

    fn get<'a>(
        &'a self,
        mass: i64,
    ) -> impl Future<Output = Result<Option<Cow<'a, HashSet<Entry>>>, Self::Error>>;
}

#[derive(Debug, Error)]
pub enum MassIndexHashMapError {}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct MassIndexHashMap(HashMap<i64, HashSet<Entry>>);

impl IsMassIndexMap for MassIndexHashMap {
    type Error = MassIndexHashMapError;

    async fn len(&self) -> Result<usize, Self::Error> {
        Ok(self.0.len())
    }

    async fn is_empty(&self) -> Result<bool, Self::Error> {
        Ok(self.0.is_empty())
    }

    async fn masses<'a>(
        &'a self,
    ) -> Result<impl Stream<Item = Result<Cow<'a, i64>, Self::Error>>, Self::Error> {
        Ok(futures::stream::iter(
            self.0.keys().map(Cow::Borrowed).map(Ok),
        ))
    }

    async fn entries<'a>(
        &'a self,
    ) -> Result<
        impl Stream<Item = Result<(Cow<'a, i64>, Cow<'a, HashSet<Entry>>), Self::Error>>,
        Self::Error,
    > {
        Ok(futures::stream::iter(
            self.0
                .iter()
                .map(|(mass, entries)| (Cow::Borrowed(mass), Cow::Borrowed(entries)))
                .map(Ok),
        ))
    }

    async fn entries_len(&self) -> Result<usize, Self::Error> {
        Ok(self.0.values().map(|entries| entries.len()).sum())
    }

    async fn insert(&mut self, mass: i64, entry: Entry) -> Result<(), Self::Error> {
        self.0
            .entry(mass) // Placeholder for mass, replace with actual mass calculation
            .or_default()
            .insert(entry);
        Ok(())
    }

    async fn get<'a>(&'a self, mass: i64) -> Result<Option<Cow<'a, HashSet<Entry>>>, Self::Error> {
        Ok(self.0.get(&mass).map(Cow::Borrowed))
    }
}

#[derive(Debug, Error)]
pub enum MassIndexDbMapError {
    #[error("Cassandra error: {0}")]
    Cassandra(#[from] scylla::errors::DbError),
    #[error("Unable to fetch page from database: {0}")]
    CqlPage(#[from] scylla::errors::PagerExecutionError),
    #[error("Type not checked out for returned rows: {0}")]
    CqlTypeCheck(#[from] scylla::errors::TypeCheckError),
    #[error("Unable to get next row from CQL stream: {0}")]
    CqlNextRowError(#[from] scylla::errors::NextRowError),
    #[error("Unable to prepare statement: {0}")]
    CqlPrepare(#[from] scylla::errors::PrepareError),
    #[error("Unable to execute statement: {0}")]
    CqlExecute(#[from] scylla::errors::ExecutionError),
    #[error("Unable to convert into rows: {0}")]
    CqlIntoRow(#[from] scylla::errors::IntoRowsResultError),
    #[error("Unable to get first row from query result: {0}")]
    CqlNoFirstRow(#[from] scylla::errors::MaybeFirstRowError),
}

#[derive(Debug)]
pub struct MassIndexDbMap(scylla::client::session::Session);

impl MassIndexDbMap {
    const TABLE_NAME: &'static str = "mass_index";

    pub fn new(session: scylla::client::session::Session) -> Self {
        Self(session)
    }
}

impl IsMassIndexMap for MassIndexDbMap {
    type Error = MassIndexDbMapError;

    async fn len(&self) -> Result<usize, Self::Error> {
        // Using count(*) in CQL-based databases is inefficient and will most likely result in timeouts
        // Streaming the masses and count them is safer.
        Ok(self.masses().await?.count().await)
    }

    async fn is_empty(&self) -> Result<bool, Self::Error> {
        // See len
        Ok(self.len().await? == 0)
    }

    async fn masses<'a>(
        &'a self,
    ) -> Result<impl Stream<Item = Result<Cow<'a, i64>, Self::Error>>, Self::Error> {
        Ok(self
            .0
            .query_iter(format!("SELECT mass FROM {}", Self::TABLE_NAME), ())
            .await?
            .rows_stream::<(i64,)>()?
            .map(|row| Ok(Cow::Owned(row?.0))))
    }

    async fn entries<'a>(
        &'a self,
    ) -> Result<
        impl Stream<Item = Result<(Cow<'a, i64>, Cow<'a, HashSet<Entry>>), Self::Error>>,
        Self::Error,
    > {
        Ok(self
            .0
            .query_iter(format!("SELECT mass, entrs FROM {}", Self::TABLE_NAME), ())
            .await?
            .rows_stream::<(i64, HashSet<Vec<u8>>)>()?
            .map(|row| {
                let row = row?;
                Ok((
                    Cow::Owned(row.0),
                    Cow::Owned(
                        row.1
                            .into_iter()
                            .map(Entry::from)
                            .collect::<HashSet<Entry>>(),
                    ),
                ))
            }))
    }

    async fn entries_len(&self) -> Result<usize, Self::Error> {
        Ok(self
            .0
            .query_iter(format!("SELECT entrs FROM {}", Self::TABLE_NAME), ())
            .await?
            .rows_stream::<(HashSet<Vec<u8>>,)>()?
            .try_fold(0, |acc, row| async move { Ok(acc + row.0.len()) })
            .await?)
    }

    async fn insert(&mut self, mass: i64, entry: Entry) -> Result<(), Self::Error> {
        let stmt = self
            .0
            .prepare(format!(
                "INSERT INTO {} (mass, entrs) VALUES (?, ?) IF NOT EXISTS",
                Self::TABLE_NAME
            ))
            .await?;

        let entries: HashSet<Vec<u8>> = HashSet::from_iter([Vec::<u8>::from(entry)]);

        self.0.execute_unpaged(&stmt, (mass, entries)).await?;

        Ok(())
    }

    async fn get<'a>(&'a self, mass: i64) -> Result<Option<Cow<'a, HashSet<Entry>>>, Self::Error> {
        let query_result = self
            .0
            .query_unpaged(
                format!(
                    "SELECT entrs FROM {} WHERE mass = ? limit 1",
                    Self::TABLE_NAME
                ),
                (mass,),
            )
            .await?
            .into_rows_result()?
            .maybe_first_row::<(HashSet<Vec<u8>>,)>()?;

        if let Some(row) = query_result {
            Ok(Some(Cow::Owned(
                row.0
                    .into_iter()
                    .map(Entry::from)
                    .collect::<HashSet<Entry>>(),
            )))
        } else {
            Ok(None)
        }
    }
}

impl Eq for MassIndexDbMap {}

impl PartialEq for MassIndexDbMap {
    fn eq(&self, _other: &Self) -> bool {
        // Woudl need async try_partial_equal. Need to come up with some kind of hash which is updated during build
        // and can be compoared.
        true
    }
}

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct MassIndex<M: IsMassIndexMap> {
    files: Vec<PathBuf>,
    map: M,
}

impl<M> MassIndex<M>
where
    M: IsMassIndexMap,
{
    pub fn new(files: Vec<PathBuf>, map: M) -> Self {
        Self { files, map }
    }

    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }

    pub async fn build(&mut self, protease: &Protease) -> Result<(), Error> {
        let file_paths = self.files.clone();

        for (path_idx, path) in file_paths.into_iter().enumerate() {
            let mut buf_reader = BufReader::new(File::open(path)?);

            let reader = uniprot_reader::reader::Reader::new(&mut buf_reader);
            for item in reader {
                let item = item?;
                #[allow(clippy::mutable_key_type)]
                let peptides = protease
                    .cleave(item.entry().sequence(), true)
                    .map_err(|err| Error::Protease(err.to_string()))?
                    .collect::<HashSet<_>>()
                    .map_err(|err| Error::Protease(err.to_string()))?;

                for peptide in peptides {
                    let mass = peptide.mass();
                    let entry = Entry {
                        file_idx: path_idx,
                        offset: item.offset().clone(),
                    };

                    self.insert(mass, entry)
                        .await
                        .map_err(|err| Error::InsertEntry(err.to_string()))?;
                }
            }
        }

        Ok(())
    }

    pub async fn build_multithreaded(
        &mut self,
        protease: &Protease,
        num_threads: usize,
    ) -> Result<(), Error> {
        let num_threads = std::cmp::min(num_threads, self.files.len());

        let file_paths_vec = self.files.clone();

        let file_path_queue = Arc::new(Mutex::new(VecDeque::from_iter(
            file_paths_vec.iter().cloned().enumerate(),
        )));
        let protease = Arc::new(protease.clone());

        let (sender, receiver) = std::sync::mpsc::channel::<(i64, Entry)>();

        let protein_reader_threads = (0..num_threads)
            .map(|_| {
                let file_path_queue = Arc::clone(&file_path_queue);
                let sender = sender.clone();
                let protease = protease.clone();

                std::thread::spawn(move || {
                    while let Some((path_idx, path)) = file_path_queue.lock().unwrap().pop_front() {
                        let mut buf_reader = BufReader::new(File::open(path).unwrap());
                        let reader = uniprot_reader::reader::Reader::new(&mut buf_reader);

                        for item in reader {
                            let item = item.unwrap();
                            #[allow(clippy::mutable_key_type)]
                            let peptides = protease
                                .cleave(item.entry().sequence(), true)
                                .unwrap()
                                .collect::<HashSet<_>>()
                                .unwrap();

                            for peptide in peptides {
                                let mass = peptide.mass();
                                let entry = Entry {
                                    file_idx: path_idx,
                                    offset: item.offset().clone(),
                                };

                                match sender.send((mass, entry)) {
                                    Ok(_) => (),
                                    Err(_) => return Err(Error::EarlyIndexThreadStop),
                                }
                            }
                        }
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();

        // drop original sender
        drop(sender);

        while let Ok((mass, entry)) = receiver.recv() {
            self.insert(mass, entry)
                .await
                .map_err(|err| Error::InsertEntry(err.to_string()))?;
        }

        for thread in protein_reader_threads.into_iter() {
            thread
                .join()
                .map_err(|err| Error::ProteinReaderThread(format!("{err:?}")))??;
        }

        Ok(())
    }
}

impl<M> Deref for MassIndex<M>
where
    M: IsMassIndexMap,
{
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<M> DerefMut for MassIndex<M>
where
    M: IsMassIndexMap,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    static PROTEINS_FILE_PATH: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
        PathBuf::from(std::env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data/some_human_proteins.uniprot.txt")
    });

    async fn test_create_singlethreaded() -> MassIndex<MassIndexHashMap> {
        let protease = Protease::get_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();
        let file_paths = vec![PROTEINS_FILE_PATH.clone()];
        let mut index = MassIndex::<MassIndexHashMap>::new(file_paths, MassIndexHashMap::default());
        index.build(&protease).await.unwrap();
        index
    }

    async fn test_create_multithreaded() -> MassIndex<MassIndexHashMap> {
        let protease = Protease::get_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();
        let file_paths = vec![PROTEINS_FILE_PATH.clone()];
        let mut index = MassIndex::<MassIndexHashMap>::new(file_paths, MassIndexHashMap::default());
        index.build_multithreaded(&protease, 4).await.unwrap();
        index
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create() {
        let st_index = test_create_singlethreaded().await;
        println!("st done");
        let mt_index = test_create_multithreaded().await;
        println!("mt done");
        assert_eq!(st_index, mt_index);
    }
}
