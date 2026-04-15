use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    hash::{Hash, Hasher},
    io::BufReader,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use fallible_iterator::FallibleIterator;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uniprot_reader::indexer::Offset;

use crate::{protease::Protease, sequence::IsSequence};

#[derive(Debug, Error)]
pub enum Error {
    #[error("UnipotReader: {0}")]
    UnprotReader(#[from] uniprot_reader::reader::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Protease error: {0}")]
    Protease(String),
    #[error("Index thread stopped before all masses were indexed")]
    EarlyIndexThreadStop,
    #[error("Index thread stopped unexpectedly: {0}")]
    IndexThread(String),
    #[error("Protein reader thread error: {0}")]
    ProteinReaderThread(String),
    #[error(
        "Unexpected multithreaded indexing outcome\n\tIndexing thread exited with: {0}\n\tReader threads stopped with {1}"
    )]
    MultithreadedIndexingOutcome(String, String),
}

#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
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

#[derive(Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub struct MassIndex {
    files: Vec<PathBuf>,
    map: HashMap<i64, HashSet<Entry>>,
}

impl MassIndex {
    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }

    pub fn files_mut(&mut self) -> &mut Vec<PathBuf> {
        &mut self.files
    }

    pub fn map(&self) -> &HashMap<i64, HashSet<Entry>> {
        &self.map
    }

    pub fn map_mut(&mut self) -> &mut HashMap<i64, HashSet<Entry>> {
        &mut self.map
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn entry_len(&self) -> usize {
        self.map.values().map(|entries| entries.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn masses(&self) -> impl Iterator<Item = &i64> {
        self.map.keys()
    }

    pub fn entries(&self) -> impl Iterator<Item = (&i64, &HashSet<Entry>)> {
        self.map.iter()
    }

    pub fn create<S: IsSequence>(
        file_paths: &[PathBuf],
        protease: &Protease<S>,
    ) -> Result<Self, Error> {
        let file_paths_vec = file_paths.to_vec();

        let mut index = Self::default();
        index.files_mut().extend(file_paths_vec);

        for (path_idx, path) in file_paths.iter().enumerate() {
            let mut buf_reader = BufReader::new(File::open(path)?);

            let reader = uniprot_reader::reader::Reader::new(&mut buf_reader);
            for item in reader {
                let item = item?;
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

                    index
                        .map_mut()
                        .entry(mass)
                        .or_insert_with(HashSet::new)
                        .insert(entry);
                }
            }
        }

        Ok(index)
    }

    pub fn create_multithreaded<S: IsSequence + 'static>(
        file_paths: &[PathBuf],
        protease: &Protease<S>,
        num_threads: usize,
    ) -> Result<Self, Error> {
        let num_threads = std::cmp::min(num_threads, file_paths.len());

        let file_paths_vec = file_paths.to_vec();

        let file_path_queue = Arc::new(Mutex::new(VecDeque::from_iter(
            file_paths_vec.iter().cloned().enumerate(),
        )));
        let protease = Arc::new(protease.clone());

        let mut index = Self::default();
        index.files_mut().extend(file_paths_vec);

        let (sender, receiver) = std::sync::mpsc::channel::<(i64, Entry)>();

        let indexing_thread = std::thread::spawn(move || {
            while let Ok((mass, entry)) = receiver.recv() {
                index
                    .map_mut()
                    .entry(mass) // Placeholder for mass, replace with actual mass calculation
                    .or_insert_with(HashSet::new)
                    .insert(entry);
            }
            index
        });

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

        let protein_reader_results = protein_reader_threads
            .into_iter()
            .map(|thread| {
                thread
                    .join()
                    .map_err(|err| Error::ProteinReaderThread(format!("{err:?}")))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let index_thread_result = indexing_thread
            .join()
            .map_err(|err| Error::IndexThread(format!("{err:?}")));

        // If indexing thread is ok and all protein readers, indexing was normal and successfull.
        // If indexing task returned an error and some of the protein readers do the same it is normal unsuccessfull.
        // If the exit with different results something is unusual
        if index_thread_result.is_ok() && protein_reader_results.iter().all(|res| res.is_ok())
            || index_thread_result.is_err() && protein_reader_results.iter().any(|res| res.is_err())
        {
            index_thread_result
        } else {
            Err(Error::MultithreadedIndexingOutcome(
                index_thread_result
                    .err()
                    .map_or_else(|| "Ok".to_string(), |err| format!("{err:?}")),
                protein_reader_results
                    .iter()
                    .map(|res| {
                        res.as_ref()
                            .err()
                            .map_or_else(|| "Ok".to_string(), |err| format!("{err:?}"))
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sequence::BitSequence;

    use super::*;

    static PROTEINS_FILE_PATH: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
        PathBuf::from(std::env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_data/some_human_proteins.uniprot.txt")
    });

    pub fn test_create_singlethreaded() -> MassIndex {
        let protease =
            Protease::<BitSequence>::get_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();
        let file_paths = vec![PROTEINS_FILE_PATH.clone()];
        MassIndex::create(&file_paths, &protease).unwrap()
    }

    pub fn test_create_multithreaded() -> MassIndex {
        let protease =
            Protease::<BitSequence>::get_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();
        let file_paths = vec![PROTEINS_FILE_PATH.clone()];
        MassIndex::create_multithreaded(&file_paths, &protease, 4).unwrap()
    }

    #[test]
    fn test_create() {
        let st_index = test_create_singlethreaded();
        println!("st done");
        let mt_index = test_create_multithreaded();
        println!("mt done");
        assert_eq!(st_index, mt_index);
    }
}
