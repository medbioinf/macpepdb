// std imports
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

// 3rd party imports
use anyhow::{bail, Result};
use futures::future::join_all;

// local imports
use crate::{io::uniprot_text::reader::Reader, tools::progress_monitor::ProgressMonitor};

/// Multithreaded protein counter
///
pub struct ProteinCounter;

impl ProteinCounter {
    /// Count the number of proteins in a list of protein files
    ///
    /// # Arguments
    /// * `protein_files` - List of protein files
    /// * `num_threads` - Number of threads to use
    ///
    pub async fn count(protein_files: &Vec<PathBuf>, num_threads: usize) -> Result<usize> {
        let protein_file_queue = Arc::new(Mutex::new(protein_files.clone()));
        let processed_files: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
        let protein_counter: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

        let mut progress_monitor = ProgressMonitor::new(
            "",
            vec![processed_files.clone(), protein_counter.clone()],
            vec![Some(protein_files.len() as u64), None],
            vec!["files".to_string(), "proteins".to_string()],
            None,
        )?;

        let threads = (0..num_threads)
            .map(|_| {
                let thread_protein_file_queue = protein_file_queue.clone();
                let thread_processed_files = processed_files.clone();
                let thread_protein_counter = protein_counter.clone();
                tokio::spawn(async move {
                    loop {
                        let protein_file = {
                            let mut protein_file_queue = match thread_protein_file_queue.lock() {
                                Ok(protein_file_queue) => protein_file_queue,
                                Err(_) => bail!("Failed to lock protein file queue"),
                            };
                            protein_file_queue.pop()
                        };
                        if protein_file.is_none() {
                            break;
                        }
                        let protein_file = protein_file.unwrap();
                        let count = Reader::new(&protein_file, 4096)?.count_proteins()?;
                        thread_protein_counter.fetch_add(count, Ordering::Relaxed);
                        thread_processed_files.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();

        join_all(threads).await;
        progress_monitor.stop().await?;

        Ok(protein_counter.load(Ordering::Relaxed))
    }
}
