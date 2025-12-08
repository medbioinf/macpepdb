use std::any::Any;

use thiserror::Error;

/// Errors which might occur in mass counting
#[derive(Error, Debug)]
pub enum PeptideMassCounterError {
    #[error("Insufficient memory for bloom filter (required: {0} bytes, available: {1} bytes)")]
    InsufficientMemory(usize, usize),
    #[error("protein queue is poisoned")]
    ProteinQueuePoisoned,
    #[error("Protein reader error: {0}")]
    ProteinReaderError(anyhow::Error),
    #[error("Protein counting error: {0}")]
    ProteinCountError(anyhow::Error),
    #[error("Could not read next protein: {0}")]
    NextProteinReadeError(anyhow::Error),
    #[error("Protease cleavage error: {0}")]
    ProteaseCleavageError(anyhow::Error),
    #[error("Protease creation error: {0}")]
    ProteaseCreationError(anyhow::Error),
    #[error("Peptide conversion error: {0}")]
    PeptideConversionError(anyhow::Error),
    #[error("Bloom filter error: {0}")]
    BloomFilterError(#[from] dihardts_cstools::bloom_filter::error::BloomFilterError),
    #[error("Protein count thread panicked with: {0:?}")]
    ProteinCountThreadPanicError(Box<dyn Any + Send + 'static>),
    #[error("Progress monitor error: {0}")]
    ProgressMonitorError(anyhow::Error),
    #[error("Queue monitor error: {0}")]
    QueueMonitorError(anyhow::Error),
}
