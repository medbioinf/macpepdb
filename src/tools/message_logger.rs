// std imports
use std::path::PathBuf;

// 3rd party imports
use anyhow::Result;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::Receiver;
use tracing::error;

/// Struct for logging messages to a file
///
pub struct MessageLogger;

impl MessageLogger {
    /// Starts logging and writes messages to the given file. Stops when all senders are dropped.
    ///
    /// # Arguments
    /// * `log_file_path` - Path to log file
    /// * `receiver` - Receiver for log messages
    /// * `flush_interval` - Number of messages after the file is flushed
    ///
    pub async fn start_logging<T>(
        log_file_path: PathBuf,
        mut receiver: Receiver<T>,
        flush_interval: usize,
    ) -> Result<usize>
    where
        T: ToLogMessage,
    {
        let mut message_counter = 0;
        if !log_file_path.exists() {
            File::create(&log_file_path).await?;
        }

        let mut log_file = BufWriter::new(File::create(log_file_path).await?);
        loop {
            match receiver.recv().await {
                Some(message) => {
                    let bytes = match message.to_message() {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            error!("Could not convert message to bytes: {:?}", e);
                            continue;
                        }
                    };
                    log_file.write_all(&bytes).await?;
                    log_file.write_all(b"\n").await?;
                    message_counter += 1;
                    if message_counter % flush_interval == 0 {
                        log_file.flush().await?;
                    }
                }
                None => break,
            }
        }

        log_file.flush().await?;
        Ok(message_counter)
    }
}

/// Trait for converting a type to a log message
pub trait ToLogMessage: Sync + Send {
    fn to_message(&self) -> Result<Vec<u8>>;
}

/// Implementations of ToLogMessage for standard String
///
impl ToLogMessage for String {
    fn to_message(&self) -> Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }
}

/// Implementations of ToLogMessage for standard anyhow::Error
///
impl ToLogMessage for anyhow::Error {
    fn to_message(&self) -> Result<Vec<u8>> {
        Ok(format!("{:?}", self).as_bytes().to_vec())
    }
}
