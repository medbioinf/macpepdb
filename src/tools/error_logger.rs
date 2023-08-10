// std imports
use std::path::PathBuf;

// 3rd party imports
use anyhow::Result;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::Receiver;

/// Writes all unprocessable proteins to a file
///
/// # Arguments
/// * `log_file_path` - Path to log file
/// * `receiver` - Receiver for log messages
pub async fn error_logger(log_file_path: PathBuf, mut receiver: Receiver<String>) -> Result<()> {
    if !log_file_path.exists() {
        File::create(&log_file_path).await?;
    }

    let mut log_file = BufWriter::new(File::create(log_file_path).await?);
    loop {
        match receiver.recv().await {
            Some(message) => {
                log_file.write_all(message.as_bytes()).await?;
                log_file.write_all("\n".as_bytes()).await?;
            }
            None => break,
        }
    }

    log_file.flush().await?;
    Ok(())
}
