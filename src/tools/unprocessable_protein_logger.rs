// std imports
use std::path::PathBuf;

// 3rd party imports
use anyhow::Result;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::Receiver;

// local imports
use crate::entities::protein::Protein;

/// Writes all unprocessable proteins to a file
///
/// # Arguments
/// * `log_file_path` - Path to the file where the unprocessable proteins should be logged
/// * `receiver` - Receiver for the unprocessable proteins
pub async fn unprocessable_proteins_logger(
    log_file_path: PathBuf,
    mut receiver: Receiver<Protein>,
) -> Result<()> {
    if !log_file_path.exists() {
        File::create(&log_file_path).await?;
    }

    let mut log_file = BufWriter::new(File::create(log_file_path).await?);
    loop {
        match receiver.recv().await {
            Some(protein) => {
                log_file
                    .write_all(protein.to_uniprot_txt_entry()?.as_bytes())
                    .await?;
                log_file.write_all("\n".as_bytes()).await?;
            }
            None => break,
        }
    }

    log_file.flush().await?;
    Ok(())
}
