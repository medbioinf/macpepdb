// std imports
use std::{
    env,
    fs::create_dir,
    io::Cursor,
    path::{Path, PathBuf},
    time::Duration,
};

// 3rd party imports
use anyhow::Result;
use reqwest::header::USER_AGENT;

/// URL of the latest `taxdmp.zip` file
///
pub const TAXDMP_URL: &'static str = "https://ftp.ncbi.nih.gov/pub/taxonomy/taxdmp.zip";

/// Downloads the latest taxdmp.zip if not given by env var
/// `TAXDMP_ZIP_PATH` or already downloaded.
///
/// Copied from di_hardts_omicstools::biology::io::taxonomy_reader::tests
///
pub async fn get_taxdmp_zip() -> Result<PathBuf> {
    if let Some(taxdmp_zip_path) = env::var_os("TAXDMP_ZIP_PATH") {
        return Ok(Path::new(taxdmp_zip_path.to_str().unwrap()).to_path_buf());
    }
    let taxdmp_zip_path = env::temp_dir().join("taxdmp_for_unit_tests.zip");
    // Create temp dir
    if !taxdmp_zip_path.parent().unwrap().is_dir() {
        create_dir(taxdmp_zip_path.parent().unwrap()).unwrap();
    }
    // Avoid unnecessary downloads
    if taxdmp_zip_path.is_file() {
        return Ok(taxdmp_zip_path);
    }
    let client = reqwest::Client::new();
    // Create a request with custom user agent to apologies for download overhead
    let request = client
        .get(TAXDMP_URL)
        .header(
            USER_AGENT,
            format!(
                "Unit test from '{}' here, apologies for the download overhead.",
                env!("CARGO_PKG_REPOSITORY")
            ),
        )
        .timeout(Duration::from_secs(1200)); // 20 minutes
    let response = request.send().await?;
    let mut file = std::fs::File::create(&taxdmp_zip_path)?;
    let mut content = Cursor::new(response.bytes().await?);
    std::io::copy(&mut content, &mut file)?;
    Ok(taxdmp_zip_path)
}
