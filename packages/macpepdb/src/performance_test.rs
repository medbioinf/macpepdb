use std::{
    fs::File,
    io::{BufRead, BufReader},
    num::NonZeroUsize,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};

use crossbeam::queue::ArrayQueue;
use futures::{StreamExt, future::join_all};
use metrics::counter;
use thiserror::Error;
use tracing::{info, warn};

use crate::{
    mass::{to_float as mass_to_float, to_int as mass_to_int},
    peptidoform_search_client::PeptidoformSearchClient,
    post_translational_modification::{PTMCollection, PostTranslationalModification},
};

pub const PROGRESS_METRIC: &str = "performance_test_progress";
pub const ERROR_METRIC: &str = "performance_test_errors";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid mass format in masses file: {0}")]
    InvalidMass(String),
    #[error("CSV error while reading PTM file: {0}")]
    PtmRead(Box<csv::Error>),
    #[error("IO error while reading masses file: {0}")]
    MassesRead(Box<std::io::Error>),
    #[error("IO error while reading next mass from masses file: {0}")]
    NextMass(Box<std::io::Error>),
    #[error("Peptidoform search client error in performance test: {0}")]
    PeptidoformSearchClient(Box<crate::peptidoform_search_client::Error>),
    #[error("Error while reading PTM collection: {0}")]
    Ptm(Box<crate::post_translational_modification::Error>),

    #[cfg(feature = "admin-api")]
    #[error("Error while sending request to admin API: {0}")]
    AdminApiRequest(Box<reqwest::Error>),
}

into_thiserror_boxed!(
    crate::peptidoform_search_client::Error,
    Error,
    PeptidoformSearchClient
);
into_thiserror_boxed!(crate::post_translational_modification::Error, Error, Ptm);

pub struct PerformanceTest {
    masses: Vec<i64>,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: usize,
    concurrent_searches: NonZeroUsize,
    ptms: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
    peptidoform_search_client: PeptidoformSearchClient,
}

impl PerformanceTest {
    /// Creates a new `PerformanceTest` instance.
    /// If feature `admin-api` is enabled and a web base URL is provided, it will attempt to rebuild the database client via the admin API.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        masses_file_path: PathBuf,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: usize,
        concurrent_searches: NonZeroUsize,
        database_url: &str,
        ptm_file_path: Option<String>,
        web_base_url: Option<String>,
    ) -> Result<Self, Error> {
        tracing::info!("Initializing peptide search client");
        let peptidoform_search_client = web_base_url
            .as_ref()
            .map_or(PeptidoformSearchClient::try_from_url(database_url), |url| {
                PeptidoformSearchClient::try_from_url(url)
            })
            .await?;

        tracing::info!("Read PTMs");
        let ptms = ptm_file_path
            .map(|path| {
                csv::ReaderBuilder::new()
                    .delimiter(b'\t')
                    .has_headers(true)
                    .from_path(path)
                    .unwrap()
                    .deserialize()
                    .map(|result| result.map(Arc::new))
                    .collect::<Result<Vec<Arc<PostTranslationalModification>>, csv::Error>>()
                    .map_err(|e| Error::PtmRead(Box::new(e)))
            })
            .transpose()?
            .unwrap_or_default();

        let ptms = Arc::new(PTMCollection::new(ptms)?);

        tracing::info!("Read masses");
        let buf_reader = BufReader::new(
            File::open(&masses_file_path).map_err(|e| Error::MassesRead(Box::new(e)))?,
        );

        let masses = buf_reader
            .lines()
            .map(|line| {
                let line = line.map_err(|e| Error::NextMass(Box::new(e)))?;
                let line = line.trim();
                if !line.contains(" ") {
                    line.parse::<f64>()
                        .map_err(|_| Error::InvalidMass(line.to_string()))
                } else {
                    let (mz, charge) = line
                        .split_once(' ')
                        .ok_or_else(|| Error::InvalidMass(line.to_string()))?;
                    let mz = mz
                        .parse::<f64>()
                        .map_err(|_| Error::InvalidMass(line.to_string()))?;
                    let charge = charge
                        .parse::<u8>()
                        .map_err(|_| Error::InvalidMass(line.to_string()))?;
                    Ok(crate::mass::mass_to_charge_to_dalton(mz, charge))
                }
            })
            .map(|result| result.map(mass_to_int))
            .collect::<Result<Vec<_>, Error>>()?;

        #[cfg(feature = "admin-api")]
        if let Some(web_base_url) = web_base_url {
            match peptidoform_search_client {
                PeptidoformSearchClient::WebApi(ref client, _) => {
                    let admin_url = format!(
                        "{}{}{}",
                        web_base_url.trim_end_matches('/'),
                        crate::web::admin_controller::CONTROLLER_PATH,
                        crate::web::admin_controller::REBUILD_CLIENT_PATH
                    );
                    let request_body = serde_json::json!({
                        "database_url": database_url,
                        "concurrent_searches": concurrent_searches.get(),
                    });

                    let _ = client
                        .post(&admin_url)
                        .json(&request_body)
                        .send()
                        .await
                        .map_err(|e| Error::AdminApiRequest(Box::new(e)))?
                        .error_for_status()
                        .map_err(|e| Error::AdminApiRequest(Box::new(e)))?;

                    info!("Successfully rebuilt DB client via admin endpoint");
                }
                PeptidoformSearchClient::Database(_, _) => {
                    warn!("Client is database but web url is set. Should not occure");
                }
            }
        }

        Ok(Self {
            masses,
            lower_mass_tolerance_ppm,
            upper_mass_tolerance_ppm,
            max_variable_modifications,
            concurrent_searches,
            ptms,
            peptidoform_search_client,
        })
    }

    pub fn concurrent_searches_mut(&mut self) -> &mut NonZeroUsize {
        &mut self.concurrent_searches
    }

    pub fn masses(&self) -> &[i64] {
        &self.masses
    }

    pub async fn run(&self, threads: NonZeroUsize) -> Result<(), Error> {
        tracing::info!("Start test run, init queue and metrics");
        let queue = Arc::new(ArrayQueue::new(self.masses.len()));
        self.masses.iter().for_each(|&mass| {
            queue.push(mass).unwrap();
        });
        let search_metrics = Arc::new(Mutex::new(Vec::<(usize, usize)>::with_capacity(
            self.masses.len(),
        )));
        let progress_metric = Arc::new(counter!(PROGRESS_METRIC));
        progress_metric.absolute(0);
        let error_metric = Arc::new(counter!(ERROR_METRIC));
        error_metric.absolute(0);

        tracing::info!("Start test threads");
        let tasks = (0..threads.get()).map(|_| {
            let queue = queue.clone();
            let peptidoform_search_client = &self.peptidoform_search_client;
            let ptms = self.ptms.clone();
            let search_metrics = search_metrics.clone();
            let progress_metric = progress_metric.clone();
            let error_metric = error_metric.clone();
            async move {
                while let Some(mass) = queue.pop() {
                    let start_time = Instant::now();
                    match peptidoform_search_client
                        .search(
                            mass,
                            self.lower_mass_tolerance_ppm,
                            self.upper_mass_tolerance_ppm,
                            self.max_variable_modifications,
                            None,
                            None,
                            ptms.clone(),
                            true,
                            self.concurrent_searches,
                        )
                        .await
                    {
                        Ok(mut peptidoform_stream) => {
                            let mut num_peptidoforms = 0;
                            while let Some(peptidoform_result) = peptidoform_stream.next().await {
                                match peptidoform_result {
                                    Ok(_peptidoform) => {
                                        num_peptidoforms += 1;
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Error while reading peptidoform for mass {}: {}",
                                            mass_to_float(mass),
                                            e
                                        );
                                        error_metric.increment(1);
                                    }
                                }
                            }
                            let elapsed_time = start_time.elapsed();
                            progress_metric.increment(1);
                            info!(
                                "Mass: {}, Peptidoforms: {}, Time: {:.2?}",
                                mass_to_float(mass),
                                num_peptidoforms,
                                elapsed_time
                            );
                            search_metrics
                                .lock()
                                .unwrap()
                                .push((num_peptidoforms, elapsed_time.as_secs() as usize));
                        }
                        Err(e) => {
                            warn!("Error searching for mass {}: {}", mass_to_float(mass), e);
                            error_metric.increment(1);
                        }
                    }
                }
            }
        });

        join_all(tasks).await;

        let (total_peptidoforms, total_time) = search_metrics.lock().unwrap().iter().fold(
            (0, 0),
            |(total_peptidoforms, total_time), (num_peptidoforms, time)| {
                (total_peptidoforms + num_peptidoforms, total_time + time)
            },
        );

        info!(
            "Total peptidoforms: {}, Total time: {} seconds",
            total_peptidoforms, total_time
        );

        Ok(())
    }
}
