// std imports
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

// 3rd party imports
use anyhow::{bail, Result};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use futures::{pin_mut, Stream, StreamExt};
use tracing::info;

use crate::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use crate::database::generic_client::GenericClient;
use crate::database::scylla::configuration_table::ConfigurationTable;
// internal imports
use crate::database::scylla::client::Client;
use crate::database::scylla::peptide_filter::{
    FalliblePeptideStream, Filter, MultiTaskFilter, MultiThreadMultiClientFilter,
    MultiThreadSingleClientFilter, QueuedMultiThreadMultiClientFilter,
    QueuedMultiThreadSingleClientFilter,
};
use crate::functions::post_translational_modification::get_ptm_conditions;
use crate::tools::metrics_logger::MetricsLogger;
use crate::tools::progress_monitor::ProgressMonitor;

const FILTER_LABELS: [&str; 5] = [
    "multi_task_filter",
    "multi_thread_multi_client_filter",
    "multi_thread_single_client_filter",
    "queued_multi_thread_multi_client_filter",
    "queued_multi_thread_single_client_filter",
];

async fn get_peptide_stream<'a>(
    filter_label: &str,
    client: Arc<Client>,
    partition_limits: Arc<Vec<i64>>,
    mass: i64,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: i16,
    distinct: bool,
    taxonomy_ids: Option<Vec<i64>>,
    proteome_ids: Option<Vec<String>>,
    is_reviewed: Option<bool>,
    ptms: Vec<PTM>,
    num_threads: Option<usize>,
) -> Result<FalliblePeptideStream> {
    match filter_label {
        "multi_task_filter" => {
            MultiTaskFilter::filter(
                client,
                partition_limits,
                mass,
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                max_variable_modifications,
                distinct,
                taxonomy_ids,
                proteome_ids,
                is_reviewed,
                ptms,
                num_threads,
            )
            .await
        }
        "multi_thread_multi_client_filter" => {
            MultiThreadMultiClientFilter::filter(
                client,
                partition_limits,
                mass,
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                max_variable_modifications,
                distinct,
                taxonomy_ids,
                proteome_ids,
                is_reviewed,
                ptms,
                num_threads,
            )
            .await
        }
        "multi_thread_single_client_filter" => {
            MultiThreadSingleClientFilter::filter(
                client,
                partition_limits,
                mass,
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                max_variable_modifications,
                distinct,
                taxonomy_ids,
                proteome_ids,
                is_reviewed,
                ptms,
                num_threads,
            )
            .await
        }
        "queued_multi_thread_multi_client_filter" => {
            QueuedMultiThreadMultiClientFilter::filter(
                client,
                partition_limits,
                mass,
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                max_variable_modifications,
                distinct,
                taxonomy_ids,
                proteome_ids,
                is_reviewed,
                ptms,
                num_threads,
            )
            .await
        }
        "queued_multi_thread_single_client_filter" => {
            QueuedMultiThreadSingleClientFilter::filter(
                client,
                partition_limits,
                mass,
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                max_variable_modifications,
                distinct,
                taxonomy_ids,
                proteome_ids,
                is_reviewed,
                ptms,
                num_threads,
            )
            .await
        }
        _ => bail!("Unknown filter label: {}", filter_label),
    }
}

pub async fn query_performance(
    database_url: &str,
    masses: Vec<i64>,
    lower_mass_tolerance: i64,
    upper_mass_tolerance: i64,
    max_variable_modifications: i16,
    metrics_log_folder: &Path,
    metrics_log_interval: u64,
    ptms: Vec<PTM>,
    num_threads: Option<usize>,
) -> Result<()> {
    for filter_label in FILTER_LABELS {
        info!(
            "Running performance measurement for filter: {}",
            filter_label
        );
        let metrics_log_file = metrics_log_folder.join(format!("{}.tsv", filter_label));
        // Count number of PTM conditions
        let processed_masses = Arc::new(AtomicUsize::new(0));
        let mut progress_monitor = ProgressMonitor::new(
            "",
            vec![processed_masses.clone()],
            vec![Some(masses.len() as u64)],
            vec!["masses".to_string()],
            None,
        )?;

        info!("Calculating number of PTM conditions (depending on given masses and PMTs) ...");
        let num_ptm_conditions: usize = masses
            .iter()
            .map(|mass| {
                processed_masses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(get_ptm_conditions(*mass, max_variable_modifications, &ptms)?.len())
            })
            .collect::<Result<Vec<usize>>>()?
            .into_iter()
            .sum();
        progress_monitor.stop().await?;
        info!("... {} PTM conditions", num_ptm_conditions);

        // Open client
        let client = Client::new(database_url).await?;

        // Get configuration and partition limits
        let config = ConfigurationTable::select(&client).await?;
        let partition_limits = Arc::new(config.get_partition_limits().clone());

        // Create atomic counters
        let processed_masses = Arc::new(AtomicUsize::new(0));
        let matching_peptides = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(AtomicUsize::new(0));

        let mut progress_monitor = ProgressMonitor::new(
            "",
            vec![
                processed_masses.clone(),
                matching_peptides.clone(),
                errors.clone(),
            ],
            vec![Some(masses.len() as u64), None, None],
            vec![
                "masses".to_string(),
                "matching peptides".to_string(),
                "errors".to_string(),
            ],
            None,
        )?;

        // Metrics logger
        let mut metrics_logger = MetricsLogger::new(
            vec![
                processed_masses.clone(),
                matching_peptides.clone(),
                errors.clone(),
            ],
            vec![
                "masses".to_string(),
                "matching peptides".to_string(),
                "errors".to_string(),
            ],
            metrics_log_file.to_path_buf(),
            metrics_log_interval,
        )?;

        let client = Arc::new(client);
        // Iterate masses
        for mass in masses.iter() {
            let mut filtered_stream = get_peptide_stream(
                filter_label,
                client.clone(),
                partition_limits.clone(),
                *mass,
                lower_mass_tolerance,
                upper_mass_tolerance,
                max_variable_modifications,
                false,
                None,
                None,
                None,
                ptms.clone(),
                num_threads,
            )
            .await?;
            while let Some(peptide) = filtered_stream.next().await {
                match peptide {
                    Ok(_) => matching_peptides.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    Err(_) => errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                };
            }
            processed_masses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        progress_monitor.stop().await?;
        metrics_logger.stop().await?;
    }
    Ok(())
}
