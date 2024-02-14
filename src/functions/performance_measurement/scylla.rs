// std imports
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use futures::future::join_all;
use futures::StreamExt;
use scylla::frame::response::result::CqlValue;
use tracing::info;

use crate::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use crate::database::generic_client::GenericClient;
use crate::database::scylla::configuration_table::ConfigurationTable;
// internal imports
use crate::database::scylla::client::Client;
use crate::database::scylla::peptide_table::{PeptideTable, SELECT_COLS};
use crate::database::table::Table;
use crate::entities::peptide::Peptide;
use crate::functions::post_translational_modification::get_ptm_conditions;
use crate::tools::metrics_logger::MetricsLogger;
use crate::tools::peptide_partitioner::get_mass_partition;
use crate::tools::progress_monitor::ProgressMonitor;

pub async fn query_performance(
    database_url: &str,
    masses: Vec<i64>,
    lower_mass_tolerance: i64,
    upper_mass_tolerance: i64,
    max_variable_modifications: i16,
    metrics_log_file: &Path,
    metrics_log_interval: u64,
    ptms: Vec<PTM>,
) -> Result<()> {
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
    let processed_ptm_conditions = Arc::new(AtomicUsize::new(0));
    let processed_masses = Arc::new(AtomicUsize::new(0));
    let processed_peptides = Arc::new(AtomicUsize::new(0));
    let matching_peptides = Arc::new(AtomicUsize::new(0));

    let mut progress_monitor = ProgressMonitor::new(
        "",
        vec![
            processed_masses.clone(),
            processed_ptm_conditions.clone(),
            processed_peptides.clone(),
            matching_peptides.clone(),
        ],
        vec![
            Some(masses.len() as u64),
            Some(num_ptm_conditions as u64),
            None,
            None,
        ],
        vec![
            "masses".to_string(),
            "PTM conditions".to_string(),
            "processed peptides".to_string(),
            "matching peptides".to_string(),
        ],
        None,
    )?;

    // Metrics logger
    let mut metrics_logger = MetricsLogger::new(
        vec![
            processed_masses.clone(),
            processed_ptm_conditions.clone(),
            processed_peptides.clone(),
            matching_peptides.clone(),
        ],
        vec![
            "masses".to_string(),
            "PTM conditions".to_string(),
            "processed peptides".to_string(),
            "matching peptides".to_string(),
        ],
        metrics_log_file.to_path_buf(),
        metrics_log_interval,
    )?;

    let query_statement_str = format!(
        "SELECT {} FROM {}.{} WHERE partition = ? AND mass >= ? AND mass <= ?",
        SELECT_COLS,
        client.get_database(),
        PeptideTable::table_name()
    );

    let client = Arc::new(client);
    // Iterate masses
    for mass in masses.iter() {
        // Create PTM conditions
        let ptm_conditions = get_ptm_conditions(*mass, max_variable_modifications, &ptms)?;

        let tasks: Vec<tokio::task::JoinHandle<Result<()>>> = ptm_conditions
            .into_iter()
            .map(|ptm_condition| {
                let task_client = client.clone();
                let task_query_statement_str = query_statement_str.clone();
                let task_partition_limits = partition_limits.clone();
                let task_processed_peptides = processed_peptides.clone();
                let task_matching_peptides = matching_peptides.clone();
                let task_processed_ptm_conditions = processed_ptm_conditions.clone();
                tokio::task::spawn(async move {
                    let query_statement = task_client.prepare(task_query_statement_str).await?;
                    let lower_mass_limit = CqlValue::BigInt(
                        ptm_condition.get_mass()
                            - (ptm_condition.get_mass() / 1000000 * lower_mass_tolerance),
                    );
                    let upper_mass_limit = CqlValue::BigInt(
                        ptm_condition.get_mass()
                            + (ptm_condition.get_mass() / 1000000 * upper_mass_tolerance),
                    );

                    let lower_partition_index = get_mass_partition(
                        &task_partition_limits,
                        lower_mass_limit.as_bigint().unwrap(),
                    )
                    .unwrap();
                    let upper_partition_index = get_mass_partition(
                        &task_partition_limits,
                        upper_mass_limit.as_bigint().unwrap(),
                    )
                    .unwrap();

                    for partition in lower_partition_index..upper_partition_index + 1 {
                        let partition_cql_value = CqlValue::BigInt(partition as i64);
                        // let params: Vec<&CqlValue> =
                        //     vec![&partition_cql_value, &lower_mass_limit, &upper_mass_limit];

                        let mut rows_stream = task_client
                            .execute_iter(
                                query_statement.to_owned(),
                                (&partition_cql_value, &lower_mass_limit, &upper_mass_limit),
                            )
                            .await
                            .unwrap();

                        while let Some(row_opt) = rows_stream.next().await {
                            let row = row_opt.unwrap();
                            let peptide = Peptide::from(row);

                            if ptm_condition.check_peptide(&peptide) {
                                task_matching_peptides
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            task_processed_peptides
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    task_processed_ptm_conditions
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        join_all(tasks).await;
        processed_masses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    progress_monitor.stop().await?;
    metrics_logger.stop().await?;
    Ok(())
}
