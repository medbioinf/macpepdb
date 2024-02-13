use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
// std imports
use std::time::Instant;

// 3rd party imports
use anyhow::Result;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
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
use crate::tools::peptide_partitioner::get_mass_partition;
use crate::tools::progress_monitor::ProgressMonitor;

pub async fn query_performance(
    database_url: &str,
    masses: Vec<i64>,
    lower_mass_tolerance: i64,
    upper_mass_tolerance: i64,
    max_variable_modifications: i16,
    ptms: Vec<PTM>,
) -> Result<()> {
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

    let client = Client::new(database_url).await?;
    let mut mass_stats: Vec<(i64, u64, u128)> = Vec::new();

    let config = ConfigurationTable::select(&client).await?;

    let partition_limits = config.get_partition_limits();

    let processed_ptm_conditions = Arc::new(AtomicUsize::new(0));
    let processed_masses = Arc::new(AtomicUsize::new(0));

    let mut progress_monitor = ProgressMonitor::new(
        "",
        vec![processed_masses.clone(), processed_ptm_conditions.clone()],
        vec![Some(masses.len() as u64), Some(num_ptm_conditions as u64)],
        vec!["masses".to_string(), "PTM conditions".to_string()],
        None,
    )?;

    let query_statement_str = format!(
        "SELECT {} FROM {}.{} WHERE partition = ? AND mass >= ? AND mass <= ?",
        SELECT_COLS,
        client.get_database(),
        PeptideTable::table_name()
    );

    let query_statement = client.prepare(query_statement_str).await?;

    // Iterate masses
    for mass in masses.iter() {
        // Create PTM conditions
        let ptm_conditions = get_ptm_conditions(*mass, max_variable_modifications, &ptms)?;

        // Iterate PTM conditions
        for ptm_condition in ptm_conditions.iter() {
            let query_start = Instant::now();
            // Calculate mass range
            let lower_mass_limit = CqlValue::BigInt(
                ptm_condition.get_mass()
                    - (ptm_condition.get_mass() / 1000000 * lower_mass_tolerance),
            );
            let upper_mass_limit = CqlValue::BigInt(
                ptm_condition.get_mass()
                    + (ptm_condition.get_mass() / 1000000 * upper_mass_tolerance),
            );

            let lower_partition_index =
                get_mass_partition(&partition_limits, lower_mass_limit.as_bigint().unwrap())
                    .unwrap();
            let upper_partition_index =
                get_mass_partition(&partition_limits, upper_mass_limit.as_bigint().unwrap())
                    .unwrap();

            let mut matching_peptides_ctr: u64 = 0;

            for partition in lower_partition_index..upper_partition_index + 1 {
                let partition_cql_value = CqlValue::BigInt(partition as i64);
                // let params: Vec<&CqlValue> =
                //     vec![&partition_cql_value, &lower_mass_limit, &upper_mass_limit];

                let mut rows_stream = client
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
                        matching_peptides_ctr += 1;
                    }
                }
            }
            mass_stats.push((
                *mass,
                matching_peptides_ctr,
                query_start.elapsed().as_millis(),
            ));
            processed_ptm_conditions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        processed_masses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    progress_monitor.stop().await?;
    // Sum up queries, matching peptides and time
    let num_matching_peptides = mass_stats
        .iter()
        .map(|(_, peps, _)| *peps as usize)
        .sum::<usize>();
    let num_millis = mass_stats
        .iter()
        .map(|(_, _, millis)| *millis as usize)
        .sum::<usize>();
    // calculate averages
    let average_queries = mass_stats.len() as f64 / masses.len() as f64;
    let average_matching_peptides = num_matching_peptides as f64 / mass_stats.len() as f64;
    let average_millis = num_millis as f64 / mass_stats.len() as f64;
    println!("Querying took {} ms", num_millis);
    println!("Queries\tMatching peptides\tTime");
    println!(
        "{}/{:.1}\t{}/{:.1}\t{}/{:.1}",
        mass_stats.len(),
        average_queries,
        num_matching_peptides,
        average_matching_peptides,
        num_millis,
        average_millis
    );
    Ok(())
}
