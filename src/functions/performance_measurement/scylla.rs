use std::path::Path;
use std::thread::sleep;
use tokio::spawn;
// std imports
use std::time::{Duration, Instant};

// 3rd party imports
use anyhow::Result;
use futures::{pin_mut, StreamExt};
use indicatif::ProgressStyle;
use scylla::frame::response::result::CqlValue;
use scylla::transport::iterator::RowIterator;
use tokio::task::JoinHandle;
use tracing::{debug, error, info_span, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use crate::database::scylla::configuration_table::ConfigurationTable;
use crate::database::scylla::{get_client, SCYLLA_KEYSPACE_NAME};
// internal imports
use crate::database::scylla::client::{Client, GenericClient};
use crate::database::scylla::peptide_table::{PeptideTable, SELECT_COLS};
use crate::database::selectable_table::SelectableTable;
use crate::database::table::Table;
use crate::entities::peptide::Peptide;
use crate::functions::post_translational_modification::get_ptm_conditions;
use crate::io::post_translational_modification_csv::reader::Reader as PtmReader;
use crate::tools::mass_to_partition::mass_to_partition_index;
use crate::tools::peptide_partitioner::get_mass_partition;

pub async fn query_performance(
    hostnames: &Vec<&str>,
    masses: Vec<i64>,
    lower_mass_tolerance: i64,
    upper_mass_tolerance: i64,
    max_variable_modifications: i16,
    ptm_file: String,
) -> Result<()> {
    let database_hosts: Vec<String> = hostnames.iter().map(|x| x.to_string()).collect();
    let mass_chunks: Vec<Vec<i64>> = masses.chunks(16).map(|x| x.to_owned()).collect();
    let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();
    let client = get_client(Some(&database_hosts)).await.unwrap();
    let config = ConfigurationTable::select(&client).await.unwrap();
    let partition_limits = config.get_partition_limits();

    for thread_id in 0..16 {
        let hostnames_clone = database_hosts.clone();
        let ptm_file_clone = ptm_file.clone();
        let masses_clone = mass_chunks[thread_id].clone();
        let partition_limits_clone = partition_limits.clone();

        handles.push(spawn(async move {
            query_performance_thread(
                hostnames_clone,
                masses_clone,
                lower_mass_tolerance,
                upper_mass_tolerance,
                max_variable_modifications,
                ptm_file_clone,
                partition_limits_clone,
            )
            .await?;
            Ok(())
        }));
    }

    Ok(())
}

pub async fn query_performance_thread(
    hostnames: Vec<String>,
    masses: Vec<i64>,
    lower_mass_tolerance: i64,
    upper_mass_tolerance: i64,
    max_variable_modifications: i16,
    ptm_file: String,
    partition_limits: Vec<i64>,
) -> Result<()> {
    let client = get_client(Some(&hostnames)).await.unwrap();
    let session = (&client).get_session();
    let mut mass_stats: Vec<(i64, u64, u128)> = Vec::new();

    let ptms = PtmReader::read(Path::new(&ptm_file)).unwrap();

    let header_span = info_span!("Querying");
    header_span.pb_set_style(&ProgressStyle::default_bar());
    header_span.pb_set_length(masses.len() as u64);
    let header_span_enter = header_span.enter();

    let query_statement_str = format!(
        "SELECT {} FROM {}.{} WHERE partition = ? AND mass >= ? AND mass <= ?",
        SELECT_COLS,
        SCYLLA_KEYSPACE_NAME,
        PeptideTable::table_name()
    );

    let query_statement = session.prepare(query_statement_str).await.unwrap();

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

                let mut rows_stream: RowIterator;

                loop {
                    let rows_stream_res = session
                        .execute_iter(
                            query_statement.to_owned(),
                            (&partition_cql_value, &lower_mass_limit, &upper_mass_limit),
                        )
                        .await;
                    if rows_stream_res.is_err() {
                        error!("Row stream err");
                        sleep(Duration::from_millis(100));
                        continue;
                    }
                    rows_stream = rows_stream_res.unwrap();
                    break;
                }

                while let Some(row_opt) = rows_stream.next().await {
                    if row_opt.is_err() {
                        error!("Row stream err");
                        sleep(Duration::from_millis(100));
                        continue;
                    }
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
        }
        Span::current().pb_inc(1);
    }
    std::mem::drop(header_span_enter);
    std::mem::drop(header_span);
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
