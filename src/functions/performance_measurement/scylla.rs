// std imports
use std::time::Instant;

// 3rd party imports
use anyhow::Result;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use futures::{pin_mut, StreamExt};
use indicatif::ProgressStyle;
use scylla::frame::response::result::CqlValue;
use tracing::{debug, info_span, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::database::scylla::SCYLLA_KEYSPACE_NAME;
// internal imports
use crate::database::scylla::client::{Client, GenericClient};
use crate::database::scylla::database_build::DatabaseBuild;
use crate::database::scylla::peptide_table::{PeptideTable, SELECT_COLS};
use crate::database::selectable_table::SelectableTable;
use crate::database::table::Table;
use crate::entities::peptide::Peptide;
use crate::functions::post_translational_modification::get_ptm_conditions;
use crate::tools::mass_to_partition::mass_to_partition_index;

pub async fn query_performance(
    hostnames: &Vec<&str>,
    masses: Vec<i64>,
    lower_mass_tolerance: i64,
    upper_mass_tolerance: i64,
    max_variable_modifications: i16,
    ptms: Vec<PTM>,
) -> Result<()> {
    let client = Client::new(hostnames).await?;
    let session = (&client).get_session();
    let mut mass_stats: Vec<(i64, usize, u64, u128)> = Vec::new();

    let config = DatabaseBuild::get_or_set_configuration(
        &mut Client::new(hostnames).await?,
        &vec![],
        0,
        0.0,
        0.0,
        None,
    )
    .await
    .unwrap();

    let partition_limits = config.get_partition_limits();

    let header_span = info_span!("Querying");
    header_span.pb_set_style(&ProgressStyle::default_bar());
    header_span.pb_set_length(masses.len() as u64);
    let header_span_enter = header_span.enter();

    // Iterate masses
    for mass in masses.iter() {
        // Create PTM conditions
        let ptm_conditions = get_ptm_conditions(*mass, max_variable_modifications, &ptms)?;
        let query_start = Instant::now();
        let mut matching_peptides_ctr: u64 = 0;

        // Iterate PTM conditions
        for ptm_condition in ptm_conditions.iter() {
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
                mass_to_partition_index(&partition_limits, lower_mass_limit.as_bigint().unwrap())
                    .unwrap();
            let upper_partition_index =
                mass_to_partition_index(&partition_limits, upper_mass_limit.as_bigint().unwrap())
                    .unwrap();

            for partition in lower_partition_index..upper_partition_index + 1 {
                let partition_cql_value = CqlValue::BigInt(partition);
                // let params: Vec<&CqlValue> =
                //     vec![&partition_cql_value, &lower_mass_limit, &upper_mass_limit];

                let query_statement = format!(
                    "SELECT {} FROM {}.{} WHERE partition = ? AND mass >= ? AND mass <= ?",
                    SELECT_COLS,
                    SCYLLA_KEYSPACE_NAME,
                    PeptideTable::table_name()
                );

                let mut rows_stream = session
                    .query_iter(
                        query_statement,
                        (partition_cql_value, &lower_mass_limit, &upper_mass_limit),
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
        }
        mass_stats.push((
            *mass,
            ptm_conditions.len(),
            matching_peptides_ctr,
            query_start.elapsed().as_millis(),
        ));
        Span::current().pb_inc(1);
    }
    std::mem::drop(header_span_enter);
    std::mem::drop(header_span);
    // Sum up queries, matching peptides and time
    let num_queries = mass_stats
        .iter()
        .map(|(_, queries, _, _)| *queries as usize)
        .sum::<usize>();
    let num_matching_peptides = mass_stats
        .iter()
        .map(|(_, _, peps, _)| *peps as usize)
        .sum::<usize>();
    let num_millis = mass_stats
        .iter()
        .map(|(_, _, millis, _)| *millis as usize)
        .sum::<usize>();
    // calculate averages
    let average_queries = num_queries / mass_stats.len();
    let average_matching_peptides = num_matching_peptides / mass_stats.len();
    let average_millis = num_millis / mass_stats.len();
    println!("Querying took {} ms", num_millis);
    println!("Queries\tMatching peptides\tTime");
    println!(
        "{}/{}\t{}/{}\t{}/{}",
        num_queries,
        average_queries,
        num_matching_peptides,
        average_matching_peptides,
        num_millis,
        average_millis
    );
    Ok(())
}
