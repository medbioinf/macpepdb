// std imports
use std::time::Instant;

// 3rd party imports
use anyhow::Result;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use futures::{pin_mut, StreamExt};
use indicatif::ProgressStyle;
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;
use tracing::{info_span, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

// internal imports
use crate::database::citus::configuration_table::ConfigurationTable;
use crate::database::citus::peptide_table::PeptideTable;
use crate::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use crate::database::selectable_table::SelectableTable;
use crate::functions::post_translational_modification::get_ptm_conditions;
use crate::tools::peptide_partitioner::get_mass_partition;

pub async fn query_performance(
    database_url: &str,
    masses: Vec<i64>,
    lower_mass_tolerance: i64,
    upper_mass_tolerance: i64,
    max_variable_modifications: i16,
    ptms: Vec<PTM>,
) -> Result<()> {
    let header_span = info_span!("Querying");
    header_span.pb_set_style(&ProgressStyle::default_bar());
    header_span.pb_set_length(masses.len() as u64);
    let header_span_enter = header_span.enter();

    let (mut client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let configuration = ConfigurationTable::select(&client).await?;

    let mut mass_stats: Vec<(i64, u64, u128)> = Vec::new();

    // Iterate masses
    for mass in masses.iter() {
        // Create PTM conditions
        let ptm_conditions = get_ptm_conditions(*mass, max_variable_modifications, &ptms)?;
        // Iterate PTM conditions
        for ptm_condition in ptm_conditions.iter() {
            let query_start = Instant::now();
            // Calculate mass range
            let lower_mass_limit = ptm_condition.get_mass()
                - (ptm_condition.get_mass() / 1000000 * lower_mass_tolerance);
            let upper_mass_limit = ptm_condition.get_mass()
                + (ptm_condition.get_mass() / 1000000 * upper_mass_tolerance);
            let lower_partition =
                get_mass_partition(configuration.get_partition_limits(), lower_mass_limit)? as i64;
            let upper_partition =
                get_mass_partition(configuration.get_partition_limits(), upper_mass_limit)? as i64;
            let (query, params): (String, Vec<&(dyn ToSql + Sync)>) = if lower_partition
                == upper_partition
            {
                (
                    String::from("WHERE partition = {} AND mass BETWEEN {} AND {}"),
                    vec![&lower_partition, &lower_mass_limit, &upper_mass_limit],
                )
            } else {
                (
                    String::from("WHERE partition BETWEEN {} AND {} AND mass BETWEEN {} AND {}"),
                    vec![
                        &lower_partition,
                        &upper_partition,
                        &lower_mass_limit,
                        &upper_mass_limit,
                    ],
                )
            };
            // Count matching peptides
            let mut matching_peptides_ctr: u64 = 0;
            let peptide_iter = PeptideTable::stream(&mut client, &query, &params, 1000).await?;
            pin_mut!(peptide_iter);
            while let Some(peptide) = peptide_iter.next().await {
                if ptm_condition.check_peptide(&peptide?) {
                    matching_peptides_ctr += 1;
                }
            }
            mass_stats.push((
                *mass,
                matching_peptides_ctr,
                query_start.elapsed().as_millis(),
            ))
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
