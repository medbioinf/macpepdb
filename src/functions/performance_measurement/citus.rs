// std imports
use std::time::Instant;

// 3rd party imports
use anyhow::Result;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use futures::{pin_mut, StreamExt};
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;

// internal imports
use crate::database::citus::peptide_table::PeptideTable;
use crate::database::selectable_table::SelectableTable;
use crate::functions::post_translational_modification::get_ptm_conditions;

pub async fn query_performance(
    database_url: &str,
    masses: Vec<i64>,
    lower_mass_tolerance: i64,
    upper_mass_tolerance: i64,
    max_variable_modifications: i16,
    ptms: Vec<PTM>,
) -> Result<()> {
    //
    let (mut client, connection) = tokio_postgres::connect(database_url, NoTls).await?;
    let mut mass_stats: Vec<(i64, usize, u64, u128)> = Vec::new();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

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
            // Count matching peptides
            let mut matching_peptides_ctr: u64 = 0;
            let params: Vec<&(dyn ToSql + Sync)> = vec![&lower_mass_limit, &upper_mass_limit];
            let peptide_iter =
                PeptideTable::stream(&mut client, "WHERE mass BETWEEN $1 AND $2", &params, 1000)
                    .await?;
            pin_mut!(peptide_iter);
            while let Some(peptide) = peptide_iter.next().await {
                if ptm_condition.check_peptide(&peptide?) {
                    matching_peptides_ctr += 1;
                }
            }
            mass_stats.push((
                *mass,
                ptm_conditions.len(),
                matching_peptides_ctr,
                query_start.elapsed().as_millis(),
            ))
        }
    }
    // Sum up queries, matching peptides and timew
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
