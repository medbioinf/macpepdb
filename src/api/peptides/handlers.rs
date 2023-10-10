use std::cmp;

use crate::chemistry::amino_acid::calc_sequence_mass;
use crate::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use crate::database::scylla::client::GenericClient;
use crate::database::scylla::configuration_table::ConfigurationTable;
use crate::database::scylla::peptide_table::{PeptideTable, SELECT_COLS};
use crate::database::scylla::SCYLLA_KEYSPACE_NAME;
use crate::database::selectable_table::SelectableTable;
use crate::database::table::Table;
use crate::entities::peptide::Peptide;
use crate::tools::peptide_partitioner::get_mass_partition;
use crate::{database::scylla::get_client, entities::domain::Domain};
use anyhow::Result;
use futures::StreamExt;
use scylla::frame::response::result::CqlValue;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct DomainsParams {
    sequence: String,
}

pub async fn get_domains_handler(
    params: DomainsParams,
    database_urls: Vec<String>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let client = get_client(Some(&database_urls)).await.unwrap();

    let config = ConfigurationTable::select(&client).await.unwrap();
    let partition_limits = config.get_partition_limits();

    let mass = calc_sequence_mass(&params.sequence).unwrap();
    let partition = get_mass_partition(partition_limits, mass).unwrap();

    let peptide_opt = PeptideTable::select(
        &client,
        "WHERE partition = ? AND mass = ? and sequence = ?",
        &[
            &CqlValue::BigInt(partition as i64),
            &CqlValue::BigInt(mass),
            &CqlValue::Text(params.sequence),
        ],
    )
    .await
    .unwrap();

    if peptide_opt.is_none() {
        return Ok(warp::reply::json(&(vec![] as Vec<Domain>)));
    }

    Ok(warp::reply::json(&(peptide_opt.unwrap()).get_domains()))
}
