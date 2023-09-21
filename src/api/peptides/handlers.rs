use std::cmp;

use crate::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use crate::database::scylla::client::GenericClient;
use crate::database::scylla::configuration_table::ConfigurationTable;
use crate::database::scylla::peptide_table::{PeptideTable, SELECT_COLS};
use crate::database::scylla::SCYLLA_KEYSPACE_NAME;
use crate::database::table::Table;
use crate::entities::peptide::Peptide;
use crate::tools::peptide_partitioner::get_mass_partition;
use crate::{database::scylla::get_client, entities::domain::Domain};
use anyhow::Result;
use futures::StreamExt;
use scylla::frame::response::result::CqlValue;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct DomainsParams {
    lower: i64,
    upper: i64,
    take: usize,
}

pub async fn get_domains_handler(
    params: DomainsParams,
    database_urls: Vec<String>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut client = get_client(Some(&database_urls)).await.unwrap();
    let session = client.get_session();

    let config = ConfigurationTable::select(&client).await.unwrap();
    let partition_limits = config.get_partition_limits();

    let lower_partition_index = get_mass_partition(&partition_limits, params.lower).unwrap();
    let upper_partition_index = get_mass_partition(&partition_limits, params.upper).unwrap();

    let take_max: usize = cmp::min(1000, params.take);

    let mut peptides: Vec<Peptide> = vec![];

    for partition in lower_partition_index..upper_partition_index + 1 {
        if peptides.len() >= take_max {
            break;
        }
        let partition_cql_value = CqlValue::BigInt(partition as i64);

        let mut rows_stream = session
            .query_iter(
                format!(
                    "SELECT {} FROM {}.{} WHERE partition = ? AND mass >= ? AND mass <= ?",
                    SELECT_COLS,
                    SCYLLA_KEYSPACE_NAME,
                    PeptideTable::table_name()
                ),
                (&partition_cql_value, &params.lower, &params.upper),
            )
            .await
            .unwrap();

        while let Some(row_opt) = rows_stream.next().await {
            let row = row_opt.unwrap();
            let peptide = Peptide::from(row);
            peptides.push(peptide);

            if peptides.len() >= take_max {
                break;
            }
        }
    }

    Ok(warp::reply::json(&peptides))
}
