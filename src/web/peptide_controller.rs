use std::sync::Arc;

// 3rd party imports
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use scylla::frame::response::result::CqlValue;

// internal imports
use crate::chemistry::amino_acid::calc_sequence_mass;
use crate::database::scylla::client::Client;
use crate::database::scylla::peptide_table::PeptideTable;
use crate::database::selectable_table::SelectableTable;
use crate::entities::configuration::Configuration;
use crate::entities::peptide::Peptide;
use crate::tools::peptide_partitioner::get_mass_partition;
use crate::web::web_error::WebError;

pub async fn get_peptide(
    State((db_client, configuration)): State<(Arc<Client>, Arc<Configuration>)>,
    Path(sequence): Path<String>,
) -> Result<Json<Peptide>, WebError> {
    let sequence = sequence.to_uppercase();
    let mass = calc_sequence_mass(sequence.as_str())?;
    let partition = get_mass_partition(configuration.get_partition_limits(), mass)?;

    let peptide_opt = PeptideTable::select(
        db_client.as_ref(),
        "WHERE partition = ? AND mass = ? and sequence = ?",
        &[
            &CqlValue::BigInt(partition as i64),
            &CqlValue::BigInt(mass),
            &CqlValue::Text(sequence),
        ],
    )
    .await?;

    if let Some(peptide) = peptide_opt {
        return Ok(Json(peptide));
    } else {
        return Err(WebError::new(
            StatusCode::NOT_FOUND,
            "Peptide not found".to_string(),
        ));
    }
}
