// std imports
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use scylla::frame::response::result::CqlValue;
use serde_json::Value as JsonValue;

// internal imports
use crate::biology::digestion_enzyme::functions::get_enzyme_by_name;
use crate::database::scylla::client::Client;
use crate::database::scylla::protein_table::ProteinTable;
use crate::database::selectable_table::SelectableTable;
use crate::entities::configuration::Configuration;
use crate::web::web_error::WebError;

pub async fn get_protein(
    State((db_client, configuration)): State<(Arc<Client>, Arc<Configuration>)>,
    Path(accession): Path<String>,
) -> Result<Json<JsonValue>, WebError> {
    let accession = accession.to_uppercase();

    let protein_opt = ProteinTable::select(
        db_client.as_ref(),
        "WHERE accession = ?",
        &[&CqlValue::Text(accession)],
    )
    .await?;

    if let Some(protein) = protein_opt {
        // Enzymes are not saved in the database, so we have to create them
        // therefore we need the enzyme from the configuration
        let enzyme = get_enzyme_by_name(
            configuration.get_enzyme_name(),
            configuration.get_max_number_of_missed_cleavages(),
            configuration.get_min_peptide_length(),
            configuration.get_max_peptide_length(),
        )?;
        // Get the protein with the peptides as JSON value
        return Ok(Json(protein.to_json_with_peptides(enzyme.as_ref())?));
    } else {
        return Err(WebError::new(
            StatusCode::NOT_FOUND,
            "Protein not found".to_string(),
        ));
    }
}
