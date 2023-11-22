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
use crate::database::scylla::protein_table::ProteinTable;
use crate::database::selectable_table::SelectableTable;
use crate::web::web_error::WebError;

use super::app_state::AppState;

/// Returns the protein for given accession.
///
/// # Arguments
/// * `db_client` - The database client
/// * `configuration` - MaCPepDB configuration
/// * `accession` - Protein accession extracted from URL path
///
/// # API
/// ## Request
/// * Path: `/api/proteins/:sequence`
/// * Method: `GET`
///
/// ## Response
/// ```json
/// {
///     "accession": "Q9WTP6",
///     "domains": [],
///     "entry_name": "KAD2_MOUSE",
///     "genes": [
///         "Ak2"
///     ],
///     "is_reviewed": true,
///     "name": "Adenylate kinase 2, mitochondrial {ECO:0000255|HAMAP-Rule:MF_03168}",
///     # List of peptide sequences as stored in the database
///     "peptides": [
///         "SYHEEFNPPK",
///         ...,
///         "KLKATMDAGK"
///     ],
///     "proteome_id": "UP000000589",
///     "secondary_accessions": [
///         "A2A820",
///         "Q3THT3",
///         "Q3TI11",
///         "Q3TKI6",
///         "Q8C7I9",
///         "Q9CY37"
///     ],
///     "sequence": "MAPNVLASEPEIPKGIRAVLLGPPG...DLVMFI",
///     "taxonomy_id": 10090,
///     "updated_at": 1687910400
/// }
/// ```
///
pub async fn get_protein(
    State(app_state): State<Arc<AppState>>,
    Path(accession): Path<String>,
) -> Result<Json<JsonValue>, WebError> {
    let accession = accession.to_uppercase();

    let protein_opt = ProteinTable::select(
        app_state.get_db_client_as_ref(),
        "WHERE accession = ?",
        &[&CqlValue::Text(accession)],
    )
    .await?;

    if let Some(protein) = protein_opt {
        // Enzymes are not saved in the database, so we have to create them
        // therefore we need the enzyme from the configuration
        let enzyme = get_enzyme_by_name(
            app_state.get_configuration_as_ref().get_enzyme_name(),
            app_state
                .get_configuration_as_ref()
                .get_max_number_of_missed_cleavages(),
            app_state
                .get_configuration_as_ref()
                .get_min_peptide_length(),
            app_state
                .get_configuration_as_ref()
                .get_max_peptide_length(),
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
