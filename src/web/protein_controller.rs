// std imports
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use async_stream::stream;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum_streams::StreamBodyAs;
use dihardts_omicstools::proteomics::proteases::functions::get_by_name as get_protease_by_name;
use scylla::frame::response::result::CqlValue;
use serde_json::Value as JsonValue;
use tracing::error;

// internal imports
use crate::database::scylla::protein_table::ProteinTable;
use crate::database::selectable_table::SelectableTable;
use crate::web::web_error::WebError;

use super::app_state::AppState;

/// Returns the protein for given accession.
/// Important: This endpoint will return the the protein including a list of full records of the contained peptides. The peptides will contain just the protein accession. Not the entire protein record.
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
///         {
///             "aa_counts": [
///                 ...
///             ],
///             "domains": [],
///             "is_swiss_prot": true,
///             "is_trembl": false,
///             "mass": 587.375495125,
///             "missed_cleavages": 1,
///             "partition": 1,
///             "proteins": [
///             	"Q9WTP6"
///             ],
///             "proteome_ids": [
///             	"UP000000589"
///             ],
///             "sequence": "ALKTR",
///             "taxonomy_ids": [
///             	10090
///             ],
///             "unique_taxonomy_ids": [
///             	10090
///             ]
///         },
///         ...
///     ],
///     "proteome_id": "UP000000589",
///     "secondary_accessions": [
///         "A2A820",
///         ...
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
        // Proteases are not saved in the database, so we have to create them
        // therefore we need the protease from the configuration
        let protease = get_protease_by_name(
            app_state.get_configuration_as_ref().get_protease_name(),
            app_state
                .get_configuration_as_ref()
                .get_min_peptide_length(),
            app_state
                .get_configuration_as_ref()
                .get_max_peptide_length(),
            app_state
                .get_configuration_as_ref()
                .get_max_number_of_missed_cleavages(),
        )?;
        // Get the protein with the peptides as JSON value
        return Ok(Json(
            protein
                .to_json_with_peptides(
                    app_state.get_db_client().clone(),
                    app_state.get_configuration_as_ref().get_partition_limits(),
                    protease.as_ref(),
                )
                .await?,
        ));
    } else {
        return Err(WebError::new(
            StatusCode::NOT_FOUND,
            "Protein not found".to_string(),
        ));
    }
}

/// Search protein for given accession or gene name. Gene name needs to be exact, while accession
/// will be wrapped in a like-query.
///
/// # Arguments
/// * `db_client` - The database client
/// * `configuration` - MaCPepDB configuration
/// * `attribute` - Accession or gene name, will be wrapped in a like-query
///
/// # API
/// ## Request
/// * Path: `/api/proteins/search/:attribute`
/// * Method: `GET`
///
/// ## Response
/// ```json
/// [
///     {
///         "accession": "Q9WTP6",
///         "domains": [],
///         "entry_name": "KAD2_MOUSE",
///         "genes": [
///             "Ak2"
///         ],
///         "is_reviewed": true,
///         "name": "Adenylate kinase 2, mitochondrial {ECO:0000255|HAMAP-Rule:MF_03168}",
///         # List of peptide sequences as stored in the database
///         "peptides": [
///             "SYHEEFNPPK",
///             ...,
///             "KLKATMDAGK"
///         ],
///         "proteome_id": "UP000000589",
///         "secondary_accessions": [
///             "A2A820",
///             "Q3THT3",
///             "Q3TI11",
///             "Q3TKI6",
///             "Q8C7I9",
///             "Q9CY37"
///         ],
///         "sequence": "MAPNVLASEPEIPKGIRAVLLGPPG...DLVMFI",
///         "taxonomy_id": 10090,
///         "updated_at": 1687910400
///     },
///    ...
/// ]
/// ```
///
pub async fn search_protein(
    State(app_state): State<Arc<AppState>>,
    Path(attribute): Path<String>,
) -> impl IntoResponse {
    if attribute.len() < 3 {
        return StreamBodyAs::text(WebError::new_string_stream(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Attribute must be at least 3 characters long".to_string(),
        ));
    }

    let attribute = attribute.replace("%", "\\%);").replace("_", "\\_");

    let protease = match get_protease_by_name(
        app_state.get_configuration_as_ref().get_protease_name(),
        app_state
            .get_configuration_as_ref()
            .get_min_peptide_length(),
        app_state
            .get_configuration_as_ref()
            .get_max_peptide_length(),
        app_state
            .get_configuration_as_ref()
            .get_max_number_of_missed_cleavages(),
    ) {
        Ok(protease) => protease,
        Err(err) => {
            return StreamBodyAs::text(WebError::new_string_stream(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error when creating protease: {}", err),
            ));
        }
    };

    let proteins = match ProteinTable::search(app_state.get_db_client(), attribute.clone()).await {
        Ok(proteins) => proteins,
        Err(err) => {
            return StreamBodyAs::text(WebError::new_string_stream(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error when selecting proteins: {}", err),
            ));
        }
    };

    StreamBodyAs::json_array(stream! {
        for await protein in proteins {
            match protein {
                Ok(protein) => yield match protein.to_json_with_peptides(
                    app_state.get_db_client().clone(),
                    app_state.get_configuration_as_ref().get_partition_limits(),
                    protease.as_ref(),
                ).await {
                    Ok(json) => json,
                    Err(err) => {
                        error!("{:?}", err);
                        continue;
                    }
                },
                Err(err) => error!("{:?}", err)
            }
        }
    })
}
