// 3rd party imports
use axum::extract::Path;
use axum::http::StatusCode;
use axum::Json;
use dihardts_omicstools::chemistry::amino_acid::{
    get_amino_acid_by_one_letter_code, CANONICAL_AMINO_ACIDS, NON_CANONICAL_AMINO_ACIDS,
};
use serde_json::Value as JsonValue;

// internal imports
use crate::tools::omicstools::amino_acid_to_json;
use crate::web::web_error::WebError;

/// Gets the amino acid by one letter code
///
/// # Arguments
/// * `code` - Amino acid one letter code
///
/// # API
/// ## Request
/// * Path: `/api/chemistry/amino_acids/:code`
/// * Method: `GET`
///
/// ## Response
/// ```json
/// {
///     "abbreviation": "Gly",
///     "average_mass": 57.05132,
///     "code": "G",
///     "mono_mass": 57.021463735,
///     "name": "Glycine"
/// }
/// ```
///
pub async fn get_amino_acid(Path(code): Path<String>) -> Result<Json<JsonValue>, WebError> {
    if code.len() != 1 {
        return Err(WebError::new(
            StatusCode::BAD_REQUEST,
            "The amino acid code must be exactly one character long".to_string(),
        ));
    }

    let amino_acid = get_amino_acid_by_one_letter_code(code.chars().next().unwrap())?;

    Ok(Json(amino_acid_to_json(amino_acid)))
}

/// Returns all amino acids
///
/// # Arguments
/// * `code` - Amino acid one letter code
///
/// # API
/// ## Request
/// * Path: `/api/chemistry/amino_acids`
/// * Method: `GET`
///
/// ## Response
/// ```json
/// [
///     {
///         "abbreviation": "Gly",
///         "average_mass": 57.05132,
///         "code": "G",
///         "mono_mass": 57.021463735,
///         "name": "Glycine"
///     },
///    ...
/// ]
/// ```
///
pub async fn get_all_amino_acids() -> Result<Json<JsonValue>, WebError> {
    let mut aa_jsons = CANONICAL_AMINO_ACIDS
        .iter()
        .map(|aa| amino_acid_to_json(aa))
        .collect::<Vec<JsonValue>>();

    aa_jsons.extend(
        NON_CANONICAL_AMINO_ACIDS
            .iter()
            .map(|aa| amino_acid_to_json(aa)),
    );

    let aa_jsons = match serde_json::to_value(aa_jsons) {
        Ok(json) => json,
        Err(err) => {
            return Err(WebError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error serializing amino acids to JSON, {}", err),
            ));
        }
    };

    Ok(Json(aa_jsons))
}
