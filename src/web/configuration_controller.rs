// std imports
use std::sync::Arc;

// 3rd party imports
use axum::extract::{Json, State};

// internal imports
use crate::entities::configuration::Configuration;

/// Returns the configuration with which MaCPepDB was created.
///
/// # Arguments
/// * `configuration` - MaCPepDB configuration
///
/// # API
/// ## Request
/// * Path: `/api/configuration`
/// * Method: `GET`
///
/// ## Response
/// ```json
/// {
///     "enzyme_name": "trypsin",
///     "max_number_of_missed_cleavages": 2,
///     "min_peptide_length": 5,
///     "max_peptide_length": 60,
///     "remove_peptides_containing_unknown": true,
///     "partition_limits": [
///         565249110009,
///         593899929397,
///         622550748785,
///         651201568173,
///         678101346913,
///         ...
///         6028263516592,
///         6349645610325,
///         11164758778800
///     ]
/// }
/// ```
///
pub async fn get_configuration(
    State(configuration): State<Arc<Configuration>>,
) -> Json<Configuration> {
    Json(configuration.as_ref().clone())
}
