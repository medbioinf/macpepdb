// std imports
use std::sync::Arc;

// 3rd party imports
use axum::extract::{Json, State};

// internal imports
use crate::entities::configuration::Configuration;

pub async fn get_configuration(
    State(configuration): State<Arc<Configuration>>,
) -> Json<Configuration> {
    Json(configuration.as_ref().clone())
}
