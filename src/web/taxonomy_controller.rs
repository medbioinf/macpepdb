// std imports
use std::collections::HashMap;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use dihardts_omicstools::biology::taxonomy::Taxonomy;
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

// internal imports
use crate::web::app_state::AppState;
use crate::web::web_error::WebError;

fn taxonomy_to_json(taxonomy: &Taxonomy, taxonomy_ranks: &HashMap<u64, String>) -> JsonValue {
    json!({
        "id": taxonomy.get_id(),
        "parent_id": taxonomy.get_parent_id(),
        "scientific_name": taxonomy.get_scientific_name(),
        "rank": taxonomy_ranks.get(&taxonomy.get_rank_id()).unwrap_or(&"unknown".to_string())
    })
}

/// Returns the taxonomy for a given ID. If the taxonomy was merged with another on the new one is returned.
///
/// # Arguments
/// * `taxonomy_tree` - The pre build taxonomy tree
///
/// # API
/// ## Request
/// * Path: `/api/taxonomies/:id`
/// * Method: `GET`
///
/// ## Response
/// Peptides are formatted as mentioned in the [`get_peptide`-endpoint](crate::web::peptide_controller::get_peptide).
/// ```json
/// {
///    "id": 9606,
///    "parent_id": 9605,
///    "rank": "species",
///    "scientific_name": "Homo sapiens"
/// }
/// ```
///
pub async fn get_taxonomy(
    State(app_state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<JsonValue>, WebError> {
    let taxonomy_tree = app_state.get_taxonomy_tree_as_ref();
    match taxonomy_tree.get_taxonomy(id) {
        Some(taxonomy) => Ok(Json(taxonomy_to_json(taxonomy, taxonomy_tree.get_ranks()))),
        None => Err(WebError::new(
            StatusCode::NOT_FOUND,
            format!("Could not find taxonomy with ID {}", id),
        )),
    }
}

/// Returns all sub taxonomies for a given ID. If the initial taxonomy was merged with another on the new one is used.
///
/// # Arguments
/// * `taxonomy_tree` - The pre build taxonomy tree
///
/// # API
/// ## Request
/// * Path: `/api/taxonomies/:id/sub`
/// * Method: `GET`
///
/// ## Response
/// Taxonomies are formatted as follows
/// ```json
/// [
///
///     {
///        "id": 9606,
///        "parent_id": 9605,
///        "rank": "species",
///        "scientific_name": "Homo sapiens"
///     }
///     ...
/// ]
/// ```
///
pub async fn get_sub_taxonomies(
    State(app_state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<JsonValue>, WebError> {
    let taxonomy_tree = app_state.get_taxonomy_tree_as_ref();
    match taxonomy_tree.get_sub_taxonomies(id) {
        Some(taxonomies) => Ok(Json(
            taxonomies
                .iter()
                .map(|taxonomy| taxonomy_to_json(taxonomy, taxonomy_tree.get_ranks()))
                .collect(),
        )),
        None => Err(WebError::new(
            StatusCode::NOT_FOUND,
            format!("Could not find taxonomy with ID {}", id),
        )),
    }
}

/// Simple struct to deserialize the request body for taxonomy search
///
#[derive(Deserialize)]
pub struct SearchRequestBody {
    name_query: String,
}

impl SearchRequestBody {
    pub fn get_name_query(&self) -> &str {
        self.name_query.as_str()
    }
}

/// Searches a taxonomies by their names   
/// **Attention:** This endpoint can be disabled on the server side. If it is disabled a `501` is returned with
/// with a message explaining that the endpoint is disabled.
///
/// # Arguments
/// * `db_client` - The database client
/// * `taxonomy_tree` - The pre build taxonomy tree
///
/// # API
/// ## Request
/// * Path: `/api/taxonomies/search`
/// * Method: `POST`
///
/// * Body:
///     ```json
///     {
///         name_query: "*sapiens*"
///     }
///     ```
///     Deserialized into [SearchRequestBody]
///
///
/// ## Response
/// Taxonomies are formatted as follows
/// ```json
/// [
///     {
///        "id": 9606,
///        "parent_id": 9605,
///        "rank": "species",
///        "scientific_name": "Homo sapiens"
///     },
///     ...
/// ]
/// ```
///
pub async fn search_taxonomies(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<SearchRequestBody>,
) -> Result<Json<Vec<JsonValue>>, WebError> {
    let taxonomy_tree = app_state.get_taxonomy_tree_as_ref();
    let taxonomy_search_idx = match app_state.get_taxonomy_index_as_ref() {
        Some(idx) => idx,
        None => {
            return Err(WebError::new(
                StatusCode::NOT_IMPLEMENTED,
                "Taxonomy search is disable on this instance of MaCPepDB".to_string(),
            ))
        }
    };
    Ok(Json(
        taxonomy_search_idx
            .search(payload.get_name_query())
            .iter()
            .map(|id| {
                let taxonomy = match taxonomy_tree.get_taxonomy(**id) {
                    Some(taxonomy) => Ok(taxonomy),
                    None => Err(WebError::new(
                        StatusCode::NOT_FOUND,
                        format!("Could not find taxonomy with ID {}", id),
                    )),
                };
                Ok(taxonomy_to_json(taxonomy?, taxonomy_tree.get_ranks()))
            })
            .collect::<Result<Vec<JsonValue>, WebError>>()?,
    ))
}
