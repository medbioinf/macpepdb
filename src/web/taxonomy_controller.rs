use std::collections::HashMap;
// std imports
use std::sync::Arc;
// 3rd party imports
use anyhow::Result;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use dihardts_omicstools::biology::taxonomy::{Taxonomy, TaxonomyTree};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::database::scylla::client::Client;
use crate::database::scylla::taxonomy_table::TaxonomyTable;
// internal imports
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
    State(taxonomy_tree): State<Arc<TaxonomyTree>>,
    Path(id): Path<u64>,
) -> Result<Json<JsonValue>, WebError> {
    match taxonomy_tree.get_taxonomy(id) {
        Some(taxonomy) => Ok(Json(taxonomy_to_json(&taxonomy, taxonomy_tree.get_ranks()))),
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
    State(taxonomy_tree): State<Arc<TaxonomyTree>>,
    Path(id): Path<u64>,
) -> Result<Json<JsonValue>, WebError> {
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

#[derive(Serialize)]
struct SerializableTaxonomy<'a> {
    id: u64,
    parent_id: u64,
    scientific_name: &'a str,
    rank: &'a String,
}

/// Searches a taxonomies by their names
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
///     Deserialized into [SearchRequestBody](SearchRequestBody)
///
///
/// ## Response
/// Taxonomies are formatted as follows
/// ```json
/// [
///     {
///         "peptides": [
///             peptide_1,
///             peptide_2,
///            ...
///         ],
///         "db_peptides": [
///            peptide_1,
///            peptide_2,
///           ...
///        ]
///     },
///     ...
/// ]
/// ```
///
pub async fn search_taxonomies(
    State((db_client, taxonomy_tree)): State<(Arc<Client>, Arc<TaxonomyTree>)>,
    Json(payload): Json<SearchRequestBody>,
) -> Result<Json<Vec<JsonValue>>, WebError> {
    let ids: Vec<u64> =
        TaxonomyTable::search_taxonomy_by_name(db_client.as_ref(), payload.get_name_query())
            .await?
            .then(|id| async move { Ok::<u64, anyhow::Error>(id?) })
            .try_collect()
            .await?;

    Ok(Json(
        ids.into_iter()
            .map(|id| {
                let taxonomy = match taxonomy_tree.get_taxonomy(id) {
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

    // Need to handle WebError manually, because we need to return a stream
    // let id_stream =
    //     match TaxonomyTable::search_taxonomy_by_name(db_client.clone(), payload.get_name_query())
    //         .await
    //     {
    //         Ok(id_stream) => id_stream,
    //         Err(err) => {
    //             return StreamBodyAs::text(WebError::new_string_stream(
    //                 StatusCode::BAD_REQUEST,
    //                 format!("Error while parsing modifications: {:?}", err),
    //             ));
    //         }
    //     };

    // StreamBodyAs::json_array(stream! {
    //     let local_taxonomy_tree = taxonomy_tree.clone();
    //     for await id in id_stream {
    //         if let Ok(id) = id {
    //             match local_taxonomy_tree.get_taxonomy(id) {
    //                 Some(taxonomy) => yield SerializableTaxonomy{
    //                     id: taxonomy.get_id(),
    //                     parent_id: taxonomy.get_parent_id(),
    //                     scientific_name: taxonomy.get_scientific_name(),
    //                     rank: taxonomy_tree.get_ranks().get(&taxonomy.get_rank_id()).unwrap_or(&"unknown".to_string())
    //                 },
    //                 None => error!("Could not find taxonomy with ID {}", id)
    //             }
    //             continue;
    //         }
    //         error!("{:?}", id.unwrap_err());
    //     }
    // })
}
