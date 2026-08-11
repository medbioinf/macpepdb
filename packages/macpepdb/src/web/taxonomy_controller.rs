use std::fmt::Write;
use std::ops::Deref;
use std::sync::Arc;

use async_stream::stream;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::TryStreamExt;
use http::{HeaderMap, HeaderValue, StatusCode, header};
use macpepdb_web_common::requests::taxonomy::SearchRequestBody;
use macpepdb_web_common::responses::taxonomy::TaxonomyResponse;
use postgres_types::ToSql;
use thiserror::Error;

use crate::taxonomy_table::{ID_COL, SCIENTIFIC_NAME_COL, TABLE_NAME, TaxonomyTable};
use crate::web::DEFAULT_ERROR_HEADER_MAP;
use crate::web::server_state::ServerState;

static CONTROLLER_PATH: &str = "/api/taxonomies";
static TAXONOMY_PATH: &str = "/{id}";
static SUB_SPECIES_PATH: &str = "/{id}/sub";
static SEARCH_TAXONOMIES_PATH: &str = "/search";
static ID_RESOLVE_PATH: &str = "/resolve-ids";

static RESOLVE_ID_BODY_SIZE_LIMIT: usize = 10485760; // 10 MiB

/// Errors that can occur while handling taxonomy endpoints.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Taxonomy error: {0}")]
    Taxonomy(Box<crate::taxonomy::Error>),
    #[error("Taxonomy not found")]
    TaxonomyNotFound,
    #[error("Taxonomy table error: {0}")]
    TaxonomyTable(Box<crate::taxonomy_table::Error>),
    #[error("Response error: {0}")]
    Response(Box<http::Error>),
}

into_thiserror_boxed!(crate::taxonomy::Error, Error, Taxonomy);
into_thiserror_boxed!(crate::taxonomy_table::Error, Error, TaxonomyTable);
into_thiserror_boxed!(http::Error, Error, Response);

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::Taxonomy(err) => (
                StatusCode::BAD_REQUEST,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(format!("{}", err)),
            )
                .into_response(),
            Error::TaxonomyNotFound => (
                StatusCode::NOT_FOUND,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from("Peptide not found".to_string()),
            )
                .into_response(),
            _ => {
                let uuid = uuid::Uuid::now_v7();
                tracing::error!("[{uuid}] {self}");

                (
                StatusCode::INTERNAL_SERVER_ERROR,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(format!("Internal server error. Contact the admin and provide this UUID `{uuid}` to help identifying the error.")),
            )
                .into_response()
            }
        }
    }
}

/// Controller providing taxonomy lookup and search endpoints under `/api/taxonomies`.
pub struct TaxonomyController;

impl TaxonomyController {
    /// Builds the axum router for the taxonomy endpoints, mounted onto the given server state.
    pub fn routes(state: Arc<ServerState>) -> Router<Arc<ServerState>> {
        let router: Router<Arc<ServerState>> = Router::new()
            .route(TAXONOMY_PATH, get(Self::taxonomy))
            .route(SUB_SPECIES_PATH, get(Self::sub_species))
            .route(SEARCH_TAXONOMIES_PATH, post(Self::search_taxonomies))
            .route(
                ID_RESOLVE_PATH,
                post(Self::resolve_ids).layer(DefaultBodyLimit::max(RESOLVE_ID_BODY_SIZE_LIMIT)),
            );

        router.with_state(state)
    }

    /// Returns the base path this controller is mounted on (`/api/taxonomies`).
    pub fn controller_path() -> &'static str {
        CONTROLLER_PATH
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
    /// Peptides are formatted as mentioned in the [`show`-endpoint](crate::web::peptide_controller::PeptideController::show).
    /// ```json
    /// {
    ///    "id": 9606,
    ///    "parent_id": 9605,
    ///    "rank_id": 1,
    ///    "rank_name": "species",
    ///    "scientific_name": "Homo sapiens"
    /// }
    /// ```
    ///
    pub async fn taxonomy(
        State(state): State<Arc<ServerState>>,
        Path(id): Path<i32>,
    ) -> Result<Json<TaxonomyResponse>, Error> {
        let where_clause = format!("WHERE {TABLE_NAME}.{ID_COL} = $1 LIMIT 1",);

        let taxonomy = TaxonomyTable::new(state.db_client())
            .select_with_rank(&where_clause, vec![Box::new(id)])
            .await?
            .try_next()
            .await?;

        match taxonomy {
            Some(taxonomy) => Ok(Json((&taxonomy).into())),
            None => Err(Error::TaxonomyNotFound),
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
    ///        "rank_id": 1,
    ///        "rank_name": "species",
    ///        "scientific_name": "Homo sapiens"
    ///     }
    ///     ...
    /// ]
    /// ```
    ///
    pub async fn sub_species(
        State(state): State<Arc<ServerState>>,
        Path(id): Path<i32>,
    ) -> Result<(StatusCode, HeaderMap, Body), Error> {
        let taxonomy_stream = TaxonomyTable::new(state.db_client())
            .select_sub_species(id)
            .await?;

        let stream = stream! {
            // start json array
            yield Ok("[".to_string());
            // set delimiter to empty string for first element
            let mut delimiter = "".to_string();
            for await taxonomy_res in taxonomy_stream {
                match taxonomy_res {
                    Ok(taxonomy) => {
                        yield Ok(delimiter.to_owned());
                        // convert to json and yield
                        match serde_json::to_string(&TaxonomyResponse::from(&taxonomy)) {
                            Ok(json) => yield Ok(json),
                            Err(err) => {
                                tracing::error!("{:?}", err);
                                yield Err(format!("!!! {:?}", err));
                                break;
                            }
                        };
                    }
                    Err(err) => {
                        tracing::error!("{:?}", err);
                        yield Err(format!("!!! {:?}", err));
                        break;
                    }
                };
                delimiter = ",".to_string();
            }
            // end json array
            yield Ok("]".to_string());
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );

        Ok((StatusCode::OK, headers, Body::from_stream(stream)))
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
    ///         "search_query": "sapiens"
    ///     }
    ///     ```
    ///   Deserialized into [SearchRequestBody] (matched as an exact taxonomy ID or a
    ///   substring of the scientific name — no wildcard characters needed, the backend
    ///   wraps the term in SQL `%...%` itself)
    ///
    ///
    /// ## Response
    /// Taxonomies are formatted as follows
    /// ```json
    /// [
    ///     {
    ///        "id": 9606,
    ///        "parent_id": 9605,
    ///        "rank_id": 1,
    ///        "rank_name": "species",
    ///        "scientific_name": "Homo sapiens"
    ///     },
    ///     ...
    /// ]
    /// ```
    ///
    pub async fn search_taxonomies(
        State(state): State<Arc<ServerState>>,
        Json(payload): Json<SearchRequestBody>,
    ) -> Result<(StatusCode, HeaderMap, Body), Error> {
        let (where_clause, params): (String, Vec<Box<dyn ToSql + Sync + Send>>) =
            if let Ok(id) = payload.search_query.parse::<i32>() {
                (
                    format!("WHERE {TABLE_NAME}.{ID_COL} = $1"),
                    vec![Box::new(id)],
                )
            } else {
                (
                    format!("WHERE {SCIENTIFIC_NAME_COL} ILIKE $1"),
                    vec![Box::new(format!("%{}%", payload.search_query))],
                )
            };

        let taxonomy_stream = TaxonomyTable::new(state.db_client())
            .select_with_rank(&where_clause, params)
            .await?;

        let stream = stream! {
            // start json array
            yield Ok("[".to_string());
            // set delimiter to empty string for first element
            let mut delimiter = "".to_string();
            for await taxonomy_res in taxonomy_stream {
                match taxonomy_res {
                    Ok(taxonomy) => {
                        yield Ok(delimiter.to_owned());
                        // convert to json and yield
                        match serde_json::to_string(&TaxonomyResponse::from(&taxonomy)) {
                            Ok(json) => yield Ok(json),
                            Err(err) => {
                                tracing::error!("{:?}", err);
                                yield Err(format!("!!! {:?}", err));
                                break;
                            }
                        };
                    }
                    Err(err) => {
                        tracing::error!("{:?}", err);
                        yield Err(format!("!!! {:?}", err));
                        break;
                    }
                };
                delimiter = ",".to_string();
            }
            // end json array
            yield Ok("]".to_string());
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );

        Ok((StatusCode::OK, headers, Body::from_stream(stream)))
    }

    /// Resolves taxonomy database IDs to their scientific names, streaming the result in
    /// chunks of 10000 IDs per query to keep memory usage bounded for large requests.
    ///
    /// # Arguments
    /// * `state` - Server state
    /// * `ids` - Taxonomy database IDs to resolve
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/taxonomies/resolve-ids`
    /// * Method: `POST`
    /// * Body:
    ///     ```json
    ///     [9606, 10090]
    ///     ```
    ///
    /// ## Response
    /// ```json
    /// {
    ///     "9606": "Homo sapiens",
    ///     "10090": "Mus musculus"
    /// }
    /// ```
    ///
    pub async fn resolve_ids(
        State(state): State<Arc<ServerState>>,
        Json(ids): Json<Vec<i32>>,
    ) -> Result<Response, Error> {
        let stream = stream! {
            let mut delimiter = "";
            yield Ok("{".to_string());
            for id_chunk in ids.chunks(10000) {
                let name_stream = match TaxonomyTable::new(state.db_client())
                    .resolve_ids(id_chunk.to_vec())
                    .await {
                        Ok(names) => names,
                        Err(err) => {
                            yield Err(format!("!!! {:?}", err));
                            break;
                        }
                    };

                let names = match name_stream.try_collect::<Vec<_>>().await {
                    Ok(names) => names,
                    Err(err) => {
                        yield Err(format!("!!! {:?}", err));
                        break;
                    }
                };

                let mut chunk = String::with_capacity(names.len() * 25);
                for (id, scientific_name) in names {
                    write!(
                        chunk,
                        "{}\"{}\":\"{}\"",
                        delimiter,
                        id,
                        scientific_name
                    ).unwrap();
                    delimiter = ",";
                }
                yield Ok(chunk);
            }

            yield Ok("}".to_string());
        };

        let response = Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Body::from_stream(stream))?;

        Ok(response)
    }
}
