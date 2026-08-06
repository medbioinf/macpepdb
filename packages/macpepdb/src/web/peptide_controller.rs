use std::fmt::Write as _;
use std::ops::Deref;
use std::sync::{Arc, OnceLock};

use async_stream::stream;
use axum::Router;
use axum::body::Body;
use axum::extract::{Json, Path, Query, State};
use axum::http::header::ACCEPT;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::{Engine as _, engine::general_purpose::STANDARD as Base64Standard};
use futures::{StreamExt, TryStreamExt};
use http::header;
use macpepdb_web_common::requests::peptide::{
    SearchRequestBody, SearchRequestMass, SearchRequestQuery,
};
use macpepdb_web_common::responses::peptide::PeptideResponse;
use postgres_types::ToSql;
use thiserror::Error;
use tokio_postgres::Row;
use urlencoding::decode as urldecode;

use crate::mass::mass_to_charge_to_dalton;
use crate::peptide::{IsPeptide, Peptide};
use crate::peptide_search::{
    PeptideSearch, PeptidoformToJsonTransformation, PeptidoformToPlainTextTransformation,
};
use crate::peptide_table::{FULL_PEPTIDE_COLUMN_SELECTION, PeptideColumnSelection, PeptideTable};
use crate::post_translational_modification::{PTMCollection, PostTranslationalModification};
use crate::protein_ids::ProteinIds;
use crate::protein_table::ProteinTable;
use crate::taxonomy_table::TaxonomyTable;
use crate::web::DEFAULT_ERROR_HEADER_MAP;
use crate::web::server_state::ServerState;

const DEFAULT_POST_SEARCH_ACCEPT_HEADER: &str = "application/json";

/// Target size (bytes) for buffered chunks written to the response stream, before yielding.
/// Matches the HTTP/2 `max_frame_size` configured in `web::server` so buffered chunks aren't
/// re-split by h2 framing.
const RESPONSE_STREAM_CHUNK_THRESHOLD: usize = 64 * 1024;

pub static CONTROLLER_PATH: &str = "/api/peptides";
static SEARCH_GET_PATH: &str = "/search/{payload}/{accept}";
pub static SEARCH_POST_PATH: &str = "/search";
static EXISTS_PATH: &str = "/{sequence}/exists";
static SHOW_PATH: &str = "/{sequence}";

/// Errors that can occur while handling peptide endpoints.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Requested output format is invalid")]
    InvalidAcceptHeader,
    #[error("Peptide error: {0}")]
    Peptide(#[from] crate::peptide::Error),
    #[error("Peptide not found")]
    PeptideNotFound,
    #[error("Peptide search: {0}")]
    PeptideSearch(#[from] crate::peptide_search::Error),
    #[error("Peptide table error: {0}")]
    PeptideTable(#[from] crate::peptide_table::Error),
    #[error("Protein table: {0}")]
    ProteinTable(#[from] crate::protein_table::Error),
    #[error("Taxonomy with ID `{0}` not found. Are you sure it exists in NCBI?")]
    TaxonomyNotFound(i32),
    #[error("Taxonomy table error: {0}")]
    TaxonomyTable(#[from] crate::taxonomy_table::Error),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::InvalidAcceptHeader => (
                StatusCode::NOT_ACCEPTABLE,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(self.to_string()),
            )
                .into_response(),
            Error::Peptide(err) => (
                StatusCode::BAD_REQUEST,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(format!("{}", err)),
            )
                .into_response(),
            Error::PeptideNotFound => (
                StatusCode::NOT_FOUND,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from("Peptide not found".to_string()),
            )
                .into_response(),
            Error::TaxonomyNotFound(err) => (
                StatusCode::NOT_FOUND,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(format!(
                    "Taxonomy with ID `{err}` not found. Are you sure it exists in NCBI?"
                )),
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

pub const SEQUENCE_PEPTIDE_COLUMN_SELECTION: PeptideColumnSelection = PeptideColumnSelection {
    columns: "mass, sequence",
    try_peptide_from_row_fn: |row: &Row| {
        Ok(Peptide::full_new(
            None,
            row.try_get(0)?,
            row.try_get(1)?,
            ProteinIds::default(),
            Vec::new(),
            Vec::new(),
            0,
            OnceLock::new(),
        ))
    },
};

// TODO: Adjust all controller methods to return error instread ok Ok(body) with error message.
/// Controller providing peptide lookup and mass search endpoints under `/api/peptides`.
pub struct PeptideController;

impl PeptideController {
    /// Builds the axum router for the peptide endpoints, mounted onto the given server state.
    pub fn routes(state: Arc<ServerState>) -> Router<Arc<ServerState>> {
        let router: Router<Arc<ServerState>> = Router::new()
            .route(SEARCH_POST_PATH, post(Self::search_by_post_request))
            .route(SEARCH_GET_PATH, get(Self::search_by_get_request))
            .route(EXISTS_PATH, get(Self::exists))
            .route(SHOW_PATH, get(Self::show));

        router.with_state(state)
    }

    /// Returns the base path this controller is mounted on (`/api/peptides`).
    pub fn controller_path() -> &'static str {
        CONTROLLER_PATH
    }

    /// Returns the peptide for given sequence.
    /// Important: `protein_ids` are raw protein database IDs, not resolved protein records —
    /// fetch `/api/proteins/{accession}` on demand for a specific protein instead.
    ///
    /// # Arguments
    /// * `state` - Server state
    /// * `sequence` - Sequence from path segment
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/peptides/:sequence`
    /// * Method: `GET`
    ///
    /// ## Response
    /// ```json
    /// {
    ///     "partition": 19,
    ///     "mass": 1015.475679562,
    ///     "sequence": "HMENEKTK",
    ///     "protein_ids": [123, 456],
    ///     "unique_taxonomy_ids": [10090],
    ///     "non_unique_taxonomy_ids": [10090],
    ///     "is_swiss_prot": true,
    ///     "is_trembl": false
    /// }
    /// ```
    ///
    pub async fn show(
        State(server_state): State<Arc<ServerState>>,
        Path(sequence): Path<String>,
    ) -> Result<Json<PeptideResponse>, Error> {
        let peptide = Peptide::try_from(sequence)?;

        let peptide = Self::select_one_peptide(&server_state, &peptide)
            .await?
            .ok_or(Error::PeptideNotFound)?;

        let proteins = ProteinTable::new(server_state.db_client())
            .select(
                "WHERE id = ANY($1)",
                vec![Box::new(peptide.protein_ids().as_vec()) as Box<dyn ToSql + Sync + Send>],
            )
            .await?
            .map(|protein_res| protein_res.map(|protein| protein.to_shallow_response()))
            .try_collect::<Vec<_>>()
            .await?;

        let mut peptide = PeptideResponse::from(&peptide);
        peptide.proteins = Some(Arc::new(proteins));

        Ok(Json(peptide))
    }

    /// Returns if a peptide exists.
    ///
    /// # Arguments
    /// * `state` - Server state
    /// * `sequence` - Peptide sequence from path
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/peptides/:sequence/exists`
    /// * Method: `GET`
    ///
    /// ## Response
    /// Response will be empty.
    /// Statuscode 200 if peptide exists, otherwise 404
    ///
    pub async fn exists(
        State(server_state): State<Arc<ServerState>>,
        Path(sequence): Path<String>,
    ) -> Result<Response, Error> {
        let peptide = Peptide::try_from(sequence)?;

        if Self::select_one_peptide(&server_state, &peptide)
            .await?
            .is_some()
        {
            Ok((StatusCode::OK, "").into_response())
        } else {
            Ok((StatusCode::NOT_FOUND, "").into_response())
        }
    }

    /// Looks up a single stored peptide by `(mass, sequence)`, resolving the candidate
    /// partitions for the mass from the configuration's mass partitioning. Returns `None`
    /// if the mass has no partitions or the sequence is not present.
    async fn select_one_peptide(
        server_state: &ServerState,
        peptide: &Peptide,
    ) -> Result<Option<Peptide>, Error> {
        let mass = peptide.mass();
        let partitions: Vec<i64> = server_state
            .configuration()
            .mass_partitioning()
            .partition_by_mass(mass)
            .collect();

        if partitions.is_empty() {
            return Ok(None);
        }

        let params: Vec<Box<dyn ToSql + Sync + Send>> = vec![
            Box::new(partitions),
            Box::new(mass),
            Box::new(peptide.sequence().clone()),
        ];

        let mut stream = PeptideTable::new(server_state.db_client())
            .select(
                &FULL_PEPTIDE_COLUMN_SELECTION,
                "WHERE partition = ANY($1) AND mass = $2 AND sequence = $3 LIMIT 1",
                params,
            )
            .await?;

        Ok(stream.next().await.transpose()?)
    }

    #[allow(clippy::tabs_in_doc_comments)]
    /// Returns a stream of peptides matching the given parameters.
    /// If the taxonomy ID is given and has sub taxonomies, the sub taxonomies are also searched.
    ///
    /// **Important:**
    /// * Peptides only contain the accession of the proteins of origin.
    /// * text/plain and text/fasta will always send an empty line at the start of a stream. In case not peptides were found the stream
    ///   to the client is not empty and cannot trigger an error in client libraries.
    ///
    /// # Arguments
    /// * `state` - Server state
    /// * `headers` - The request headers
    /// * `query` - The query parameters, see [SearchRequestQuery]
    /// * `payload` - The request body, see [SearchRequestBody]
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/peptides/search`
    /// * Method: `POST`
    /// * Headers:
    ///     * `Content-Type`: `application/json`
    ///     * `Accept`: `application/json`, `text/plain`, `text/fasta` (optional, default: `application/json`, controls the output format)
    /// * Query:
    ///     * `is_download`: `bool` (optional, default: `false`, if true set the Content-Disposition header to download the response instead of showing it in the browser)
    /// * Body:
    ///     ```json
    ///     {
    ///         "mass": 1475.748089764,
    ///         "lower_mass_tolerance_ppm": 10,
    ///         "upper_mass_tolerance_ppm": 10,
    ///         "max_variable_modifications": 3,
    ///         "modifications": [
    ///             {
    ///                 "name": "the ususal ",
    ///                 "amino_acid": "C",
    ///                 "mass_delta": 57.021464,
    ///                 "mod_type": "Static",
    ///                 "position": "Anywhere"
    ///             },
    ///             {
    ///                 "name": "the ususal M",
    ///                 "amino_acid": "M",
    ///                 "mass_delta": 15.994915,
    ///                 "mod_type": "Variable",
    ///                 "position": "Anywhere"
    ///             }
    ///         ],
    ///         "resolve_modifications": false
    ///     }
    ///     ```
    ///     See [SearchRequestBody] for details.
    ///
    /// ## Response
    /// ### `application/json`
    /// ```json
    /// [
    ///    peptide_1,
    ///    peptide_2,
    ///    ...
    /// ]
    /// ```
    /// Peptides are formatted as mentioned in the [`show`-endpoint](PeptideController::show) + attribute `additional_sequences` if `resolve_modifications` is true.
    ///
    ///
    /// ### `text/plain`
    /// With resolve modification set to `false`the canonical seqeunces are returned.
    /// ```text
    /// NCLETPSCKNGFLLDGFPR
    /// NCLETPSCKNGFLLMDGFPR
    /// ...
    /// ```
    ///
    /// With resolve modification set to `true` the sequences with the needed modification to reach the target mass are returned in Profoma compliant notation.
    /// ```text
    /// <57.021464@C>NCLETPSCKNGFLLDGFPR
    /// <57.021464@C>NCLETPSCKNGFLLM[+15.994915]DGFPR
    /// ...
    /// ```
    ///
    /// ### `text/fasta`
    /// Again, resolve modification controls if canonical sequences or Profoma compliant notation
    ///
    /// ```text
    /// >mdb|<sequential peptidoform counter>
    /// <57.021464@C>NCLETPSCKNGFLLDGFPR
    /// >mdb|<sequential peptidoform counter>
    /// <57.021464@C>NCLETPSCKNGFLLM[+15.994915]DGFPR
    /// ...
    /// ```
    ///
    pub async fn search_by_post_request(
        State(server_state): State<Arc<ServerState>>,
        headers: HeaderMap,
        Query(query): Query<SearchRequestQuery>,
        Json(payload): Json<SearchRequestBody>,
    ) -> Result<(StatusCode, HeaderMap, Body), Error> {
        let default_header: HeaderValue =
            match HeaderValue::from_str(DEFAULT_POST_SEARCH_ACCEPT_HEADER) {
                Ok(header) => header,
                Err(err) => {
                    return Ok((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        HeaderMap::new(),
                        Body::from(format!("!!! Error while setting default header: {:?}", err)),
                    ));
                }
            };

        let accept_header = headers
            .get(ACCEPT)
            .unwrap_or(&default_header)
            .to_str()
            .unwrap_or(DEFAULT_POST_SEARCH_ACCEPT_HEADER)
            .to_string();

        Self::search(server_state, payload, accept_header, query.is_download).await
    }

    /// This is basically the same as [PeptideController::search_by_post_request], but the payload and mime type are base64 encoded in the URL.
    /// This is useful for GET requests, where the body is not allowed. E.g. for initializing browser downloads via JS or WASM
    /// where the usual blob-download is not possible or would be too large
    ///
    /// # Arguments
    /// * `state` - Server state
    /// * `headers` - The request headers
    /// * `query` - The query parameters, see [SearchRequestQuery]
    /// * `payload` - The request body, see [SearchRequestBody], but urlsafe base64 encoded JSON string
    /// * `accept` - The accept header, but urlsafe base64 encoded
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/peptides/search/:playload/:accept`
    ///     * `:accept`: Allowed are the same values like in [`PeptideController::search_by_post_request`] Accept-header, but urlsafe encoded
    ///     * `:payload`: The payload as urlsafe base64 encoded JSON string, see [`PeptideController::search_by_post_request`]
    /// * Method: `GET`
    ///
    ///
    pub async fn search_by_get_request(
        State(server_state): State<Arc<ServerState>>,
        Query(query): Query<SearchRequestQuery>,
        Path((payload, accept)): Path<(String, String)>,
    ) -> Result<(StatusCode, HeaderMap, Body), Error> {
        // Decode payload from URL saftyness
        let payload: String = match urldecode(payload.as_str()) {
            Ok(payload) => payload.into_owned(),
            Err(err) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                    Body::from(format!(
                        "!!! Error while decoding payload form URL: {:?}",
                        err
                    )),
                ));
            }
        };

        // Decode payload from base64
        let payload: Vec<u8> = match Base64Standard.decode(payload.as_bytes()) {
            Ok(payload) => payload,
            Err(err) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                    Body::from(format!(
                        "!!! Error while decoding payload from base64: {:?}",
                        err
                    )),
                ));
            }
        };

        // Create string from decoded bytes
        let payload = match String::from_utf8(payload) {
            Ok(payload) => payload,
            Err(err) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                    Body::from(format!(
                        "!!! Error while decoding payload from bytes: {:?}",
                        err
                    )),
                ));
            }
        };

        // Deserialize payload
        let payload: SearchRequestBody = match serde_json::from_str(payload.as_str()) {
            Ok(payload) => payload,
            Err(err) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                    Body::from(format!("!!! Error while deserializing payload: {:?}", err)),
                ));
            }
        };

        // Decode accept from URL saftyness
        let accept: String = match urldecode(accept.as_str()) {
            Ok(accept) => accept.into_owned(),
            Err(err) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                    Body::from(format!(
                        "!!! Error while decoding payload form URL: {:?}",
                        err
                    )),
                ));
            }
        };

        Self::search(server_state, payload, accept, query.is_download).await
    }

    async fn search(
        server_state: Arc<ServerState>,
        payload: SearchRequestBody,
        accept_header: String,
        is_download: bool,
    ) -> Result<(StatusCode, HeaderMap, Body), Error> {
        let mass = match payload.mass {
            SearchRequestMass::ThompsonCharge(mass, charge) => {
                mass_to_int!(mass_to_charge_to_dalton(mass, charge))
            }
            SearchRequestMass::Dalton(mass) => mass_to_int!(mass),
        };

        let mut taxonomy_ids: Option<Vec<i32>> = None;
        if let Some(taxonomy_id) = payload.taxonomy_id {
            // Check if taxonomy exists
            let matching_taxonomy_ids = TaxonomyTable::new(server_state.db_client())
                .select_sub_species(taxonomy_id)
                .await?
                .map(|taxonomy_result| taxonomy_result.map(|taxonomy| taxonomy.id()))
                .try_collect::<Vec<i32>>()
                .await?;

            if !matching_taxonomy_ids.is_empty() {
                taxonomy_ids = Some(matching_taxonomy_ids);
            } else {
                return Err(Error::TaxonomyNotFound(taxonomy_id));
            }
        }

        let modifications: Vec<PostTranslationalModification> = match payload
            .modifications
            .into_iter()
            .map(PostTranslationalModification::try_from)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(modifications) => modifications,
            Err(err) => {
                return Ok((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                    Body::from(format!("Error while parsing PTMs: {:?}", err)),
                ));
            }
        };

        let ptm_collection = match PTMCollection::new(modifications.into_iter().map(Arc::new)) {
            Ok(collection) => Arc::new(collection),
            Err(err) => {
                return Ok((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                    Body::from(format!("Error while validating PTMs: {:?}", err)),
                ));
            }
        };

        let mut selection: &'static PeptideColumnSelection = &FULL_PEPTIDE_COLUMN_SELECTION;
        let mut headers = HeaderMap::new();
        if is_download {
            let file_extension = match accept_header.as_str() {
                "application/json" => ".json",
                "text/tab-separated-values" => ".tsv",
                "text/plain" => ".txt",
                _ => "",
            };

            headers.insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(
                    format!(
                        "attachment; filename=\"macpepdb_peptides_download{}\"",
                        file_extension
                    )
                    .as_str(),
                )
                .unwrap(),
            );
        }

        match accept_header.as_str() {
            "application/json" => {
                headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json; charset=utf-8"),
                );
            }
            "text/plain" | "text/fasta" => {
                headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/plain; charset=utf-8"),
                );
                selection = &SEQUENCE_PEPTIDE_COLUMN_SELECTION;
            }
            _ => return Err(Error::InvalidAcceptHeader),
        }

        let peptide_search = PeptideSearch::new(
            server_state.db_client(),
            selection,
            server_state.configuration(),
            mass,
            payload.lower_mass_tolerance_ppm,
            payload.upper_mass_tolerance_ppm,
            payload.max_variable_modifications,
            true,
            taxonomy_ids,
            payload.is_reviewed,
            ptm_collection.clone(),
            payload.resolve_modifications.unwrap_or(false),
            server_state.concurrent_searches(),
        );

        let (status_code, headers, body) = match accept_header.as_str() {
            "application/json" => {
                let peptide_stream = peptide_search
                    .search::<PeptidoformToJsonTransformation>()
                    .await?;
                (
                    StatusCode::OK,
                    headers,
                    Body::from_stream(stream! {
                        let mut buffer = String::with_capacity(RESPONSE_STREAM_CHUNK_THRESHOLD + 4096);
                        // start json array
                        buffer.push('[');
                        // set delimiter to empty string for first element
                        let mut delimiter: &'static str = "";
                        // stream peptides
                        for await peptidoforms in peptide_stream {
                            match peptidoforms {
                                Ok(peptidoforms) => {
                                    for peptidoform in peptidoforms.into_iter() {
                                        buffer.push_str(delimiter);
                                        buffer.push_str(&peptidoform);
                                        delimiter = ",";
                                        if buffer.len() >= RESPONSE_STREAM_CHUNK_THRESHOLD {
                                            yield Ok(std::mem::replace(&mut buffer, String::with_capacity(RESPONSE_STREAM_CHUNK_THRESHOLD + 4096)));
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::error!("{:?}", err);
                                    if !buffer.is_empty() {
                                        yield Ok(std::mem::take(&mut buffer));
                                    }
                                    yield Err(format!("!!! {:?}", err));
                                    break;
                                }
                            };
                        }
                        // end json array
                        buffer.push(']');
                        yield Ok(buffer);
                    }),
                )
            }
            "text/plain" => {
                let peptide_stream = peptide_search
                    .search::<PeptidoformToPlainTextTransformation>()
                    .await?;
                (
                    StatusCode::OK,
                    headers,
                    Body::from_stream(stream! {
                        let mut buffer = String::with_capacity(RESPONSE_STREAM_CHUNK_THRESHOLD + 4096);
                        // guarantee at least one non-empty chunk even on zero hits, so the
                        // streamed body is never fully empty (empty h2c bodies can trip the
                        // client's decoder)
                        buffer.push('\n');
                        let mut delimiter: &'static str = "";
                        for await peptidoforms in peptide_stream {
                            match peptidoforms {
                                Ok(peptidoforms) => {
                                    for peptidoform in peptidoforms.into_iter() {
                                        buffer.push_str(delimiter);
                                        // sequence() is a &ModifiedSequence (Display only, no cheap &str/as_bytes for the general modified case)
                                        write!(buffer, "{}", peptidoform).unwrap();
                                        delimiter = "\n";
                                        if buffer.len() >= RESPONSE_STREAM_CHUNK_THRESHOLD {
                                            yield Ok(std::mem::replace(&mut buffer, String::with_capacity(RESPONSE_STREAM_CHUNK_THRESHOLD + 4096)));
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::error!("{:?}", err);
                                    if !buffer.is_empty() {
                                        yield Ok(std::mem::take(&mut buffer));
                                    }
                                    yield Err(format!("!!! {:?}", err));
                                    break;
                                }
                            };
                        }
                        if !buffer.is_empty() {
                            yield Ok(buffer);
                        }
                    }),
                )
            }
            "text/fasta" => {
                let peptide_stream = peptide_search
                    .search::<PeptidoformToPlainTextTransformation>()
                    .await?;
                (
                    StatusCode::OK,
                    headers,
                    Body::from_stream(stream! {
                        let mut buffer = String::with_capacity(RESPONSE_STREAM_CHUNK_THRESHOLD + 4096);
                        // guarantee at least one non-empty chunk even on zero hits, so the
                        // streamed body is never fully empty (empty h2c bodies can trip the
                        // client's decoder)
                        buffer.push('\n');
                        let mut peptidoform_ctr: usize = 0;
                        let mut delimiter: &'static str = "";
                        for await peptidoforms in peptide_stream {
                            match peptidoforms {
                                Ok(peptidoforms) => {
                                    for peptidoform in peptidoforms.into_iter() {
                                        buffer.push_str(delimiter);
                                        write!(
                                            buffer,
                                            ">mdb|{peptidoform_ctr}\n{}",
                                            peptidoform
                                        )
                                        .unwrap();
                                        peptidoform_ctr += 1;
                                        delimiter = "\n";
                                        if buffer.len() >= RESPONSE_STREAM_CHUNK_THRESHOLD {
                                            yield Ok(std::mem::replace(&mut buffer, String::with_capacity(RESPONSE_STREAM_CHUNK_THRESHOLD + 4096)));
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::error!("{:?}", err);
                                    if !buffer.is_empty() {
                                        yield Ok(std::mem::take(&mut buffer));
                                    }
                                    yield Err(format!("!!! {:?}", err));
                                    break;
                                }
                            };
                        }
                        if !buffer.is_empty() {
                            yield Ok(buffer);
                        }
                    }),
                )
            }
            _ => (
                StatusCode::NOT_ACCEPTABLE,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from("Unsupported accept header".to_string()),
            ),
        };
        Ok((status_code, headers, body))
    }
}
