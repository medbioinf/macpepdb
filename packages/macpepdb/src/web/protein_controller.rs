use std::ops::Deref;
use std::sync::Arc;

use async_stream::stream;
use axum::Router;
use axum::body::Body;
use axum::extract::{Json, Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use fallible_iterator::FallibleIterator;
use futures::TryStreamExt;
use http::{HeaderMap, StatusCode};
use macpepdb_web_common::responses::peptide::PeptideResponse;
use macpepdb_web_common::responses::protein::ProteinResponse;
use postgres_types::ToSql;
use thiserror::Error;

use crate::peptide::{IsPeptide, Peptide};
use crate::peptide_table::PeptideTable;
use crate::protease::Protease;
use crate::protein::Protein;
use crate::protein_table::ProteinTable;
use crate::sequence::IsBitSequence;
use crate::web::DEFAULT_ERROR_HEADER_MAP;
use crate::web::server_state::ServerState;

static CONTROLLER_PATH: &str = "/api/proteins";

static SEARCH_PATH: &str = "/search/{attribute}";
static PROTEIN_PATH: &str = "/{accession}";
static ID_RESOLVE_PATH: &str = "/resolve-ids";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Peptide table error: {0}")]
    PeptideTable(Box<crate::peptide_table::Error>),
    #[error("Protease error: {0}")]
    Protease(Box<crate::protease::Error>),
    #[error("Protien error: {0}")]
    Protein(Box<crate::protein::Error>),
    #[error("Protein not found")]
    ProteinNotFound,
    #[error("Protein table error: {0}")]
    ProteinTable(Box<crate::protein_table::Error>),
    #[error("Search term too short. Minimum length is 3 characters.")]
    SearchTermTooShort,
}

into_thiserror_boxed!(crate::peptide_table::Error, Error, PeptideTable);
into_thiserror_boxed!(crate::protease::Error, Error, Protease);
into_thiserror_boxed!(crate::protein::Error, Error, Protein);
into_thiserror_boxed!(crate::protein_table::Error, Error, ProteinTable);

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::Protein(err) => (
                StatusCode::BAD_REQUEST,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(format!("{}", err)),
            )
                .into_response(),
            Error::ProteinNotFound => (
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

pub struct ProteinController;

impl ProteinController {
    pub fn routes(state: Arc<ServerState>) -> Router<Arc<ServerState>> {
        let router: Router<Arc<ServerState>> = Router::new()
            .route(SEARCH_PATH, get(Self::search))
            .route(PROTEIN_PATH, get(Self::show))
            .route(ID_RESOLVE_PATH, post(Self::resolve_ids));

        router.with_state(state)
    }

    pub fn controller_path() -> &'static str {
        CONTROLLER_PATH
    }

    /// Builds the response for this protein including its peptides.
    /// As the peptides are not stored in the same record, the protein sequence needs to be digested
    /// using the given protease.
    ///
    /// # Arguments
    /// * `protein` - The protein to build the response for
    /// * `state` - Server state
    ///
    pub async fn to_response(
        protein: &Protein,
        state: &ServerState,
    ) -> Result<ProteinResponse<PeptideResponse>, Error> {
        let peptides = state
            .configuration()
            .protease()
            .cleave(protein.sequence().data(), None)
            .collect::<Vec<Peptide>>()?;

        let peptide_len = peptides.len();

        let mut params: Vec<Box<dyn ToSql + Send + Sync>> = Vec::with_capacity(peptides.len() * 3);
        peptides.into_iter().for_each(|peptide| {
            let partitions: Vec<i64> = state
                .configuration()
                .mass_partitioning()
                .partition_by_mass(peptide.mass())
                .map(|(_, partition)| partition)
                .collect();

            params.push(Box::new(partitions));
            params.push(Box::new(peptide.mass()));
            params.push(Box::new(peptide.into_sequence()));
        });

        let where_clause = (0..peptide_len)
            .map(|i| {
                format!(
                    "(partition = ANY(${}) AND mass = ${} AND sequence = ${})",
                    i * 3 + 1,
                    i * 3 + 2,
                    i * 3 + 3
                )
            })
            .collect::<Vec<String>>()
            .join(" OR ");

        let where_clause = format!("WHERE {}", where_clause);

        let mut peptides = PeptideTable::new(state.db_client())
            .select(&where_clause, params)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        peptides.sort_by(|pep_x, pep_y| {
            pep_x
                .mass()
                .partial_cmp(&pep_y.mass())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let peptides: Vec<PeptideResponse> = peptides.iter().map(PeptideResponse::from).collect();

        Ok(protein.to_response(peptides))
    }

    /// Builds the response for this protein including just its peptide sequences.
    pub fn to_summary_response(
        protein: Protein,
        protease: &Protease,
    ) -> Result<ProteinResponse<String>, Error> {
        let mut peptides: Vec<Peptide> = protease
            .cleave(protein.sequence().data(), None)
            .collect()
            .map_err(Error::from)?;

        peptides.sort_by(|pep_x, pep_y| {
            pep_x
                .mass()
                .partial_cmp(&pep_y.mass())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let sequences = peptides
            .into_iter()
            .map(|pep| pep.into_sequence().to_string())
            .collect::<Vec<String>>();

        Ok(protein.to_summary_response(sequences))
    }

    /// Returns the protein for given accession.
    /// Important: This endpoint will return the the protein including a list of full records of the contained peptides. The peptides will contain just their protein IDs (see [PeptideResponse]), not the entire protein record.
    ///
    /// # Arguments
    /// * `db_client` - The database client
    /// * `configuration` - MaCPepDB configuration
    /// * `accession` - Protein accession extracted from URL path
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/proteins/{accession}`
    /// * Method: `GET`
    ///
    /// ## Response
    /// ```json
    /// {
    ///     "accession": "Q9WTP6",
    ///     "id": 1,
    ///     "genes": [
    ///         "Ak2"
    ///     ],
    ///     "is_reviewed": true,
    ///     "peptides": [
    ///         {
    ///             "partition": 1,
    ///             "mass": 587.375495125,
    ///             "sequence": "ALKTR",
    ///             "protein_ids": [1],
    ///             "unique_taxonomy_ids": [10090],
    ///             "non_unique_taxonomy_ids": [10090],
    ///             "is_swiss_prot": true,
    ///             "is_trembl": false
    ///         },
    ///         ...
    ///     ],
    ///     "sequence": "MAPNVLASEPEIPKGIRAVLLGPPG...DLVMFI",
    ///     "taxonomy_id": 10090
    /// }
    /// ```
    ///
    pub async fn show(
        State(state): State<Arc<ServerState>>,
        Path(accession): Path<String>,
    ) -> Result<Json<ProteinResponse<PeptideResponse>>, Error> {
        let protein_opt = ProteinTable::new(state.db_client())
            .select(
                "WHERE accession = $1 LIMIT 1",
                vec![Box::new(accession.to_uppercase())],
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .pop();

        let protein = protein_opt.ok_or(Error::ProteinNotFound)?;

        Ok(Json(Self::to_response(&protein, state.as_ref()).await?))
    }

    /// Fuzzy search protein for given (partial) accession or (partial) gene name.
    ///
    /// # Arguments
    /// * `db_client` - The database client
    /// * `configuration` - MaCPepDB configuration
    /// * `attribute` - Accession or gene name, will be wrapped in a like-query
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/proteins/search/{attribute}`
    /// * Method: `GET`
    ///
    /// ## Response
    /// ```json
    /// [
    ///     {
    ///         "accession": "Q9WTP6",
    ///         "id": 1,
    ///         "genes": [
    ///             "Ak2"
    ///         ],
    ///         "is_reviewed": true,
    ///         "peptides": [
    ///             "SYHEEFNPPK",
    ///             ...,
    ///             "KLKATMDAGK"
    ///         ],
    ///         "sequence": "MAPNVLASEPEIPKGIRAVLLGPPG...DLVMFI",
    ///         "taxonomy_id": 10090
    ///     },
    ///    ...
    /// ]
    /// ```
    ///
    pub async fn search(
        State(state): State<Arc<ServerState>>,
        Path(attribute): Path<String>,
    ) -> Result<(StatusCode, HeaderMap, Body), Error> {
        if attribute.len() < 3 {
            return Err(Error::SearchTermTooShort);
        }
        let protein_stream = ProteinTable::new(state.db_client())
            .search(Some(&attribute), Some(&attribute))
            .await?;

        let mut headers = HeaderMap::with_capacity(1);
        headers.insert("Content-Type", "application/json".parse().unwrap());

        Ok((
            StatusCode::OK,
            headers,
            Body::from_stream(stream! {
                // start json array
                yield Ok("[".to_string());
                // set delimiter to empty string for first element
                let mut delimiter = "".to_string();
                // stream peptides
                for await protein in protein_stream {
                    match protein {
                        Ok(protein) => {
                            yield Ok(delimiter.to_owned());
                            match Self::to_summary_response(protein, state.configuration().protease()) {
                                Ok(response) => match serde_json::to_string(&response) {
                                    Ok(json) => yield Ok(json),
                                    Err(err) => {
                                        tracing::error!("{:?}", err);
                                        yield Err(format!("!!! {:?}", err));
                                        break;
                                    }
                                },
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
            }),
        ))
    }

    pub async fn resolve_ids(
        State(state): State<Arc<ServerState>>,
        Json(ids): Json<Vec<i32>>,
    ) -> Result<Json<Vec<(i32, String)>>, Error> {
        let accessions = ProteinTable::new(state.db_client())
            .resolve_ids(ids)
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Json(accessions))
    }
}
