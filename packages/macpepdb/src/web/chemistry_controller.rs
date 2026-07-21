use std::ops::Deref;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::response::Response;
use axum::routing::get;
use axum::{Json, response::IntoResponse};
use http::StatusCode;
use macpepdb_web_common::responses::amino_acid::AminoAcidResponse;
use thiserror::Error;

use crate::amino_acid::AminoAcid;
use crate::web::DEFAULT_ERROR_HEADER_MAP;
use crate::web::server_state::ServerState;

static CONTROLLER_PATH: &str = "/api/chemistry";
static SHOW_AMINO_ACID_PATH: &str = "/amino-acids/{code}";
static AMINO_ACIDS_PATH: &str = "/amino-acids";
static HYDROPHOBICITY_KROKHIN_PATH: &str = "/hydrophobicity/krokhin/{sequence}";

/// Errors that can occur while handling chemistry endpoints.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Amino acid error: {0}")]
    AminoAcid(Box<crate::amino_acid::Error>),
    #[error("Invalid amino acid code, needs to be exactly 1 character.")]
    InvalidAminoAcidCode,
}

into_thiserror_boxed!(crate::amino_acid::Error, Error, AminoAcid);

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::AminoAcid(err) => (
                StatusCode::BAD_REQUEST,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(format!("{}", err)),
            )
                .into_response(),
            Error::InvalidAminoAcidCode => (
                StatusCode::BAD_REQUEST,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(format!("{}", self)),
            )
                .into_response(),
        }
    }
}

/// Controller providing chemistry related endpoints (amino acids, hydrophobicity) under `/api/chemistry`.
pub struct ChemistryController;

impl ChemistryController {
    /// Builds the axum router for the chemistry endpoints, mounted onto the given server state.
    pub fn routes(state: Arc<ServerState>) -> Router<Arc<ServerState>> {
        let router: Router<Arc<ServerState>> = Router::new()
            .route(AMINO_ACIDS_PATH, get(Self::amino_acids))
            .route(SHOW_AMINO_ACID_PATH, get(Self::amino_acid))
            .route(
                HYDROPHOBICITY_KROKHIN_PATH,
                get(Self::hydrophobicity_krokhin),
            );

        router.with_state(state)
    }

    /// Returns the base path this controller is mounted on (`/api/chemistry`).
    pub fn controller_path() -> &'static str {
        CONTROLLER_PATH
    }

    /// Gets the amino acid by one letter code
    ///
    /// # Arguments
    /// * `code` - Amino acid one letter code
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/chemistry/amino_acids/{code}`
    /// * Method: `GET`
    ///
    /// ## Response
    /// ```json
    /// {
    ///     "code": "G",
    ///     "is_canonical": true
    ///     "mono_mass": 57.021463735,
    ///     "name": "Glycine"
    /// }
    /// ```
    ///
    pub async fn amino_acid(Path(code): Path<String>) -> Result<Json<AminoAcidResponse>, Error> {
        if code.len() != 1 {
            return Err(Error::InvalidAminoAcidCode);
        }

        let amino_acid = AminoAcid::by_code(code.chars().next().unwrap())?;

        Ok(Json(amino_acid.into()))
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
    ///         "code": "G",
    ///         "is_canonical": true
    ///         "mono_mass": 57.021463735,
    ///         "name": "Glycine"
    ///     },
    ///    ...
    /// ]
    /// ```
    ///
    pub async fn amino_acids() -> Result<Json<Vec<AminoAcidResponse>>, Error> {
        let amino_acid_values = AminoAcid::all().iter().map(|aa| (*aa).into()).collect();

        Ok(Json(amino_acid_values))
    }

    /// Returns hydrophobicity score of a peptide sequence using methodb by Krokhin et al. (2006)
    ///
    /// # Arguments
    /// * `sequence` - Amino acid seqeunce
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/hydrophobicity/krokhin/{sequence}`
    /// * Method: `GET`
    ///
    /// ## Response
    /// ```json
    /// [
    ///     TODO
    /// ]
    /// ```
    ///
    pub async fn hydrophobicity_krokhin(Path(sequence): Path<String>) -> Result<Json<f64>, Error> {
        Ok(Json(macpepdb_peptide_hydrophobicity::krokhin::score_sequence(
            sequence.as_str(),
        )))
    }
}
