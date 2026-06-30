use std::ops::Deref;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::response::Response;
use axum::routing::get;
use axum::{Json, response::IntoResponse};
use http::StatusCode;
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::amino_acid::AminoAcid;
use crate::mass::to_float as mass_to_float;
use crate::web::DEFAULT_ERROR_HEADER_MAP;
use crate::web::server_state::ServerState;

static CONTROLLER_PATH: &str = "/api/chemistry";
static SHOW_AMINO_ACID_PATH: &str = "/amino-acids/{code}";
static AMINO_ACIDS_PATH: &str = "/amino-acids";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Amino acid error: {0}")]
    AminoAcid(Box<crate::amino_acid::Error>),
    #[error("Amino acid serialization error: {0}")]
    AminoAcidSerializationError(Box<serde_json::Error>),
    #[error("Invalid amino acid code, needs to be exactly 1 character.")]
    InvalidAminoAcidCode,
    #[error("Unable to serialize amono acid mono mass: {0}")]
    MonoMassFloatSerialization(Box<serde_json::Error>),
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

pub struct ChemistryController;

impl ChemistryController {
    pub fn routes(state: Arc<ServerState>) -> Router<Arc<ServerState>> {
        let router: Router<Arc<ServerState>> = Router::new()
            .route(AMINO_ACIDS_PATH, get(Self::amino_acids))
            .route(SHOW_AMINO_ACID_PATH, get(Self::amino_acid));

        router.with_state(state)
    }

    pub fn controller_path() -> &'static str {
        CONTROLLER_PATH
    }

    /// Returns something which implements amino acid from `omicstools` crate to json
    ///
    /// # Arguments
    /// * `amino_acid` - Amino acid
    ///
    fn amino_acid_to_json(amino_acid: &AminoAcid) -> Result<JsonValue, Error> {
        let mut amino_acid_value = serde_json::to_value(amino_acid)
            .map_err(|err| Error::AminoAcidSerializationError(Box::new(err)))?;
        amino_acid_value["mono_mass"] = serde_json::to_value(mass_to_float(amino_acid.mono_mass()))
            .map_err(|err| Error::MonoMassFloatSerialization(Box::new(err)))?;
        Ok(amino_acid_value)
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
    pub async fn amino_acid(Path(code): Path<String>) -> Result<Json<JsonValue>, Error> {
        if code.len() != 1 {
            return Err(Error::InvalidAminoAcidCode);
        }

        let amino_acid = AminoAcid::by_code(code.chars().next().unwrap())?;

        Ok(Json(Self::amino_acid_to_json(amino_acid)?))
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
    pub async fn amino_acids() -> Result<Json<JsonValue>, Error> {
        let amino_acid_values = AminoAcid::all()
            .iter()
            .map(|aa| Self::amino_acid_to_json(aa))
            .collect::<Result<Vec<JsonValue>, Error>>()?;

        Ok(Json(serde_json::Value::Array(amino_acid_values)))
    }
}
