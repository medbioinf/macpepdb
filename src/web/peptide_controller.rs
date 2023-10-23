// std imports
use std::str::FromStr;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use async_stream::stream;
use axum::extract::{Json, Path, State};
use axum::http::header::ACCEPT;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum_streams::*;
use dihardts_omicstools::chemistry::amino_acid::get_amino_acid_by_one_letter_code;
use dihardts_omicstools::proteomics::post_translational_modifications::{
    ModificationType as PtmType, Position as PtmPosition, PostTranslationalModification as PTM,
};
use scylla::frame::response::result::CqlValue;
use serde::Deserialize;
use tracing::error;

// internal imports
use crate::chemistry::amino_acid::calc_sequence_mass;
use crate::database::scylla::client::Client;
use crate::database::scylla::peptide_table::PeptideTable;
use crate::database::selectable_table::SelectableTable;
use crate::entities::configuration::Configuration;
use crate::entities::peptide::Peptide;
use crate::mass::convert::to_int as mass_to_int;
use crate::tools::peptide_partitioner::get_mass_partition;
use crate::web::web_error::WebError;

/// Returns the peptide for given sequence.
///
/// # Arguments
/// * `db_client` - The database client
/// * `configuration` - MaCPepDB configuration
/// * `accession` - Protein accession extracted from URL path
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
///     "mass": 1015475679562,
///     "sequence": "HMENEKTK",
///     "missed_cleavages": 1,
///     # Amino acid counts, the amino acid at index 0 is A, at index 1 is B, ...
///     "aa_counts": [
///         0, 0, 0, 0, 2, 0, 0, 1, 0, 0, 2, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0
///     ],
///     "proteins": [
///         "Q924W6"
///     ],
///     "is_swiss_prot": true,
///     "is_trembl": false,
///     "taxonomy_ids": [
///         10090
///     ],
///     "unique_taxonomy_ids": [
///         10090
///     ],
///     "proteome_ids": [
///         "UP000000589"
///     ],
///     "domains": []
/// }
///
pub async fn get_peptide(
    State((db_client, configuration)): State<(Arc<Client>, Arc<Configuration>)>,
    Path(sequence): Path<String>,
) -> Result<Json<Peptide>, WebError> {
    let sequence = sequence.to_uppercase();
    let mass = calc_sequence_mass(sequence.as_str())?;
    let partition = get_mass_partition(configuration.get_partition_limits(), mass)?;

    let peptide_opt = PeptideTable::select(
        db_client.as_ref(),
        "WHERE partition = ? AND mass = ? and sequence = ?",
        &[
            &CqlValue::BigInt(partition as i64),
            &CqlValue::BigInt(mass),
            &CqlValue::Text(sequence),
        ],
    )
    .await?;

    if let Some(peptide) = peptide_opt {
        return Ok(Json(peptide));
    } else {
        return Err(WebError::new(
            StatusCode::NOT_FOUND,
            "Peptide not found".to_string(),
        ));
    }
}

/// Returns if a peptide exists.
///
/// # Arguments
/// * `db_client` - The database client
/// * `configuration` - MaCPepDB configuration
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
pub async fn get_peptide_existence(
    State((db_client, configuration)): State<(Arc<Client>, Arc<Configuration>)>,
    Path(sequence): Path<String>,
) -> Result<Response, WebError> {
    let sequence = sequence.to_uppercase();
    let mass = calc_sequence_mass(sequence.as_str())?;
    let partition = get_mass_partition(configuration.get_partition_limits(), mass)?;

    let peptide_opt = PeptideTable::select(
        db_client.as_ref(),
        "WHERE partition = ? AND mass = ? and sequence = ?",
        &[
            &CqlValue::BigInt(partition as i64),
            &CqlValue::BigInt(mass),
            &CqlValue::Text(sequence),
        ],
    )
    .await?;

    if peptide_opt.is_some() {
        Ok((StatusCode::OK, "").into_response())
    } else {
        Ok((StatusCode::NOT_FOUND, "").into_response())
    }
}

/// Simple struct to deserialize the request body for peptide search
///
#[derive(Deserialize)]
pub struct SearchRequestBody {
    mass: f64,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: i16,
    modifications: Vec<(char, f64, String, String)>,
    taxonomy_id: Option<i64>,
    proteome_id: Option<String>,
    is_reviewed: Option<bool>,
}

impl SearchRequestBody {
    /// Convert the modifications from the request body to a vector of PTMs
    ///
    pub fn get_modifications(&self) -> Result<Vec<PTM>> {
        let mut psms: Vec<PTM> = Vec::new();
        for (ptm_idx, ptm_tuple) in self.modifications.iter().enumerate() {
            let ptm = PTM::new(
                format!("ptm_{}", ptm_idx).as_str(),
                get_amino_acid_by_one_letter_code(ptm_tuple.0)?,
                ptm_tuple.1,
                PtmType::from_str(ptm_tuple.2.as_str())?,
                PtmPosition::from_str(ptm_tuple.3.as_str())?,
            );
            psms.push(ptm);
        }
        Ok(psms)
    }
}

/// Returns a stream of peptides matching the given parameters.
///
/// # Arguments
/// * `db_client` - The database client
/// * `configuration` - The configuration
/// * `payload` - The request body
///
/// # API
/// ## Request
/// * Path: `/api/peptides/search`
/// * Method: `POST`
/// * Headers:
///     * `Content-Type`: `application/json`
///     * `Accept`: `application/json`, `text/csv`, `text/plain` (optional, default: `application/json`, controls the output format)
/// * Body:
///     ```json
///     {
///         # Mass to search for
///         "mass": 2006.988396539,
///         # Lower mass tolerance in ppm
///         "lower_mass_tolerance_ppm": 5,
///         # Upper mass tolerance in ppm
///         "upper_mass_tolerance_ppm": 5,
///         # Optional parameters for digestion, if one of them is skipped
///         "max_variable_modifications": 3,
///         # List of post translational modifications
///         "modifications": [
///             [
///                 "C",        # Amino acid one letter code
///                 57.021464,  # Mass shift
///                 "static",   # Type: static, variable
///                 "anywhere"  # Position: anywhere, n, c
///             ]
///         ]
///     }
///     ```
///     Deserialized into [SearchRequestBody](SearchRequestBody)
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
/// Peptides are formatted as mentioned in the [`get_peptide`-endpoint](get_peptide).
///
/// ### `text/csv`
/// Due to serde limitations, the CSV does not contain a header.
/// The columns are:
/// * `partition`
/// * `mass`
/// * `sequence`
/// * `amino_acid_count_a`
/// * `amino_acid_count_b`
/// * ...
/// * `amino_acid_count_z`
/// * `proteins`
/// * `is_swiss_prot`
/// * `is_trembl`
/// * `taxonomy_ids`
/// * `unique_taxonomy_ids`
/// * `proteome_ids`
///
/// ```csv
/// 51,2006988396539,NLETPSCKNGFLLDGFPR,1,0,0,1,1,1,2,2,0,0,0,1,3,0,2,0,2,0,1,1,1,0,0,0,0,0,0,Q9WTP6,true,false,10090,10090,UP000000589
/// ...
/// ```
///
/// ### `text/plain`
/// ```text
/// sequence_1
/// sequence_2
/// ...
/// ```
///
pub async fn search(
    State((db_client, configuration)): State<(Arc<Client>, Arc<Configuration>)>,
    headers: HeaderMap,
    Json(payload): Json<SearchRequestBody>,
) -> impl IntoResponse {
    // Need to handle WebError manually, because we need to return a stream
    let ptms = match payload.get_modifications() {
        Ok(ptms) => ptms,
        Err(err) => {
            return StreamBodyAs::text(WebError::new_string_stream(
                StatusCode::BAD_REQUEST,
                format!("Error while parsing modifications: {:?}", err),
            ));
        }
    };

    let peptide_stream = match PeptideTable::search(
        db_client.clone(),
        configuration.clone(),
        mass_to_int(payload.mass),
        payload.lower_mass_tolerance_ppm.clone(),
        payload.upper_mass_tolerance_ppm.clone(),
        payload.max_variable_modifications.clone(),
        payload.taxonomy_id.clone(),
        payload.proteome_id.clone(),
        payload.is_reviewed.clone(),
        ptms,
    )
    .await
    {
        Ok(peptide_stream) => peptide_stream,
        Err(err) => {
            return StreamBodyAs::text(WebError::new_string_stream(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error while searching for peptides: {:?}", err),
            ));
        }
    };

    let default_header = match HeaderValue::from_str("application/json") {
        Ok(header) => header,
        Err(err) => {
            return StreamBodyAs::text(WebError::new_string_stream(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error while setting default header: {:?}", err),
            ));
        }
    };

    let accept_header = headers.get(ACCEPT).unwrap_or(&default_header);

    match accept_header.to_str().unwrap() {
        "application/json" => StreamBodyAs::json_array(stream! {
            for await peptide_res in peptide_stream {
                match peptide_res {
                    Ok(peptide) => yield peptide,
                    Err(err) => error!("{:?}", err)
                }
            }
        }),
        "text/csv" => StreamBodyAs::csv(stream! {
            for await peptide_res in peptide_stream {
                match peptide_res {
                    Ok(peptide) => yield peptide,
                    Err(err) => error!("{:?}", err)
                }
            }
        }),
        "text/plain" => StreamBodyAs::text(stream! {
            for await peptide_res in peptide_stream {
                match peptide_res {
                    Ok(peptide) => yield peptide.to_string(),
                    Err(err) => error!("{:?}", err)
                }
                yield "\n".to_string();
            }
        }),
        _ => StreamBodyAs::text(WebError::new_string_stream(
            StatusCode::BAD_REQUEST,
            "Unknown format requested".to_string(),
        )),
    }
}
