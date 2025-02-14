// std imports
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use async_stream::stream;
use axum::body::Body;
use axum::extract::{Json, Path, State};
use axum::http::header::ACCEPT;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use dihardts_omicstools::proteomics::proteases::functions::get_by_name as get_protease_by_name;
use futures::TryStreamExt;
use scylla::frame::response::result::CqlValue;
use tracing::error;

// internal imports
use crate::chemistry::amino_acid::calc_sequence_mass_int;
use crate::database::scylla::peptide_table::PeptideTable;
use crate::database::scylla::protein_table::ProteinTable;
use crate::entities::peptide::TsvPeptide;
use crate::entities::protein::Protein;
use crate::mass::convert::to_int as mass_to_int;
use crate::tools::peptide_partitioner::get_mass_partition;
use crate::web::app_state::AppState;
use crate::web::web_error::WebError;

/// Returns the peptide for given sequence.
/// Important: This endpoint will return the the peptide inclduing a list of full records of the proteins of origin. The proteins will include only the contained peptide sequences. Not the entire peptide records.
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
///          {
///             "accession": "Q924W6",
///             "domains": [],
///             "entry_name": "TRI66_MOUSE",
///             "genes": [
///                 ...
///             ],
///             "is_reviewed": true,
///             "name": "Tripartite motif-containing protein 66",
///             "peptides": [
///                 "MSPGLPVSIPSQPHCSTDERVEALAPTCSMCGRDLQAEGSR",
///                 ...
///             ],
///             "proteome_id": "UP000000589",
///             "secondary_accessions": [
///                 ...
///             ],
///             "sequence": "...",
///             "taxonomy_id": 10090,
///             "updated_at": 1687910400
///         },
///         ...
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
///     ]
/// }
///
pub async fn get_peptide(
    State(app_state): State<Arc<AppState>>,
    Path(sequence): Path<String>,
) -> Result<Json<serde_json::Value>, WebError> {
    let sequence = sequence.to_uppercase();
    let mass = calc_sequence_mass_int(sequence.as_str())?;
    let partition = get_mass_partition(
        app_state.get_configuration_as_ref().get_partition_limits(),
        mass,
    )?;

    let peptide_opt = PeptideTable::select(
        app_state.get_db_client_as_ref(),
        "WHERE partition = ? AND mass = ? and sequence = ?",
        &[
            &CqlValue::BigInt(partition as i64),
            &CqlValue::BigInt(mass),
            &CqlValue::Text(sequence),
        ],
    )
    .await?
    .try_collect::<Vec<_>>()
    .await?
    .pop();

    if peptide_opt.is_none() {
        return Err(WebError::new(
            StatusCode::NOT_FOUND,
            "Peptide not found".to_string(),
        ));
    }

    let protease = get_protease_by_name(
        app_state.get_configuration_as_ref().get_protease_name(),
        app_state
            .get_configuration_as_ref()
            .get_min_peptide_length(),
        app_state
            .get_configuration_as_ref()
            .get_max_peptide_length(),
        app_state
            .get_configuration_as_ref()
            .get_max_number_of_missed_cleavages(),
    )?;

    let peptide = peptide_opt.unwrap();

    let proteins: Vec<Protein> =
        ProteinTable::get_proteins_of_peptide(app_state.get_db_client_as_ref(), &peptide)
            .await?
            .try_collect()
            .await?;

    let protein_jsons = proteins
        .into_iter()
        .map(|protein| protein.to_json_with_peptide_sequences(protease.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    let mut peptide_json = match serde_json::to_value(peptide) {
        Ok(json) => json,
        Err(err) => {
            return Err(WebError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error while serializing peptide: {:?}", err),
            ))
        }
    };
    peptide_json["proteins"] = match serde_json::to_value(protein_jsons) {
        Ok(json) => json,
        Err(err) => {
            return Err(WebError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error while serializing proteins: {:?}", err),
            ))
        }
    };
    return Ok(Json(peptide_json));
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
    State(app_state): State<Arc<AppState>>,
    Path(sequence): Path<String>,
) -> Result<Response, WebError> {
    if PeptideTable::exists_by_sequence(
        app_state.get_db_client_as_ref(),
        sequence.as_str(),
        app_state.get_configuration_as_ref(),
    )
    .await?
    {
        Ok((StatusCode::OK, "").into_response())
    } else {
        Ok((StatusCode::NOT_FOUND, "").into_response())
    }
}

/// Simple struct to deserialize the request body for peptide search
///
#[derive(serde::Deserialize)]
pub struct SearchRequestBody {
    mass: f64,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: i16,
    modifications: Vec<PTM>,
    taxonomy_id: Option<i64>,
    proteome_id: Option<String>,
    is_reviewed: Option<bool>,
}

/// Returns a stream of peptides matching the given parameters.
/// If the taxonomy ID is given and has sub taxonomies, the sub taxonomies are also searched.
/// Important: Peptides only contain the accession of the proteins of origin.
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
///     * `Accept`: `application/json`, `text/tsv`, `text/plain` (optional, default: `application/json`, controls the output format)
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
///             {
///                 "name": "Mod something",
///                 "amino_acid": "C",
///                 "mass_delta": 42.0,
///                 "mod_type": Static,     # Type: Static, Variable
///                 "position": Anywhere    # Position: Anywhere, Terminus-N, Terminus-C, Bond-C, Bond-N
///             }
///         ],
///         # Optional taxonomy ID to search for
///         "taxonomy_id": 10090,
///         # Optional proteome ID to search for
///         "proteome_id": "UP000000589",
///         # Optional flag to search only reviewed proteins
///         "is_reviewed": true
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
/// ### `text/tsv`
/// ```tsv
/// partition	mass	sequence	missed_cleavages	aa_counts	proteins	is_swiss_prot	is_trembl	taxonomy_ids	unique_taxonomy_ids	proteome_ids
/// 51\t2006.988396539\tNLETPSCKNGFLLDGFPR\t1,0,0,1,1,1,2,2,0,0,0,1,3,0,2,0,2,0,1,1,1,0,0,0,0,0,0\tQ9WTP6\ttrue\tfalse\t10090\t10090\tUP000000589
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
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<SearchRequestBody>,
) -> Result<(StatusCode, Body), WebError> {
    let mut taxonomy_ids: Option<Vec<i64>> = None;
    if let Some(taxonomy_id) = payload.taxonomy_id {
        // Check if taxonomy exists
        if app_state
            .get_taxonomy_tree_as_ref()
            .get_taxonomy(taxonomy_id as u64)
            .is_none()
        {
            return Ok((
                StatusCode::BAD_REQUEST,
                Body::from(format!(
                    "!!! Taxonomy with id {} does not exist",
                    taxonomy_id
                )),
            ));
        }

        let mut ids: Vec<i64> = match app_state
            .get_taxonomy_tree_as_ref()
            .get_sub_taxonomies(taxonomy_id as u64)
        {
            Some(taxonomies) => taxonomies.iter().map(|tax| tax.get_id() as i64).collect(),
            None => Vec::new(),
        };
        ids.push(taxonomy_id);
        taxonomy_ids = Some(ids);
    }

    let proteome_ids = match payload.proteome_id {
        Some(proteome_id) => Some(vec![proteome_id]),
        None => None,
    };

    let peptide_stream = match PeptideTable::search(
        app_state.get_db_client(),
        app_state.get_configuration(),
        mass_to_int(payload.mass),
        payload.lower_mass_tolerance_ppm.clone(),
        payload.upper_mass_tolerance_ppm.clone(),
        payload.max_variable_modifications.clone(),
        taxonomy_ids,
        proteome_ids,
        payload.is_reviewed.clone(),
        payload.modifications,
    )
    .await
    {
        Ok(peptide_stream) => peptide_stream,
        Err(err) => {
            return Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                Body::from(format!("Error while searching for peptides: {:?}", err)),
            ));
        }
    };

    let default_header = match HeaderValue::from_str("application/json") {
        Ok(header) => header,
        Err(err) => {
            return Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                Body::from(format!("!!! Error while setting default header: {:?}", err)),
            ));
        }
    };

    let accept_header = headers.get(ACCEPT).unwrap_or(&default_header);

    let (status_code, body) = match accept_header.to_str().unwrap() {
        "application/json" => (
            StatusCode::OK,
            Body::from_stream(stream! {
                // start json array
                yield Ok("[".to_string());
                // set delimiter to empty string for first element
                let mut delimiter = "".to_string();
                // stream peptides
                for await peptide in peptide_stream {
                    yield Ok(delimiter.to_owned());
                    // handle error on underlaying stream
                    if let Err(err) = peptide {
                        error!("{:?}", err);
                        yield Err(format!("!!! {:?}", err));
                        break;
                    }
                    let peptide = peptide.unwrap();
                    match serde_json::to_string(&peptide) {
                        Ok(json) => yield Ok(json),
                        Err(err) => {
                            error!("{:?}", err);
                            yield Err(format!("!!! {:?}", err));
                            break;
                        }
                    };
                    // set delimiter to comma after first element
                    delimiter = ",".to_string();
                }
                // end json array
                yield Ok("]".to_string());
            }),
        ),
        "text/csv" => (
            StatusCode::OK,
            Body::from_stream(stream! {
                let mut has_headers = true;
                for await peptide in peptide_stream {
                    // handle error on underlaying stream
                    if let Err(err) = peptide {
                        error!("{:?}", err);
                        yield Err(format!("!!! {:?}", err));
                        break;
                    }
                    let peptide = TsvPeptide::from(peptide.unwrap());
                    let mut writer = csv::WriterBuilder::new().has_headers(has_headers).delimiter(b'\t').from_writer(vec![]);
                    match writer.serialize(peptide) {
                        Ok(_) => (),
                        Err(err) => {
                            error!("{:?}", err);
                            yield Err(format!("!!! {:?}", err));
                            break;
                        }
                    };
                    match writer.into_inner() {
                        Ok(csv) => yield Ok(csv),
                        Err(err) => {
                            error!("{:?}", err);
                            yield Err(format!("!!! {:?}", err));
                            break;
                        }
                    };
                    has_headers = false;
                }
                yield Ok(vec![b'\n']);
            }),
        ),
        "text/plain" => (
            StatusCode::OK,
            Body::from_stream(stream! {
                let mut delimiter = "".to_string();
                for await peptide in peptide_stream {
                    yield Ok(delimiter.to_owned());
                    yield match peptide {
                        Ok(peptide) => Ok(peptide.get_sequence().to_owned()),
                        Err(err) => Err(format!("!!! {:?}", err)),
                    };
                    delimiter = "\n".to_string();
                }
                yield Ok(delimiter);
            }),
        ),
        _ => (
            StatusCode::NOT_ACCEPTABLE,
            Body::from_stream(stream! {
                yield Err::<String, String>("!!! Unsupported accept header".to_string());
            }),
        ),
    };
    Ok((status_code, body))
}
