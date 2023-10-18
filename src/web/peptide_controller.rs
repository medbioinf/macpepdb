// std imports
use std::str::FromStr;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use async_stream::{stream, try_stream};
use axum::extract::{Json, Path, State};
use axum::http::header::ACCEPT;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum_streams::*;
use dihardts_cstools::bloom_filter::BloomFilter;
use dihardts_omicstools::chemistry::amino_acid::get_amino_acid_by_one_letter_code;
use dihardts_omicstools::proteomics::post_translational_modifications::{
    ModificationType as PtmType, Position as PtmPosition, PostTranslationalModification as PTM,
};
use futures::{pin_mut, Stream, StreamExt};
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
use crate::functions::post_translational_modification::{get_ptm_conditions, PTMCondition};
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

/// Returns a basic fallible stream over the filtered peptides
///
/// # Arguments
/// * `payload` - The request body
/// * `mass` - The mass to search for
/// * `db_client` - The database client
/// * `configuration` - MaCPepDB configuration
/// * `matching_peptides` - A bloom filter to check if a peptide was already found
/// * `ptm_condition` - Optional: PTM condition to check if a peptide is matches
///
async fn search_peptide_stream_mass<'a>(
    payload: &'a SearchRequestBody,
    mass: i64,
    db_client: Arc<Client>,
    configuration: Arc<Configuration>,
    matching_peptides: &'a mut BloomFilter,
    ptm_condition: Option<&'a PTMCondition>,
) -> Result<impl Stream<Item = Result<Peptide>> + 'a> {
    Ok(try_stream! {
        // Calculate mass range
        let lower_mass_limit = mass - (mass / 1000000 * payload.lower_mass_tolerance_ppm);
        let upper_mass_limit = mass + (mass / 1000000 * payload.upper_mass_tolerance_ppm);

        // Get partition
        let lower_partition_index =
            get_mass_partition(&configuration.get_partition_limits(), lower_mass_limit)?;
        let upper_partition_index =
            get_mass_partition(&configuration.get_partition_limits(), upper_mass_limit)?;

        // Convert to CqlValue
        let lower_mass_limit = CqlValue::BigInt(lower_mass_limit);
        let upper_mass_limit = CqlValue::BigInt(upper_mass_limit);

        for partition in lower_partition_index..=upper_partition_index {
            let partition = CqlValue::BigInt(partition as i64);

            let params = vec![&partition, &lower_mass_limit, &upper_mass_limit];

            let peptide_stream = PeptideTable::stream(
                db_client.as_ref(),
                "WHERE partition = ? AND mass >= ? AND mass <= ?",
                params.as_slice(),
                10000,
            )
            .await?;
            pin_mut!(peptide_stream);

            while let Some(peptide) = peptide_stream.next().await {
                let peptide = peptide?;
                // Fastest check first
                if matching_peptides.contains(peptide.get_sequence())? {
                    continue;
                }

                // Check PTM conditions
                if let Some(ptm_condition) =  ptm_condition {
                    if !ptm_condition.check_peptide(&peptide) {
                        continue;
                    }
                }

                if let Some(taxonomy_id) = payload.taxonomy_id {
                    if !peptide.get_taxonomy_ids().contains(&taxonomy_id) {
                        continue;
                    }
                }

                if let Some(proteome_id) = &payload.proteome_id {
                    if !peptide.get_proteome_ids().contains(proteome_id) {
                        continue;
                    }
                }

                if let Some(is_reviewed) = payload.is_reviewed {
                    if  is_reviewed && !peptide.get_is_swiss_prot()
                        || !is_reviewed && !peptide.get_is_trembl()
                    {
                        continue;
                    }
                }

                matching_peptides.add(&peptide.get_sequence())?;

                yield peptide;
            }
        }
    })
}

/// Returns a fallible stream over the filtered peptides.
/// (Combines stream for multiple PTM conditions)
///
/// # Arguments
/// * `payload` - The request body
/// * `db_client` - The database client
/// * `configuration` - MaCPepDB configuration
///
async fn search_peptide_stream<'a>(
    payload: SearchRequestBody,
    db_client: Arc<Client>,
    configuration: Arc<Configuration>,
) -> Result<impl Stream<Item = Result<Peptide>> + 'a> {
    Ok(try_stream! {
        let ptms = payload.get_modifications()?;
        let mut matching_peptides = BloomFilter::new_by_size_and_fp_prob(80_000_000, 0.001)?; // around 10MB

        if ptms.len() == 0 {
            for await peptide in search_peptide_stream_mass(&payload, mass_to_int(payload.mass), db_client.clone(), configuration.clone(), &mut matching_peptides, None).await? {
                yield peptide?;
            }
        } else {
            let ptm_conditions = get_ptm_conditions(
                mass_to_int(payload.mass),
                payload.max_variable_modifications,
                &ptms,
            )?;

            for ptm_condition in ptm_conditions.iter() {
                for await peptide in search_peptide_stream_mass(&payload, *ptm_condition.get_mass(), db_client.clone(), configuration.clone(), &mut matching_peptides, Some(&ptm_condition)).await? {
                    yield peptide?;
                }
            }
        }
    })
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

    let peptide_stream =
        match search_peptide_stream(payload, db_client.clone(), configuration.clone()).await {
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
