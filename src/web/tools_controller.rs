use std::collections::HashSet;
// std imports
use std::sync::Arc;

// 3rd party imports
use axum::extract::State;
use axum::Json;
use scylla::frame::response::result::CqlValue;
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};

// internal imports
use crate::biology::digestion_enzyme::functions::{
    create_peptides_entities_from_digest, get_enzyme_by_name,
};
use crate::database::scylla::client::Client;
use crate::database::scylla::peptide_table::PeptideTable;
use crate::database::selectable_table::SelectableTable;
use crate::entities::configuration::Configuration;
use crate::entities::peptide::Peptide;
use crate::web::web_error::WebError;

/// Request body for the digest endpoint
///
/// # API
/// ## Request
/// ```json
/// {
///     "sequence": "PEPTIDER",
///     # Optional parameters
///     "db_match": false,
///     "digestion_enzyme": "trypsin",
///     "max_number_of_missed_cleavages": "2"
///     "min_peptide_length": "6",
///     "max_peptide_length": "50",
/// }
/// ```
///
#[derive(Deserialize)]
pub struct DigestionRequestBody {
    /// Sequence to digest
    pub sequence: String,
    /// If true the resulting peptides will be matched against the database
    /// And the result will include a list `db_peptides` with all of the peptides which are in the database
    #[serde(default = "bool::default")]
    pub db_match: bool,
    /// The enzyme to use for digestion, default the enzyme specified during MaCPepDB creation is used
    pub digestion_enzyme: Option<String>,
    /// The `max_number_of_missed_cleavages` to use for digestion, default the enzyme specified during MaCPepDB creation is used
    pub max_number_of_missed_cleavages: Option<usize>,
    /// The `min_peptide_length` to use for digestion, default the enzyme specified during MaCPepDB creation is used
    pub min_peptide_length: Option<usize>,
    /// The `max_peptide_length` to use for digestion, default the enzyme specified during MaCPepDB creation is used
    pub max_peptide_length: Option<usize>,
}

/// Digests a sequence with the optionally specified enzyme and returns the peptides.
/// See: [DigestionRequestBody](DigestionRequestBody) for parameter details
///
/// # Arguments
/// * `db_client` - The database client
/// * `configuration` - The configuration
/// * `payload` - The request body
///
/// # API
/// ## Request
/// * Path: `/api/tools/digest`
/// * Method: `POST`
/// * Headers:
///     * `Content-Type`: `application/json`
/// * Body: see [DigestionRequestBody](DigestionRequestBody)
///
/// ## Response
/// Peptides are formatted as mentioned in the [`get_peptide`-endpoint](crate::web::peptide_controller::get_peptide).
/// ```
/// {
///     "peptides": [
///         peptide_1,
///         peptide_2,
///        ...
///     ],
///     "db_peptides": [
///        peptide_1,
///        peptide_2,
///       ...
///    ]
/// }
/// ```
///
pub async fn digest(
    State((db_client, configuration)): State<(Arc<Client>, Arc<Configuration>)>,
    Json(payload): Json<DigestionRequestBody>,
) -> Result<Json<JsonValue>, WebError> {
    let enzyme = get_enzyme_by_name(
        &payload
            .digestion_enzyme
            .unwrap_or(configuration.get_enzyme_name().to_owned()),
        payload
            .max_number_of_missed_cleavages
            .unwrap_or(configuration.get_max_number_of_missed_cleavages()),
        payload
            .min_peptide_length
            .unwrap_or(configuration.get_min_peptide_length()),
        payload
            .max_peptide_length
            .unwrap_or(configuration.get_max_peptide_length()),
    )?;

    let peptides: HashSet<Peptide> = create_peptides_entities_from_digest(
        &enzyme.digest(&payload.sequence),
        configuration.get_partition_limits(),
        None,
    )?;

    if payload.db_match {
        let mut select_params_by_partition: Vec<Vec<(CqlValue, CqlValue)>> =
            vec![Vec::new(); configuration.get_partition_limits().len()];

        for peptide in peptides.iter() {
            select_params_by_partition[peptide.get_partition() as usize].push((
                CqlValue::BigInt(peptide.get_mass()),
                CqlValue::Text(peptide.get_sequence().to_owned()),
            ));
        }

        let mut db_peptides: Vec<Peptide> = Vec::with_capacity(peptides.len());

        for (partition, select_params) in select_params_by_partition.iter().enumerate() {
            if select_params.is_empty() {
                continue;
            }

            let mut statement_addition = "WHERE partition = ? AND (mass, sequence) IN (".to_owned();

            statement_addition.push_str(
                &(0..select_params.len())
                    .map(|_| "(?, ?)".to_owned())
                    .collect::<Vec<String>>()
                    .join(", "),
            );

            statement_addition.push_str(")");

            let partition = CqlValue::BigInt(partition as i64);

            let mut select_params_ref: Vec<&CqlValue> =
                Vec::with_capacity(select_params.len() * 2 + 1);
            select_params_ref.push(&partition);
            select_params_ref.extend(
                select_params
                    .iter()
                    .map(|params| vec![&params.0, &params.1])
                    .flatten(),
            );

            let db_peptides_partition = PeptideTable::select_multiple(
                db_client.as_ref(),
                &statement_addition,
                select_params_ref.as_slice(),
            )
            .await?;

            db_peptides.extend(db_peptides_partition);
        }

        Ok(Json(json!({
            "peptides": peptides,
            "db_peptides": db_peptides,
        })))
    } else {
        Ok(Json(json!({
            "peptides": peptides,
        })))
    }
}
