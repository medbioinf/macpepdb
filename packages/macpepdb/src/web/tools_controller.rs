use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

use crate::mass::{dalton_to_mass_to_charge, to_float};
use crate::peptide::{IsPeptide, Peptidoform};
use crate::peptide_search::{PeptideConditionBuilder, PeptideSearch};
use crate::post_translational_modification::{PTMCollection, PostTranslationalModification};
use crate::protein_table::ProteinTable;
use crate::taxonomy_table::TaxonomyTable;
use crate::web::DEFAULT_ERROR_HEADER_MAP;
use crate::web::protein_controller::ProteinController;
use crate::web::server_state::ServerState;
use axum::Router;
use axum::body::Body;
use axum::extract::{Json, State};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use futures::{StreamExt, TryStreamExt};
use http::StatusCode;
use macpepdb_web_common::requests::tools::SrmPrmRequest;
use macpepdb_web_common::responses::tools::{SrmPrmResponse, SrmPrmTarget};
use thiserror::Error;

static CONTROLLER_PATH: &str = "/api/tools";

static PRM_SRM_ASSAY: &str = "/prm-srm";

/// Errors that can occur while handling tool endpoints.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Error occured whiel using Koina/IM2Deep for ion mobility prediciton : {0}")]
    IonMobilityPrediction(#[from] crate::koina::Error),
    #[error("Taxonomy with ID `{0}` not found. Are you sure it exists in NCBI?")]
    TaxonomyNotFound(i32),
    #[error("Taxonomy table error: {0}")]
    TaxonomyTable(#[from] crate::taxonomy_table::Error),
    #[error("Protein with accession `{0}` not found.")]
    ProteinNotFound(String),
    #[error("Protein table error: {0}")]
    ProteinTable(#[from] crate::protein_table::Error),
    #[error("Protein digestion error: {0}")]
    ProteinDigestion(#[from] crate::web::protein_controller::Error),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::IonMobilityPrediction(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(format!("Error while predicting ion mobility: {err}")),
            )
                .into_response(),
            Error::TaxonomyNotFound(id) => (
                StatusCode::NOT_FOUND,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(format!(
                    "Taxonomy with ID `{id}` not found. Are you sure it exists in NCBI?"
                )),
            )
                .into_response(),
            Error::ProteinNotFound(accession) => (
                StatusCode::NOT_FOUND,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(format!("Protein with accession `{accession}` not found.")),
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

/// Parses a charge specification: a single integer (`"2"`), a comma-separated list
/// (`"2,3,4"`), or an inclusive range (`"2-4"`). Returns the sorted, deduplicated charges.
///
/// # Arguments
/// * `spec` - charge spec as single integer (`"2"`), a comma-separated list (`"2,3,4"`) or an inclusive range (`"2-4"`)
///
fn parse_charge_spec(spec: &str) -> Result<Vec<u8>, String> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err("charge spec must not be empty".to_string());
    }

    let mut charges: Vec<u8> = Vec::new();

    if let Some((lower, upper)) = spec.split_once('-') {
        let lower_trimmed = lower.trim();
        let upper_trimmed = upper.trim();
        let lower: u8 = lower_trimmed
            .parse()
            .map_err(|_| format!("invalid range lower bound `{lower_trimmed}`"))?;
        let upper: u8 = upper_trimmed
            .parse()
            .map_err(|_| format!("invalid range upper bound `{upper_trimmed}`"))?;
        if lower > upper {
            return Err(format!(
                "range lower bound `{lower}` is greater than upper bound `{upper}`"
            ));
        }
        charges.extend(lower..=upper);
    } else {
        for part in spec.split(',') {
            let part_trimmed = part.trim();
            let charge: u8 = part_trimmed
                .parse()
                .map_err(|_| format!("invalid charge `{part_trimmed}`"))?;
            charges.push(charge);
        }
    }

    charges.sort_unstable();
    charges.dedup();
    Ok(charges)
}

/// Controller providing SRM/PRM target finding under `/api/tools`.
pub struct ToolsController;

impl ToolsController {
    /// Builds the axum router for the tools endpoints, mounted onto the given server state.
    pub fn routes(state: Arc<ServerState>) -> Router<Arc<ServerState>> {
        let router: Router<Arc<ServerState>> =
            Router::new().route(PRM_SRM_ASSAY, post(Self::srm_prm_target_finder));

        router.with_state(state)
    }

    /// Returns the base path this controller is mounted on (`/api/tools`).
    pub fn controller_path() -> &'static str {
        CONTROLLER_PATH
    }

    /// Searches suitable peptides for SRM/PRM assays: for each requested (protein accession,
    /// charge spec) target, digests the protein in-memory, searches the given taxonomies
    /// (expanded to their species-level subtree) and returns only peptides that are unique
    /// within an individual species, at every requested charge.
    ///
    /// # Arguments
    /// * `state` - Server state
    /// * `payload` - The request body, see [SrmPrmRequest]
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/tools/prm-srm`
    /// * Method: `POST`
    ///
    /// ```json
    /// {
    ///     "targets": [
    ///         ["P12345", "2"],
    ///         ["Q9WTP6", "2-4"]
    ///     ],
    ///     "max_variable_modifications": 2,
    ///     "ptms": [],
    ///     "taxonomies": [10090, 9606]
    /// }
    /// ```
    /// See [SrmPrmRequest] for details.
    ///
    /// ## Response
    /// ```json
    /// {
    ///     "targets": [
    ///         {
    ///             "sequence": "NCLETPSC[+57.021464]KNGFLLDGFPR",
    ///             "mz": 1003.5,
    ///             "charge": 2,
    ///             "taxonomy_id": 10090,
    ///             "accession": "P12345 (GENE1, GENE2)"
    ///         },
    ///         ...
    ///     ]
    /// }
    /// ```
    /// See [SrmPrmResponse] for details.
    ///
    pub async fn srm_prm_target_finder(
        State(server_state): State<Arc<ServerState>>,
        Json(payload): Json<SrmPrmRequest>,
    ) -> Result<Response, Error> {
        // Expand every requested taxonomy to its species subtree; union all resulting
        // species IDs into one sorted, deduped list, reused as the taxonomy scoping filter
        // for every target below.
        let mut all_species_ids: Vec<i32> = Vec::new();
        for &taxonomy_id in &payload.taxonomies {
            let matching_taxonomy_ids = TaxonomyTable::new(server_state.db_client())
                .select_sub_species(taxonomy_id)
                .await?
                .map(|taxonomy_result| taxonomy_result.map(|taxonomy| taxonomy.id()))
                .try_collect::<Vec<i32>>()
                .await?;

            if matching_taxonomy_ids.is_empty() {
                return Err(Error::TaxonomyNotFound(taxonomy_id));
            }
            all_species_ids.extend(matching_taxonomy_ids);
        }
        all_species_ids.sort_unstable();
        all_species_ids.dedup();

        // Build the PTM collection once, reused across every target.
        let modifications: Vec<PostTranslationalModification> = match payload
            .ptms
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
                )
                    .into_response());
            }
        };

        let ptm_collection = match PTMCollection::new(modifications.into_iter().map(Arc::new)) {
            Ok(collection) => collection,
            Err(err) => {
                return Ok((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                    Body::from(format!("Error while validating PTMs: {:?}", err)),
                )
                    .into_response());
            }
        };

        // For each (accession, charge spec) target: digest the protein in-memory, filter its
        // peptides to those unique within a single selected species, apply the PTM collection
        // to each unique peptide, and emit one target per resulting peptidoform/charge pair.
        // No cross-target dedup/ambiguity check is needed here: the taxonomy-uniqueness check
        // above already guarantees each peptide's sequence is unique among the selected species,
        // and `seen_sequences` below already guarantees each peptide contributes each distinct
        // peptidoform sequence at most once — so the same sequence can only reappear because it
        // was legitimately requested at more than one charge, not because of any duplication.
        let mut targets: Vec<SrmPrmTarget> = Vec::new();
        for (accession, charge_spec) in payload.targets {
            let charges = match parse_charge_spec(&charge_spec) {
                Ok(charges) => charges,
                Err(err) => {
                    return Ok((
                        StatusCode::UNPROCESSABLE_ENTITY,
                        DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                        Body::from(format!(
                            "Error while parsing charge spec `{charge_spec}`: {err}"
                        )),
                    )
                        .into_response());
                }
            };

            let protein = ProteinTable::new(server_state.db_client())
                .select(
                    "WHERE accession = $1 LIMIT 1",
                    vec![Box::new(accession.to_uppercase())],
                )
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .pop()
                .ok_or_else(|| Error::ProteinNotFound(accession.clone()))?;

            let accession_label = if protein.genes().is_empty() {
                protein.accession().to_string()
            } else {
                format!("{} ({})", protein.accession(), protein.genes().join(", "))
            };

            let peptides =
                ProteinController::digest_and_fetch_peptides(&protein, server_state.as_ref())
                    .await?;

            for peptide in peptides {
                // Check which taxonomy was matched
                let taxonomy_ids = peptide
                    .unique_taxonomy_ids()
                    .iter()
                    .filter(|&id| all_species_ids.contains(id))
                    .cloned()
                    .collect::<Vec<_>>();

                if taxonomy_ids.len() != 1 {
                    continue; // skip this peptide as it is not unique among the given taxonomies
                }
                let taxonomy_id = taxonomy_ids[0];

                // Apply the PTM collection to this peptide "on the fly" (in-memory, no DB
                // round-trip): build the condition(s) around this peptide's own mass so
                // `PeptideConditionBuilder::finalize` maps onto a real DB partition, then run
                // each condition's filter pipeline and, on match, enumerate its peptidoforms.
                // Dedup by sequence string (not `Peptidoform` itself as a `HashSet` key —
                // clippy's `mutable_key_type` flags interior mutability reachable through it).
                let mut peptidoforms: Vec<Peptidoform> = Vec::new();
                let mut seen_sequences: HashSet<String> = HashSet::new();
                let builders = if ptm_collection.is_empty() {
                    vec![PeptideConditionBuilder::new(peptide.mass())]
                } else {
                    let (min_mass, max_mass) = PeptideSearch::ptm_mass_bounds(
                        peptide.mass(),
                        &ptm_collection,
                        server_state.configuration().protease(),
                    );
                    PeptideConditionBuilder::from_ptm_collection(
                        &ptm_collection,
                        peptide.mass(),
                        min_mass,
                        max_mass,
                        payload.max_variable_modifications,
                    )
                };

                for builder in builders {
                    for condition in
                        builder.finalize(server_state.configuration().mass_partitioning(), 0, 0)
                    {
                        if condition.is_match(&peptide) {
                            for peptidoform in condition.modify_peptide(&peptide) {
                                if seen_sequences.insert(peptidoform.sequence().to_string()) {
                                    peptidoforms.push(peptidoform);
                                }
                            }
                        }
                    }
                }

                // Always include the fully unmodified peptide as a target, regardless of the
                // PTM collection: e.g. the "no static modification" condition above excludes
                // peptides that contain a statically-modified amino acid, so a peptide with
                // such a residue would otherwise never surface its plain, unmodified form.
                let unmodified = Peptidoform::from(peptide);
                if seen_sequences.insert(unmodified.sequence().to_string()) {
                    peptidoforms.push(unmodified);
                }

                for peptidoform in peptidoforms {
                    let mass = to_float(peptidoform.mass());
                    for &charge in &charges {
                        let target = SrmPrmTarget {
                            sequence: peptidoform.sequence().to_string(),
                            mz: dalton_to_mass_to_charge(mass, charge),
                            charge,
                            taxonomy_id,
                            accession: accession_label.clone(),
                        };

                        targets.push(target);
                    }
                }
            }
        }

        Ok(Json(SrmPrmResponse { targets }).into_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_charge_spec_single() {
        assert_eq!(parse_charge_spec("2").unwrap(), vec![2]);
    }

    #[test]
    fn parse_charge_spec_list() {
        assert_eq!(parse_charge_spec("2,4,3,2").unwrap(), vec![2, 3, 4]);
    }

    #[test]
    fn parse_charge_spec_range() {
        assert_eq!(parse_charge_spec("2-4").unwrap(), vec![2, 3, 4]);
    }

    #[test]
    fn parse_charge_spec_whitespace() {
        assert_eq!(parse_charge_spec(" 2 , 3 ").unwrap(), vec![2, 3]);
        assert_eq!(parse_charge_spec(" 2 - 4 ").unwrap(), vec![2, 3, 4]);
    }

    #[test]
    fn parse_charge_spec_invalid() {
        assert!(parse_charge_spec("").is_err());
        assert!(parse_charge_spec("abc").is_err());
        assert!(parse_charge_spec("4-2").is_err());
        assert!(parse_charge_spec("2,abc").is_err());
    }
}
