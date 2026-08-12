use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::mass::mass_to_charge_to_dalton;
use crate::peptide::IsPeptide;
use crate::peptide_search::{PeptideSearch, PeptidoformPassthroughTransformation};
use crate::peptide_table::FULL_PEPTIDE_COLUMN_SELECTION;
use crate::post_translational_modification::{PTMCollection, PostTranslationalModification};
use crate::taxonomy_table::TaxonomyTable;
use crate::web::DEFAULT_ERROR_HEADER_MAP;
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
    #[error("Peptide search: {0}")]
    PeptideSearch(#[from] crate::peptide_search::Error),
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

    /// Searches suitable peptides for SRM/PRM assays: for each requested (m/z, charge)
    /// target, searches the given taxonomies (expanded to their species-level subtree) and
    /// returns only peptides that are unique within an individual species.
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
    ///     "thompson": [
    ///         [1003.5, 2],
    ///         [750.25, 3]
    ///     ],
    ///     "lower_tolerance_ppm": 10,
    ///     "upper_tolerance_ppm": 10,
    ///     "max_variable_modifications": 2,
    ///     "ptms": [],
    ///     "taxonomies": [10090, 9606],
    ///     "normalized_collision_energy": 30
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
    ///             "normalized_collision_energy": 30
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
        // for every target-mass search below.
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

        // Build the PTM collection once, reused across every target mass.
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
            Ok(collection) => Arc::new(collection),
            Err(err) => {
                return Ok((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                    Body::from(format!("Error while validating PTMs: {:?}", err)),
                )
                    .into_response());
            }
        };

        // Run one PeptideSearch per (m/z, charge) target, reusing all_species_ids and
        // ptm_collection across all of them.
        let mut targets: HashMap<SrmPrmTarget, bool> = HashMap::new();
        for (mz, charge) in payload.thompson {
            tracing::info!("{} Da", mass_to_charge_to_dalton(mz, charge),);

            let mass = mass_to_int!(mass_to_charge_to_dalton(mz, charge));
            let search = PeptideSearch::new(
                server_state.db_client(),
                &FULL_PEPTIDE_COLUMN_SELECTION,
                server_state.configuration(),
                mass,
                payload.lower_tolerance_ppm,
                payload.upper_tolerance_ppm,
                payload.max_variable_modifications,
                true, // is_distinct
                Some(all_species_ids.clone()),
                None, // is_reviewed
                ptm_collection.clone(),
                true, // resolve_modifications: assay target mass depends on the exact PTM combination
                server_state.concurrent_searches(),
            );

            let mut stream = search
                .search::<PeptidoformPassthroughTransformation>()
                .await?;
            while let Some(batch) = stream.next().await {
                for peptidoform in batch? {
                    // Check which taxonomy was matched
                    let taxonomy_ids = peptidoform
                        .unique_taxonomy_ids()
                        .iter()
                        .filter(|&id| all_species_ids.contains(id))
                        .cloned()
                        .collect::<Vec<_>>();

                    if taxonomy_ids.len() != 1 {
                        continue; // skip this peptide as it is not unique among the given taxonomies
                    }

                    let target = SrmPrmTarget {
                        sequence: peptidoform.sequence().to_string(),
                        mz,
                        charge,
                        taxonomy_id: taxonomy_ids.first().copied().unwrap(),
                    };

                    targets
                        .entry(target)
                        .and_modify(|seen_multiple_times| {
                            tracing::info!("already seen");
                            *seen_multiple_times = true
                        })
                        .or_insert(false);
                }
            }
        }

        targets.retain(|_, seen_multiple_times| !*seen_multiple_times); // only keep targets that were seen once (unique)

        let targets = targets.into_keys().collect::<Vec<_>>();

        // FEATURE: Calculate Calculate collisional cross sectio and calculate ion mobility
        // if !targets.is_empty() {
        //     let calculate_collisional_cross_sections = KoinaClient::new("https://koina.wilhelmlab.org:443")
        //         .im2deep_prediction(
        //             targets
        //                 .iter()
        //                 .map(|target| target.sequence.clone())
        //                 .collect::<Vec<_>>(),
        //             targets
        //                 .iter()
        //                 .map(|target| &target.charge)
        //                 .cloned()
        //                 .collect::<Vec<_>>(),
        //         )
        //         .await?;
        //
        //     for (calculate_collisional_cross_section, target) in calculate_collisional_cross_sections.into_iter().zip(targets.iter_mut()) {
        //         target.ion_mobility_mut().replace(calc_ion_mobility);
        //     }
        // }

        Ok(Json(SrmPrmResponse { targets }).into_response())
    }
}
