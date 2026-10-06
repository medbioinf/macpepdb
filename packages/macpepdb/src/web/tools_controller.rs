use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use crate::mass::{dalton_to_mass_to_charge, to_float};
use crate::peptide::{IsPeptide, Peptide, Peptidoform};
use crate::peptide_search::{PeptideConditionBuilder, PeptideSearch};
use crate::post_translational_modification::{PTMCollection, PostTranslationalModification};
use crate::protein_table::ProteinTable;
use crate::sequence::{IsBitSequence, IsSimpleSequence};
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

/// m/z tolerance (ppm) below which two targets of the same charge count as similar.
const SIMILAR_MZ_TOLERANCE_PPM: f64 = 10.0;

/// Result of the taxonomy-array based uniqueness check of a peptide.
#[derive(Debug, PartialEq, Eq)]
enum Uniqueness {
    /// Unique in exactly one selected taxon, no other selected taxon contains it.
    Unique(i32),
    /// Contained in exactly one selected taxon, but in more than one protein there. Unique
    /// only if all of these proteins are isoforms of the target, needs a protein check.
    CheckProteins(i32),
    /// Not contained in any selected taxon or shared by multiple selected taxa.
    Shared,
}

/// Classifies a peptide by its taxonomy arrays within the selected taxa. Sharing with taxa
/// outside the selection is ignored.
///
/// # Arguments
/// * `unique_taxonomy_ids` - Taxa in which the peptide occurs in exactly one protein
/// * `non_unique_taxonomy_ids` - Taxa in which the peptide occurs in more than one protein
/// * `selected` - Selected (species) taxonomy IDs
///
fn classify_uniqueness(
    unique_taxonomy_ids: &[i32],
    non_unique_taxonomy_ids: &[i32],
    selected: &HashSet<i32>,
) -> Uniqueness {
    let mut matching = unique_taxonomy_ids
        .iter()
        .chain(non_unique_taxonomy_ids)
        .filter(|id| selected.contains(id));
    match (matching.next(), matching.next()) {
        (Some(&id), None) if unique_taxonomy_ids.contains(&id) => Uniqueness::Unique(id),
        (Some(&id), None) => Uniqueness::CheckProteins(id),
        _ => Uniqueness::Shared,
    }
}

/// Strips an isoform suffix (`-<digits>`) from an accession: `P12345-2` -> `P12345`.
fn base_accession(accession: &str) -> &str {
    match accession.rsplit_once('-') {
        Some((base, suffix))
            if !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            base
        }
        _ => accession,
    }
}

/// Checks if all proteins of the peptide within the selected taxa are the target protein or
/// its isoforms.
///
/// # Arguments
/// * `protein_ids` - IDs of all proteins containing the peptide
/// * `proteins` - Resolved proteins (ID -> (accession, taxonomy ID))
/// * `selected` - Selected (species) taxonomy IDs
/// * `target_base_accession` - Accession of the target without isoform suffix
///
fn only_isoforms_of_target(
    protein_ids: &[i32],
    proteins: &HashMap<i32, (String, i32)>,
    selected: &HashSet<i32>,
    target_base_accession: &str,
) -> bool {
    let mut found_any = false;
    for protein_id in protein_ids {
        let Some((accession, taxonomy_id)) = proteins.get(protein_id) else {
            return false;
        };
        if !selected.contains(taxonomy_id) {
            continue;
        }
        if base_accession(accession) != target_base_accession {
            return false;
        }
        found_any = true;
    }
    found_any
}

/// Marks targets which have another target (different sequence) with the same charge and an
/// m/z within the tolerance, e.g. modified forms colliding with other peptides.
///
/// # Arguments
/// * `targets` - Targets to inspect and mark
/// * `tolerance_ppm` - Tolerance in ppm
///
fn flag_similar_mz(targets: &mut [SrmPrmTarget], tolerance_ppm: f64) {
    let mut order: Vec<usize> = (0..targets.len()).collect();
    order.sort_by(|&a, &b| {
        targets[a]
            .charge
            .cmp(&targets[b].charge)
            .then(targets[a].mz.total_cmp(&targets[b].mz))
    });

    for i in 0..order.len() {
        for j in (i + 1)..order.len() {
            let (a, b) = (order[i], order[j]);
            if targets[a].charge != targets[b].charge
                || (targets[b].mz - targets[a].mz) / targets[a].mz * 1e6 > tolerance_ppm
            {
                break;
            }
            if targets[a].sequence != targets[b].sequence {
                targets[a].similar_mz = true;
                targets[b].similar_mz = true;
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
    ///     "taxonomies": [10090, 9606],
    ///     "max_missed_cleavages": 0
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
        let mut selected_species_ids: HashSet<i32> = HashSet::new();
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
            selected_species_ids.extend(matching_taxonomy_ids);
        }

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

            // Targets of a protein outside the selected taxa can not be unique within them.
            if !selected_species_ids.contains(&protein.taxonomy_id()) {
                continue;
            }
            let target_base_accession = base_accession(protein.accession()).to_string();

            // First pass: missed cleavages + cheap uniqueness classification from the taxonomy
            // arrays. Peptides that are only non-unique because of isoforms need a protein check.
            let mut classified: Vec<(Peptide, i32, bool)> = Vec::new();
            let mut pending_protein_ids: HashSet<i32> = HashSet::new();
            for peptide in peptides {
                // Missed cleavages are calculated on the fly via the protease
                if server_state
                    .configuration()
                    .protease()
                    .count_missed_cleavages(peptide.sequence().data())
                    > payload.max_missed_cleavages
                {
                    continue;
                }

                match classify_uniqueness(
                    peptide.unique_taxonomy_ids(),
                    peptide.non_unique_taxonomy_ids(),
                    &selected_species_ids,
                ) {
                    Uniqueness::Shared => {}
                    Uniqueness::Unique(taxonomy_id) => {
                        classified.push((peptide, taxonomy_id, false))
                    }
                    Uniqueness::CheckProteins(taxonomy_id) => {
                        pending_protein_ids.extend(peptide.protein_ids().as_slice());
                        classified.push((peptide, taxonomy_id, true));
                    }
                }
            }

            // Second pass: resolve the proteins of the remaining candidates (one query per
            // target). A peptide shared only by the target and its isoforms stays unique.
            let proteins_by_id: HashMap<i32, (String, i32)> = if pending_protein_ids.is_empty() {
                HashMap::new()
            } else {
                let ids: Vec<i32> = pending_protein_ids.into_iter().collect();
                ProteinTable::new(server_state.db_client())
                    .select_by_ids(&ids)
                    .await?
                    .try_filter_map(|protein| async move {
                        Ok(protein.id().map(|id| {
                            (id, (protein.accession().to_string(), protein.taxonomy_id()))
                        }))
                    })
                    .try_collect()
                    .await?
            };

            let verified = classified
                .into_iter()
                .filter(|(peptide, _, needs_protein_check)| {
                    !needs_protein_check
                        || only_isoforms_of_target(
                            peptide.protein_ids().as_slice(),
                            &proteins_by_id,
                            &selected_species_ids,
                            &target_base_accession,
                        )
                })
                .map(|(peptide, taxonomy_id, _)| (peptide, taxonomy_id))
                .collect::<Vec<_>>();

            for (peptide, taxonomy_id) in verified {
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
                    let plain_sequence: String = peptidoform
                        .sequence()
                        .amino_acids()
                        .map(|aa| aa.code())
                        .collect();
                    let hydrophobicity =
                        macpepdb_peptide_hydrophobicity::krokhin::score_sequence(&plain_sequence);
                    for &charge in &charges {
                        let target = SrmPrmTarget {
                            sequence: peptidoform.sequence().to_string(),
                            mz: dalton_to_mass_to_charge(mass, charge),
                            hydrophobicity,
                            charge,
                            taxonomy_id,
                            accession: accession_label.clone(),
                            similar_mz: false,
                        };

                        targets.push(target);
                    }
                }
            }
        }

        flag_similar_mz(&mut targets, SIMILAR_MZ_TOLERANCE_PPM);

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

    fn selected(ids: &[i32]) -> HashSet<i32> {
        ids.iter().copied().collect()
    }

    #[test]
    fn classify_unique_in_single_taxon() {
        assert_eq!(
            classify_uniqueness(&[9606], &[], &selected(&[9606, 10090])),
            Uniqueness::Unique(9606)
        );
    }

    #[test]
    fn classify_ignores_taxa_outside_selection() {
        assert_eq!(
            classify_uniqueness(&[9606], &[10090], &selected(&[9606])),
            Uniqueness::Unique(9606)
        );
    }

    #[test]
    fn classify_shared_between_selected_taxa() {
        assert_eq!(
            classify_uniqueness(&[9606], &[10090], &selected(&[9606, 10090])),
            Uniqueness::Shared
        );
        assert_eq!(
            classify_uniqueness(&[9606, 10090], &[], &selected(&[9606, 10090])),
            Uniqueness::Shared
        );
    }

    #[test]
    fn classify_non_unique_needs_protein_check() {
        assert_eq!(
            classify_uniqueness(&[], &[9606], &selected(&[9606])),
            Uniqueness::CheckProteins(9606)
        );
    }

    #[test]
    fn classify_empty_arrays_or_no_match() {
        assert_eq!(
            classify_uniqueness(&[], &[], &selected(&[9606])),
            Uniqueness::Shared
        );
        assert_eq!(
            classify_uniqueness(&[10090], &[], &selected(&[9606])),
            Uniqueness::Shared
        );
    }

    #[test]
    fn base_accession_strips_isoform_suffix() {
        assert_eq!(base_accession("P12345-2"), "P12345");
        assert_eq!(base_accession("P12345"), "P12345");
        assert_eq!(base_accession("P12345-abc"), "P12345-abc");
    }

    #[test]
    fn only_isoforms_of_target_cases() {
        let proteins: HashMap<i32, (String, i32)> = HashMap::from([
            (1, ("P12345".to_string(), 9606)),
            (2, ("P12345-2".to_string(), 9606)),
            (3, ("Q99999".to_string(), 9606)),
            (4, ("Q99999".to_string(), 10090)),
        ]);
        let sel = selected(&[9606]);
        assert!(only_isoforms_of_target(&[1, 2], &proteins, &sel, "P12345"));
        assert!(!only_isoforms_of_target(&[1, 3], &proteins, &sel, "P12345"));
        // protein outside the selected taxa is ignored
        assert!(only_isoforms_of_target(&[1, 4], &proteins, &sel, "P12345"));
        // unresolved protein or no protein at all
        assert!(!only_isoforms_of_target(&[1, 9], &proteins, &sel, "P12345"));
        assert!(!only_isoforms_of_target(&[], &proteins, &sel, "P12345"));
    }

    fn target(sequence: &str, mz: f64, charge: u8) -> SrmPrmTarget {
        SrmPrmTarget {
            sequence: sequence.to_string(),
            mz,
            hydrophobicity: 0.0,
            charge,
            taxonomy_id: 9606,
            accession: "P12345".to_string(),
            similar_mz: false,
        }
    }

    #[test]
    fn flag_similar_mz_marks_close_targets_of_same_charge() {
        let mut targets = vec![
            target("AAAK", 500.0000, 2),
            target("AAAK[+1.0]", 500.0010, 2), // 2 ppm
            target("BBBK", 500.0010, 3),       // other charge
            target("CCCK", 600.0, 2),          // far away
        ];
        flag_similar_mz(&mut targets, 10.0);
        assert!(targets[0].similar_mz);
        assert!(targets[1].similar_mz);
        assert!(!targets[2].similar_mz);
        assert!(!targets[3].similar_mz);
    }

    #[test]
    fn flag_similar_mz_ignores_same_sequence() {
        let mut targets = vec![target("AAAK", 500.0, 2), target("AAAK", 500.0, 2)];
        flag_similar_mz(&mut targets, 10.0);
        assert!(!targets[0].similar_mz && !targets[1].similar_mz);
    }
}
