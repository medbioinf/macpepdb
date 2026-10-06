use serde::{Deserialize, Serialize};

use crate::requests::ptm::PostTranslationalModificationRequest;

/// Review status of the proteins considered by the SRM/PRM target finder, both as targets and
/// when judging peptide uniqueness.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReviewStatus {
    SwissProt,
    TrEMBL,
    #[default]
    Both,
}

impl ReviewStatus {
    /// Returns `true` if a protein with the given review state is considered.
    pub fn matches(self, is_reviewed: bool) -> bool {
        match self {
            ReviewStatus::SwissProt => is_reviewed,
            ReviewStatus::TrEMBL => !is_reviewed,
            ReviewStatus::Both => true,
        }
    }
}

/// Request body for `POST /api/tools/prm-srm`. `targets` is a list of independent
/// (protein accession, charge spec) targets, where charge spec is a single integer
/// (`"2"`), a comma-separated list (`"2,3,4"`), or a range (`"2-4"`); `taxonomies` and
/// `ptms` apply to every target. The backend expands each taxonomy ID to its
/// species-level subtree and only returns peptides unique within an individual species.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SrmPrmRequest {
    pub targets: Vec<(String, String)>,
    pub max_variable_modifications: usize,
    pub ptms: Vec<PostTranslationalModificationRequest>,
    pub taxonomies: Vec<i32>,
    /// Maximum number of missed cleavages a peptide may contain (default 0). Counted on the
    /// fly via the configured protease.
    #[serde(default)]
    pub max_missed_cleavages: usize,
    /// Only proteins with this review status are considered: targets must match it and
    /// peptides only have to be unique among proteins matching it (default: both).
    #[serde(default)]
    pub review_status: ReviewStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::requests::ptm::{PtmPosition, PtmType};

    #[test]
    fn srm_prm_request_round_trips() {
        let request = SrmPrmRequest {
            targets: vec![
                ("P12345".to_string(), "2".to_string()),
                ("Q9WTP6".to_string(), "2-4".to_string()),
            ],
            max_variable_modifications: 2,
            ptms: vec![PostTranslationalModificationRequest {
                name: "Oxidation".to_string(),
                amino_acid: 'M',
                mass_delta: 15.994915,
                mod_type: PtmType::Variable,
                position: PtmPosition::Anywhere,
            }],
            taxonomies: vec![10090, 9606],
            max_missed_cleavages: 1,
            review_status: ReviewStatus::SwissProt,
        };

        let json = serde_json::to_string(&request).unwrap();
        let round_tripped: SrmPrmRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, request);
    }
}
