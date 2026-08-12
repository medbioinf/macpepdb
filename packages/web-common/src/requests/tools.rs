use serde::{Deserialize, Serialize};

use crate::requests::ptm::PostTranslationalModificationRequest;

/// Request body for `POST /api/tools/prm-srm`. `thompson` is a list of independent
/// (m/z, charge) targets; `taxonomies`, tolerances and `ptms`
/// apply to every target. The backend expands each taxonomy ID to its species-level subtree
/// and only returns peptides unique within an individual species.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SrmPrmRequest {
    pub thompson: Vec<(f64, u8)>,
    pub lower_tolerance_ppm: i64,
    pub upper_tolerance_ppm: i64,
    pub max_variable_modifications: usize,
    pub ptms: Vec<PostTranslationalModificationRequest>,
    pub taxonomies: Vec<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::requests::ptm::{PtmPosition, PtmType};

    #[test]
    fn srm_prm_request_round_trips() {
        let request = SrmPrmRequest {
            thompson: vec![(1003.5, 2), (750.25, 3)],
            lower_tolerance_ppm: 5,
            upper_tolerance_ppm: 5,
            max_variable_modifications: 2,
            ptms: vec![PostTranslationalModificationRequest {
                name: "Oxidation".to_string(),
                amino_acid: 'M',
                mass_delta: 15.994915,
                mod_type: PtmType::Variable,
                position: PtmPosition::Anywhere,
            }],
            taxonomies: vec![10090, 9606],
        };

        let json = serde_json::to_string(&request).unwrap();
        let round_tripped: SrmPrmRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, request);
    }
}
