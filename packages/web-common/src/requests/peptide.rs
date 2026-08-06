use serde::{Deserialize, Serialize};

use crate::requests::ptm::PostTranslationalModificationRequest;

/// Query mass, either as Dalton or as (m/z, charge) which the backend converts to Dalton.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SearchRequestMass {
    ThompsonCharge(f64, u8),
    Dalton(f64),
}

/// Request body for `POST /api/peptides/search` (and its base64-encoded GET variant).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchRequestBody {
    pub mass: SearchRequestMass,
    pub lower_mass_tolerance_ppm: i64,
    pub upper_mass_tolerance_ppm: i64,
    pub max_variable_modifications: usize,
    pub modifications: Vec<PostTranslationalModificationRequest>,
    /// Not wired up server-side yet (see `crate::web::peptide_controller::search`) — kept typed
    /// so the frontend can already send it once taxonomy filtering is implemented.
    pub taxonomy_id: Option<i32>,
    pub is_reviewed: Option<bool>,
    pub resolve_modifications: Option<bool>,
}

/// Query parameters for `GET/POST /api/peptides/search`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchRequestQuery {
    #[serde(default)]
    pub is_download: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::requests::ptm::{PtmPosition, PtmType};

    #[test]
    fn search_request_mass_untagged_round_trips() {
        let dalton = SearchRequestMass::Dalton(2006.988396539);
        let json = serde_json::to_string(&dalton).unwrap();
        assert_eq!(json, "2006.988396539");
        assert_eq!(
            serde_json::from_str::<SearchRequestMass>(&json).unwrap(),
            dalton
        );

        let thompson_charge = SearchRequestMass::ThompsonCharge(1003.5, 2);
        let json = serde_json::to_string(&thompson_charge).unwrap();
        assert_eq!(json, "[1003.5,2]");
        assert_eq!(
            serde_json::from_str::<SearchRequestMass>(&json).unwrap(),
            thompson_charge
        );
    }

    #[test]
    fn search_request_body_round_trips() {
        let body = SearchRequestBody {
            mass: SearchRequestMass::Dalton(2006.988396539),
            lower_mass_tolerance_ppm: 5,
            upper_mass_tolerance_ppm: 5,
            max_variable_modifications: 3,
            modifications: vec![PostTranslationalModificationRequest {
                name: "Oxidation".to_string(),
                amino_acid: 'M',
                mass_delta: 15.994915,
                mod_type: PtmType::Variable,
                position: PtmPosition::Anywhere,
            }],
            taxonomy_id: Some(10090),
            is_reviewed: Some(true),
            resolve_modifications: Some(true),
        };

        let json = serde_json::to_string(&body).unwrap();
        let round_tripped: SearchRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, body);
    }
}
