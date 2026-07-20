use serde::{Deserialize, Serialize};

/// Whether a post translational modification is always present (static) or optional (variable)
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PtmType {
    Static,
    Variable,
}

/// Where on the peptide/amino acid a post translational modification can occur.
/// Wire representation must match `dihardts_omicstools`'s `Position::Display`/`FromStr`
/// (`"Anywhere"`, `"Terminus-N"`, `"Terminus-C"`, `"Bond-N"`, `"Bond-C"`), hence the renames.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PtmPosition {
    Anywhere,
    #[serde(rename = "Terminus-N")]
    NTerminus,
    #[serde(rename = "Terminus-C")]
    CTerminus,
    #[serde(rename = "Bond-N")]
    NBond,
    #[serde(rename = "Bond-C")]
    CBond,
}

/// Wire shape of a post translational modification, as sent by the frontend inside a peptide
/// search request body and received by the backend.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PostTranslationalModificationRequest {
    pub name: String,
    pub amino_acid: char,
    pub mass_delta: f64,
    pub mod_type: PtmType,
    pub position: PtmPosition,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ptm_position_round_trips_with_backend_wire_format() {
        let cases = [
            (PtmPosition::Anywhere, "\"Anywhere\""),
            (PtmPosition::NTerminus, "\"Terminus-N\""),
            (PtmPosition::CTerminus, "\"Terminus-C\""),
            (PtmPosition::NBond, "\"Bond-N\""),
            (PtmPosition::CBond, "\"Bond-C\""),
        ];

        for (position, expected_json) in cases {
            let json = serde_json::to_string(&position).unwrap();
            assert_eq!(json, expected_json);
            let round_tripped: PtmPosition = serde_json::from_str(&json).unwrap();
            assert_eq!(round_tripped, position);
        }
    }

    #[test]
    fn post_translational_modification_request_round_trips() {
        let ptm = PostTranslationalModificationRequest {
            name: "Oxidation".to_string(),
            amino_acid: 'M',
            mass_delta: 15.994915,
            mod_type: PtmType::Variable,
            position: PtmPosition::Anywhere,
        };

        let json = serde_json::to_string(&ptm).unwrap();
        let round_tripped: PostTranslationalModificationRequest =
            serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, ptm);
    }
}
