use serde::{Deserialize, Serialize};

/// Wire shape for a single peptide (`GET /api/peptides/{sequence}`) or one element of a peptide
/// search result stream (`POST/GET /api/peptides/search`). Covers both the backend's `Peptide`
/// (has protein/taxonomy provenance) and `Peptidoform` (search result, may carry PTMs in
/// `sequence`, has no provenance) — the latter simply has empty `protein_ids`/taxonomy id lists.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PeptideResponse {
    pub partition: Option<i64>,
    pub mass: f64,
    pub sequence: String,
    pub protein_ids: Vec<i32>,
    pub unique_taxonomy_ids: Vec<i32>,
    pub non_unique_taxonomy_ids: Vec<i32>,
    pub is_swiss_prot: bool,
    pub is_trembl: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peptide_response_round_trips() {
        let peptide = PeptideResponse {
            partition: Some(19),
            mass: 1015.475679562,
            sequence: "HMENEKTK".to_string(),
            protein_ids: vec![123, 456],
            unique_taxonomy_ids: vec![10090],
            non_unique_taxonomy_ids: vec![10090],
            is_swiss_prot: true,
            is_trembl: false,
        };

        let json = serde_json::to_string(&peptide).unwrap();
        let round_tripped: PeptideResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, peptide);
    }
}
