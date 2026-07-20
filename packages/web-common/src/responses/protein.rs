use serde::{Deserialize, Serialize};

/// Wire shape for `GET /api/proteins/{accession}` — a protein together with its full peptide
/// records.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(bound = "T: Serialize + for<'a> Deserialize<'a>")]
pub struct ProteinResponse<T: Serialize + for<'a> Deserialize<'a>> {
    pub accession: String,
    pub id: Option<i32>,
    pub sequence: String,
    pub taxonomy_id: i32,
    pub is_reviewed: bool,
    pub genes: Vec<String>,
    pub peptides: Vec<T>,
}

#[cfg(test)]
mod tests {
    use crate::responses::peptide::PeptideResponse;

    use super::*;

    #[test]
    fn protein_response_round_trips() {
        let protein = ProteinResponse {
            accession: "Q9WTP6".to_string(),
            id: Some(1),
            sequence: "MAPNVLASEPEIPKGIR".to_string(),
            taxonomy_id: 10090,
            is_reviewed: true,
            genes: vec!["Ak2".to_string()],
            peptides: vec![PeptideResponse {
                partition: Some(1),
                mass: 587.375495125,
                sequence: "ALKTR".to_string(),
                protein_ids: vec![1],
                unique_taxonomy_ids: vec![10090],
                non_unique_taxonomy_ids: vec![10090],
                is_swiss_prot: true,
                is_trembl: false,
                proteins: None,
            }],
        };

        let json = serde_json::to_string(&protein).unwrap();
        let round_tripped: ProteinResponse<PeptideResponse> = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, protein);
    }

    #[test]
    fn protein_summary_response_round_trips() {
        let protein = ProteinResponse {
            accession: "Q9WTP6".to_string(),
            id: Some(1),
            sequence: "MAPNVLASEPEIPKGIR".to_string(),
            taxonomy_id: 10090,
            is_reviewed: true,
            genes: vec!["Ak2".to_string()],
            peptides: vec!["ALKTR".to_string()],
        };

        let json = serde_json::to_string(&protein).unwrap();
        let round_tripped: ProteinResponse<String> = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, protein);
    }
}
