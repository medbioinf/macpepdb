use serde::{Deserialize, Serialize};

/// Wire shape for `GET /api/taxonomies/{id}`, `GET /api/taxonomies/{id}/sub` (one array element),
/// and `POST /api/taxonomies/search` (one array element). Mirrors the backend's `Taxonomy`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaxonomyResponse {
    pub id: i32,
    pub parent_id: i32,
    pub scientific_name: String,
    pub rank_id: i16,
    pub rank_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taxonomy_response_round_trips() {
        let taxonomy = TaxonomyResponse {
            id: 9606,
            parent_id: 9605,
            scientific_name: "Homo sapiens".to_string(),
            rank_id: 1,
            rank_name: Some("species".to_string()),
        };

        let json = serde_json::to_string(&taxonomy).unwrap();
        let round_tripped: TaxonomyResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, taxonomy);
    }
}
