use serde::{Deserialize, Serialize};

/// Request body for `POST /api/taxonomies/search`. `search_query` is matched either as an exact
/// taxonomy ID or as a case-sensitive substring of the scientific name (the backend wraps it in
/// SQL `%...%` itself — do not add wildcard characters here).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchRequestBody {
    pub search_query: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_request_body_round_trips() {
        let body = SearchRequestBody {
            search_query: "sapiens".to_string(),
        };
        let json = serde_json::to_string(&body).unwrap();
        assert_eq!(json, r#"{"search_query":"sapiens"}"#);
        assert_eq!(
            serde_json::from_str::<SearchRequestBody>(&json).unwrap(),
            body
        );
    }
}
