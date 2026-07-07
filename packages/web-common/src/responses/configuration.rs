use serde::{Deserialize, Serialize};

/// Mirrors the backend's `Protease`, minus the internal `Box<dyn IsProtease>` trait object
/// (reduced here to its `name`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProteaseResponse {
    pub name: String,
    pub semi_specific: bool,
    pub min_length: usize,
    pub max_length: usize,
    pub max_missed_cleavages: usize,
    pub keep_unknown: bool,
}

/// Wire shape for `GET /api/configuration`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeConfigurationResponse {
    pub protease: ProteaseResponse,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_configuration_response_round_trips() {
        let configuration = RuntimeConfigurationResponse {
            protease: ProteaseResponse {
                name: "trypsin".to_string(),
                semi_specific: false,
                min_length: 6,
                max_length: 50,
                max_missed_cleavages: 2,
                keep_unknown: false,
            },
        };

        let json = serde_json::to_string(&configuration).unwrap();
        let round_tripped: RuntimeConfigurationResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, configuration);
    }
}
