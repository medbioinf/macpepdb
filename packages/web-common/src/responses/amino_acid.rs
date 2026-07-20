use serde::{Deserialize, Serialize};

/// Wire shape for `GET /api/chemistry/amino-acids` and `GET /api/chemistry/amino-acids/{code}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AminoAcidResponse {
    pub code: char,
    pub mono_mass: f64,
    pub is_canonical: bool,
    pub name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amino_acid_response_round_trips() {
        let amino_acid = AminoAcidResponse {
            code: 'G',
            mono_mass: 57.021463735,
            is_canonical: true,
            name: "Glycine".to_string(),
        };

        let json = serde_json::to_string(&amino_acid).unwrap();
        let round_tripped: AminoAcidResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, amino_acid);
    }
}
