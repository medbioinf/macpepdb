use std::hash::Hash;

use serde::{Deserialize, Serialize};

/// One unique SRM/PRM assay target: a peptide digested from a requested protein accession,
/// matched at a given charge, unique within the given species.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SrmPrmTarget {
    /// ProForma-annotated sequence (modifications resolved).
    pub sequence: String,
    pub mz: f64,
    /// Predicted hydrophobicity (Krokhin/SSRCalc3 retention score).
    pub hydrophobicity: f64,
    pub charge: u8,
    /// Species-level taxonomy ID this peptide is unique in. May differ from the
    /// originally-requested taxonomy ID if that was a higher-rank clade.
    pub taxonomy_id: i32,
    /// Originating protein accession, with gene names in parentheses if any
    /// (e.g. `"P12345 (GENE1, GENE2)"`).
    pub accession: String,
    // FEATURE
    // pub ion_mobility: Option<f64>,
}

// FEATURE
// impl SrmPrmTarget {
//     pub fn ion_mobility_mut(&mut self) -> &mut Option<f64> {
//         &mut self.ion_mobility
//     }
// }

impl Hash for SrmPrmTarget {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sequence.hash(state);
    }
}

impl PartialEq for SrmPrmTarget {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence
    }
}

impl Eq for SrmPrmTarget {}

/// Response body for `POST /api/tools/prm-srm`. Contains only unique targets — a
/// peptide/species combination that is not unique in that species is omitted entirely.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SrmPrmResponse {
    pub targets: Vec<SrmPrmTarget>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn srm_prm_response_round_trips() {
        let response = SrmPrmResponse {
            targets: vec![
                SrmPrmTarget {
                    sequence: "NCLETPSC[+57.021464]KNGFLLDGFPR".to_string(),
                    mz: 1003.5,
                    hydrophobicity: 24.7,
                    charge: 2,
                    taxonomy_id: 10090,
                    accession: "P12345 (GENE1)".to_string(),
                },
                SrmPrmTarget {
                    sequence: "NCLETPSCKNGFLLDGFPR".to_string(),
                    mz: 750.25,
                    hydrophobicity: 22.1,
                    charge: 3,
                    taxonomy_id: 9606,
                    accession: "Q9WTP6".to_string(),
                },
            ],
        };

        let json = serde_json::to_string(&response).unwrap();
        let round_tripped: SrmPrmResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, response);
    }
}
