// 3rd party imports
use scylla::FromUserType;
use scylla::IntoUserType;

#[derive(
    Clone, PartialEq, Debug, IntoUserType, FromUserType, serde::Deserialize, serde::Serialize,
)]
pub struct Domain {
    name: String,
    evidence: String,
    start_index: i64,
    end_index: i64,
    protein: Option<String>,
    start_index_protein: Option<i64>,
    end_index_protein: Option<i64>,
    peptide_offset: Option<i64>,
}

impl Domain {
    pub fn new(
        start_index: i64,
        end_index: i64,
        name: String,
        evidence: String,
        protein: Option<String>,
        start_index_protein: Option<i64>,
        end_index_protein: Option<i64>,
        peptide_offset: Option<i64>,
    ) -> Self {
        Self {
            start_index,
            end_index,
            name,
            evidence,
            protein,
            start_index_protein,
            end_index_protein,
            peptide_offset,
        }
    }

    pub fn get_start_index(&self) -> &i64 {
        &self.start_index
    }

    pub fn get_end_index(&self) -> &i64 {
        &self.end_index
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_evidence(&self) -> &String {
        &self.evidence
    }

    pub fn get_protein_opt(self) -> Option<String> {
        self.protein
    }

    pub fn get_protein_start_index_opt(self) -> Option<i64> {
        self.start_index_protein
    }

    pub fn get_protein_end_index_opt(self) -> Option<i64> {
        self.end_index_protein
    }

    pub fn get_peptide_offset(self) -> Option<i64> {
        self.peptide_offset
    }
}
