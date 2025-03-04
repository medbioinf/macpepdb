use std::borrow::Cow;

use scylla::{
    frame::response::result::ColumnType,
    macros::{DeserializeValue, SerializeValue},
};
use serde::{Deserialize, Serialize};

const DOMAIN_TYPE_NAME: &str = "domain";

#[derive(
    Debug, Clone, Hash, Eq, PartialEq, DeserializeValue, SerializeValue, Serialize, Deserialize,
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
    #[allow(clippy::too_many_arguments)]
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

    pub fn get_user_defined_type(keyspace: Cow<'_, str>) -> ColumnType<'_> {
        ColumnType::UserDefinedType {
            keyspace,
            type_name: Cow::Borrowed(DOMAIN_TYPE_NAME),
            field_types: vec![
                (Cow::Borrowed("name"), ColumnType::Text),
                (Cow::Borrowed("evidence"), ColumnType::Text),
                (Cow::Borrowed("start_index"), ColumnType::BigInt),
                (Cow::Borrowed("end_index"), ColumnType::BigInt),
                (Cow::Borrowed("protein"), ColumnType::Text),
                (Cow::Borrowed("start_index_protein"), ColumnType::BigInt),
                (Cow::Borrowed("end_index_protein"), ColumnType::BigInt),
                (Cow::Borrowed("peptide_offset"), ColumnType::BigInt),
            ],
        }
    }
}
