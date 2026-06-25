use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Taxonomy {
    /// Taxonomy ID
    pub id: u64,
    /// Parent taxonomy ID
    pub parent_id: u64,
    /// Scientific name
    pub scientific_name: String,
    // Rank
    pub rank: String,
}
