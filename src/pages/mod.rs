/// 404 page
mod not_found;
/// Peptide page
mod peptide;
/// Protein page
mod protein;
/// Protein search
mod protein_search;
/// Information page about MaCPepDB
mod status;

// reexport
pub use not_found::NotFound;
pub use peptide::Peptide;
pub use protein::Protein;
pub use protein_search::ProteinSearch;
pub use status::Status;
