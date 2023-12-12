/// 404 page
mod not_found;
/// Protein search
mod protein_search;
/// Information page about MaCPepDB
mod status;

// reexport
pub use not_found::NotFound;
pub use protein_search::ProteinSearch;
pub use status::Status;
