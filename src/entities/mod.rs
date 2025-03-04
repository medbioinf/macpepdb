// Struct which keeps configuration parameters
pub mod configuration;
/// Structs and functions for working with peptide entities.
pub mod peptide;
/// Structs and functions for working with protein entities.
pub mod protein;

#[cfg(feature = "domains")]
/// Structs and functions for working with domain entities.
pub mod domain;
