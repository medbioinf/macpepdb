/// Endpoints for getting information about the database, digestion enzymes, max missed cleavages, etc.
pub mod configuration_controller;
/// Endpoints for getting peptides by sequence, mass search, ...
pub mod peptide_controller;
/// Endpoints for getting proteins by accession, ...
pub mod protein_controller;
/// Web server definition and configuration
pub mod server;
/// Endpoint for different tools and functions which comes along the MaCPepDB, e.g. mass calculation, digestion, ...
pub mod tools_controller;
/// Module for error handling
pub mod web_error;
