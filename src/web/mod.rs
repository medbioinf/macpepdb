/// Module for handling the web application state
pub mod app_state;
/// Controller for serving chemistry related information, e.g. amino acids
pub mod chemistry_controller;
/// Endpoints for getting information about the database, digestion enzymes, max missed cleavages, etc.
pub mod configuration_controller;
/// Controller for error handling
pub mod error_controller;
/// Endpoints for getting peptides by sequence, mass search, ...
pub mod peptide_controller;
/// Endpoints for getting proteins by accession, ...
pub mod protein_controller;
/// Web server definition and configuration
pub mod server;
/// Controller for accessing the taxonomy tree
pub mod taxonomy_controller;
/// Endpoint for different tools and functions which comes along the MaCPepDB, e.g. mass calculation, digestion, ...
/// and also has endpoints for some capabilities, e.g. the available proteases
pub mod tools_controller;
/// Module for error handling
pub mod web_error;
