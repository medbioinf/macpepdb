/// Trait for database client
pub mod generic_client;
/// Support for ScyllaDB
pub mod scylla;
/// Trait for tables
pub mod table;

/// Database client
pub mod client;

/// System table for stroring complex document type datastructures like configurations
pub mod system_table;

/// Table for accessing proteins
pub mod protein_table;

/// Errors when working with the database
pub mod errors;
