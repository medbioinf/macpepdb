/// Alternative database implementation with client as central point
pub mod alternative;
/// Trait define configuration table
pub mod configuration_table;
/// Trait for database building / maintenance.
pub mod database_build;
/// Trait for database client
pub mod generic_client;
/// Support for ScyllaDB
pub mod scylla;
/// Trait for selectable tables storing entity data
pub mod selectable_table;
/// Trait for tables
pub mod table;
