use super::client::Client;

pub struct Build;

impl Build {
    /// Build the database
    ///
    pub fn build(database_url: &str) {
        let client: Box<dyn Client<_>> = match database_url {
            "scylla" => Box::new(crate::database::alternative::scylla::client::Client::new(
                database_url,
            )),
            "pgsql" => Box::new(crate::database::alternative::pgsql::client::Client::new(
                database_url,
            )),
            _ => panic!("Unsupported database"),
        };
    }
}
