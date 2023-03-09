/// Module for storing and retrieving data from the Citus database.
pub mod protein_table;


mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("src/database/citus/migrations");
}

#[cfg(test)]
mod tests {
    // std imports
    use std::thread::sleep;

    // 3rd party imports
    use postgres::{Client, NoTls};

    // internal imports
    use super::*;

    const DATABASE_URL: &str = "postgresql://postgres:developer@localhost:5433/macpepdb_dev";

    pub fn get_client() -> Client {
        return Client::connect(DATABASE_URL, NoTls).unwrap();
    }

    /// Clear the database of all tables. Use it before each test.
    /// 
    /// # Arguments
    /// `client` - The client to use to connect to the database.
    /// 
    pub fn prepare_database_for_tests() {
        let mut client = get_client();
        let mut transaction = client.transaction().unwrap();
        let row = transaction.query_one(
            "SELECT EXISTS (SELECT FROM pg_tables WHERE  schemaname = 'public' AND tablename  = 'refinery_schema_history');", 
            &[]
        ).unwrap();
        if row.get::<_, bool>(0) {
            transaction.execute("DELETE FROM refinery_schema_history", &[]).unwrap();
        }
        let drop_table_res = transaction.execute("DROP TABLE IF EXISTS proteins", &[]);
        assert!(drop_table_res.is_ok(), "Failed to drop table: {:?}", drop_table_res);

        transaction.commit().unwrap();
        sleep(std::time::Duration::from_secs(5)); // Wait for the database to finish dropping the tables.
        let migration_result = embedded::migrations::runner().run(&mut client);
        assert!(migration_result.is_ok(), "Failed to run migrations: {:?}", migration_result);
        client.close().unwrap();
    }
}