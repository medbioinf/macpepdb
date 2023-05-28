pub mod migrations;

#[cfg(test)]
mod tests {
    use scylla::{transport::session::Session, SessionBuilder};

    pub const DATABASE_URL: &str = "127.0.0.1:9042";
    // let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| DATABASE_URL.to_string());
    pub async fn get_session() -> Session {
        return SessionBuilder::new()
            .known_node(DATABASE_URL)
            .build()
            .await
            .unwrap();
    }
}
