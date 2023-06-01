// 3rd party imports
use anyhow::Result;
use scylla::{transport::session::Session, SessionBuilder};

pub trait GenericClient {
    async fn new(database_url: &str) -> Result<Self>
    where
        Self: Sized;
    fn get_session(&self) -> &Session;
}

pub struct Client {
    session: Session,
}

impl GenericClient for Client {
    async fn new(database_url: &str) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            session: SessionBuilder::new()
                .known_node(database_url)
                .build()
                .await?,
        })
    }

    fn get_session(&self) -> &Session {
        &self.session
    }
}
