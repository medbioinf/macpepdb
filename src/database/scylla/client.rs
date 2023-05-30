use scylla::transport::session::Session;

pub trait GenericClient {
    fn new(session: Session) -> Self;
    fn get_session(&self) -> &Session;
}

pub struct Client {
    session: Session,
}

impl GenericClient for Client {
    fn new(session: Session) -> Self {
        Self { session }
    }

    fn get_session(&self) -> &Session {
        &self.session
    }
}
