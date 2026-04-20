use thiserror::Error;

use crate::sequence::IsSequence;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Cql error: {0}")]
    Cql(Box<scylla::errors::ExecutionError>),
    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("{0}")]
    Sequence(#[from] crate::sequence::Error),
}

#[derive(Eq, Hash, PartialEq)]
pub struct Peptide<S: IsSequence> {
    mass: i64,
    sequence: S,
}

impl<S> Peptide<S>
where
    S: IsSequence,
{
    pub fn new(sequence: S) -> Result<Self, Error> {
        let mass = sequence.to_peptide_mass()?;
        Ok(Self { mass, sequence })
    }

    pub fn mass(&self) -> i64 {
        self.mass
    }

    pub fn sequence(&self) -> &S {
        &self.sequence
    }

    pub fn len(&self) -> usize {
        self.sequence.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sequence.is_empty()
    }

    pub fn psql_insert_statement() -> String {
        format!(
            "INSERT INTO {} (mass, sequence) VALUES ($1, $2) ON CONFLICT (mass, sequence) DO NOTHING",
            S::PEPTIDE_DATABASE
        )
    }

    pub async fn psql_insert<C: tokio_postgres::GenericClient>(
        &self,
        client: &C,
    ) -> Result<u64, Error> {
        client
            .execute(
                &Self::psql_insert_statement(),
                &[&self.mass(), self.sequence()],
            )
            .await
            .map_err(Error::Database)
    }

    pub async fn psql_insert_with_preped_statement<C: tokio_postgres::GenericClient>(
        &self,
        client: &C,
        prepared_statement: &tokio_postgres::Statement,
    ) -> Result<u64, Error> {
        client
            .execute(prepared_statement, &[&self.mass(), self.sequence()])
            .await
            .map_err(Error::Database)
    }

    pub fn cssndr_insert_statement() -> String {
        format!(
            "INSERT INTO {} (mass, sequence) VALUES (?, ?)",
            S::PEPTIDE_DATABASE
        )
    }

    pub async fn cssndr_insert_with_preped_statement(
        &self,
        client: &scylla::client::session::Session,
        prepared_statement: &scylla::statement::prepared::PreparedStatement,
    ) -> Result<(), Error> {
        client
            .execute_unpaged(prepared_statement, (self.mass(), self.sequence()))
            .await
            .map_err(|err| Error::Cql(Box::new(err)))?;

        Ok(())
    }

    pub async fn cssndr_insert_with_preped_statement_owned(
        self,
        client: &scylla::client::session::Session,
        prepared_statement: &scylla::statement::prepared::PreparedStatement,
    ) -> Result<Self, Error> {
        self.cssndr_insert_with_preped_statement(client, prepared_statement)
            .await?;
        Ok(self)
    }
}
