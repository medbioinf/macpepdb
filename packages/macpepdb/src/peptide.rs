use thiserror::Error;

use crate::sequence::IsSequence;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Database error: {0}")]
    Database(tokio_postgres::Error),
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

    pub async fn insert<C: tokio_postgres::GenericClient>(&self, client: &C) -> Result<u64, Error> {
        client
                .execute(
                    &format!("INSERT INTO {} (mass, sequence) VALUES ($1, $2) ON CONFLICT (mass, sequence) DO NOTHING", S::PEPTIDE_DATABASE),
                    &[&self.mass(), self.sequence()],
                )
                .await
                .map_err(Error::Database)
    }
}
