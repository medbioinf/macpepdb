#[cfg(feature = "domains")]
use std::borrow::Cow;
use std::sync::Arc;

use anyhow::Result;
use async_stream::try_stream;
use futures::Stream;
use scylla::batch::Batch;
use scylla::deserialize::row::ColumnIterator;
use scylla::deserialize::{DeserializationError, DeserializeRow, TypeCheckError};
use scylla::frame::response::result::{ColumnSpec, ColumnType, CqlValue};

use super::client::Client;
use super::errors::{ColumnTypeMismatchError, UnexpectedEndOfRowError};
use crate::database::generic_client::GenericClient;
use crate::database::scylla::errors::UnknownColumnError;
use crate::database::table::Table;
#[cfg(feature = "domains")]
use crate::entities::domain::Domain;
use crate::entities::peptide::Peptide;
use crate::entities::protein::Protein;
use crate::tools::cql::convert_raw_col;

const TABLE_NAME: &str = "proteins";

lazy_static! {
    static ref SELECT_COLS: Vec<&'static str> = vec![
        "accession",
        "secondary_accessions",
        "entry_name",
        "name",
        "genes",
        "taxonomy_id",
        "proteome_id",
        "is_reviewed",
        "sequence",
        "updated_at",
        #[cfg(feature = "domains")] "domains",
    ];

    static ref INSERT_COLS: Vec<&'static str> = SELECT_COLS.clone();

    /// Select statement without WHERE clause.
    ///
    static ref SELECT_STATEMENT: String = format!(
        "SELECT {} FROM :KEYSPACE:.{}",
        SELECT_COLS.join(", "),
        TABLE_NAME
    );

    /// Insert statement.
    /// Takes INSERT_COLS as columns in same order.
    static ref INSERT_STATEMENT: String = format!(
        "INSERT INTO :KEYSPACE:.{} ({}) VALUES ({})",
        TABLE_NAME,
        INSERT_COLS.join(", "),
        vec!["?"; INSERT_COLS.len()].join(", ")
    );

    /// Delete statement.
    /// Takes only the primary key as parameter.
    ///
    static ref DELETE_STATEMENT: String =
        format!("DELETE FROM :KEYSPACE:.{} WHERE accession = ?", TABLE_NAME);
}

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for Protein {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        for spec in specs {
            match spec.name() {
                "accession" => {
                    if *spec.typ() != ColumnType::Text {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: ColumnType::Text,
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                "secondary_accessions" => {
                    if *spec.typ() != ColumnType::List(Box::new(ColumnType::Text)) {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: ColumnType::Text,
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                "entry_name" => {
                    if *spec.typ() != ColumnType::Text {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: ColumnType::Text,
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                "name" => {
                    if *spec.typ() != ColumnType::Text {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: ColumnType::Text,
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                "genes" => {
                    if *spec.typ() != ColumnType::List(Box::new(ColumnType::Text)) {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: ColumnType::Text,
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                "taxonomy_id" => {
                    if *spec.typ() != ColumnType::BigInt {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: ColumnType::Text,
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                "proteome_id" => {
                    if *spec.typ() != ColumnType::Text {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: ColumnType::Text,
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                "is_reviewed" => {
                    if *spec.typ() != ColumnType::Boolean {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: ColumnType::Text,
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                "sequence" => {
                    if *spec.typ() != ColumnType::Text {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: ColumnType::Text,
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                "updated_at" => {
                    if *spec.typ() != ColumnType::BigInt {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: ColumnType::Text,
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                #[cfg(feature = "domains")]
                "domains" => {
                    if *spec.typ()
                        != ColumnType::Set(Box::new(Domain::get_user_defined_type(Cow::Owned(
                            spec.table_spec().ks_name().to_string(),
                        ))))
                    {
                        return Err(TypeCheckError::new(ColumnTypeMismatchError {
                            name: spec.name().to_string(),
                            expected: Domain::get_user_defined_type(Cow::Owned(
                                spec.table_spec().ks_name().to_string(),
                            )),
                            actual: spec.typ().clone().into_owned(),
                        }));
                    }
                }
                _ => {
                    return Err(TypeCheckError::new(UnknownColumnError {
                        name: spec.name().to_string(),
                        typ: spec.typ().clone().into_owned(),
                    }))
                }
            }
        }

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        Ok(Protein::new(
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
            #[cfg(feature = "domains")]
            convert_raw_col(
                row.next()
                    .ok_or(DeserializationError::new(UnexpectedEndOfRowError))??,
            )?,
        ))
    }
}

pub struct ProteinTable {}

impl ProteinTable {
    pub async fn insert(client: &Client, protein: &Protein) -> Result<()> {
        let statement = client.get_prepared_statement(&INSERT_STATEMENT).await?;
        client
            .execute_unpaged(
                &statement,
                (
                    protein.get_accession(),
                    protein.get_secondary_accessions(),
                    protein.get_entry_name(),
                    protein.get_name(),
                    protein.get_genes(),
                    protein.get_taxonomy_id(),
                    protein.get_proteome_id(),
                    &protein.get_is_reviewed(),
                    protein.get_sequence(),
                    &protein.get_updated_at(),
                    #[cfg(feature = "domains")]
                    protein.get_domains(),
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn update(client: &Client, old_prot: &Protein, updated_prot: &Protein) -> Result<()> {
        let mut batch: Batch = Batch::default();

        let delete_statement = DELETE_STATEMENT.replace(":KEYSPACE:", client.get_database());
        let insert_statement = INSERT_STATEMENT.replace(":KEYSPACE:", client.get_database());

        batch.append_statement(delete_statement.as_str());
        batch.append_statement(insert_statement.as_str());

        client
            .batch(
                &batch,
                (
                    (old_prot.get_accession(),),
                    (
                        updated_prot.get_accession(),
                        updated_prot.get_secondary_accessions(),
                        updated_prot.get_entry_name(),
                        updated_prot.get_name(),
                        updated_prot.get_genes(),
                        updated_prot.get_taxonomy_id(),
                        updated_prot.get_proteome_id(),
                        &updated_prot.get_is_reviewed(),
                        updated_prot.get_sequence(),
                        &updated_prot.get_updated_at(),
                        #[cfg(feature = "domains")]
                        updated_prot.get_domains(),
                    ),
                ),
            )
            .await?;

        Ok(())
    }

    pub async fn delete(client: &Client, protein: &Protein) -> Result<()> {
        let statement = client.get_prepared_statement(&DELETE_STATEMENT).await?;

        client
            .execute_unpaged(&statement, (protein.get_accession(),))
            .await?;

        Ok(())
    }

    /// Searches for a protein by accession or gene name. Gene name needs to be exact,
    /// attribute is wrapped in a like-query.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `attribute` - Accession or gene name, will be wrapped in a like-query
    pub async fn search<'a>(
        client: Arc<Client>,
        attribute: String,
    ) -> Result<impl Stream<Item = Result<Protein>> + 'a> {
        Ok(try_stream! {

            for await protein in Self::select(
                client.as_ref(),
                "WHERE accession = ?",
                &[&CqlValue::Text(attribute.clone())],
            ).await? {
                yield protein?;
            }

            for await protein in Self::select(
                client.as_ref(),
                "WHERE genes contains ?",
                &[&CqlValue::Text(attribute.clone())],
            ).await? {
                yield protein?;
            }
        })
    }

    /// get proteins of peptide
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `peptide` - The peptide
    pub async fn get_proteins_of_peptide<'a>(
        client: &'a Client,
        peptide: &'a Peptide,
    ) -> Result<impl Stream<Item = Result<Protein>> + 'a> {
        Ok(try_stream! {
            for await protein in Self::select(
                client,
                "WHERE accession IN ?",
                &[&CqlValue::List(
                    peptide.get_proteins().iter().map(|x| CqlValue::Text(x.to_owned())).collect(),
                )],
            ).await? {
                yield protein?;
            }
        })
    }

    /// Selects proteins from the database ans streams them back.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `additional` - Additional statement to add to the select statement, e.g. WHERE clause
    /// * `params` - Parameters for the additional statement
    ///
    pub async fn select(
        client: &Client,
        additional: &str,
        params: &[&CqlValue],
    ) -> Result<impl Stream<Item = Result<Protein>>> {
        let statement = format!("{} {};", SELECT_STATEMENT.as_str(), additional);
        let prepared_statement = client.get_prepared_statement(&statement).await?;

        let stream = client
            .execute_iter(prepared_statement, params)
            .await?
            .rows_stream::<Protein>()?;

        Ok(try_stream! {
            for await protein_result in stream  {
                yield protein_result?.into();
            }
        })
    }
}

impl Table for ProteinTable {
    fn table_name() -> &'static str {
        TABLE_NAME
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use fallible_iterator::FallibleIterator;
    use futures::TryStreamExt;
    use serial_test::serial;
    use tokio::pin;

    use super::*;
    use crate::chemistry::amino_acid::calc_sequence_mass_int;
    use crate::database::generic_client::GenericClient;
    use crate::database::scylla::client::Client;
    use crate::database::scylla::prepare_database_for_tests;
    use crate::database::scylla::tests::DATABASE_URL;
    #[cfg(feature = "domains")]
    use crate::entities::domain::Domain;
    use crate::io::uniprot_text::reader::Reader;

    const EXPECTED_PROTEINS: i64 = 3;

    lazy_static! {
        static ref TRYPSIN: Protein = Protein::new(
            "P07477".to_string(),
            [
                "A1A509",
                "A6NJ71",
                "B2R5I5",
                "Q5NV57",
                "Q7M4N3",
                "Q7M4N4",
                "Q92955",
                "Q9HAN4",
                "Q9HAN5",
                "Q9HAN6",
                "Q9HAN7"
            ].iter().map(|s| s.to_string()).collect(),
            "TRY1_HUMAN".to_string(),
            "Serine protease 1".to_string(),
            [
                "PRSS1",
                "TRP1",
                "TRY1",
                "TRYP1"
            ].iter().map(|s| s.to_string()).collect(),
            9606,
            "UP000005640".to_string(),
            true,
            "MNPLLILTFVAAALAAPFDDDDKIVGGYNCEENSVPYQVSLNSGYHFCGGSLINEQWVVSAGHCYKSRIQVRLGEHNIEVLEGNEQFINAAKIIRHPQYDRKTLNNDIMLIKLSSRAVINARVSTISLPTAPPATGTKCLISGWGNTASSGADYPDELQCLDAPVLSQAKCEASYPGKITSNMFCVGFLEGGKDSCQGDSGGPVVCNGQLQGVVSWGDGCAQKNKPGVYTKVYNYVKWIKNTIAANS".to_string(),
            1677024000,
            #[cfg(feature = "domains")] vec![],
        );

        static ref UPDATED_TRYPSIN: Protein = Protein::new(
            "UPDTRY".to_string(),
            [
                "P07477",
                "A1A509",
                "A6NJ71",
                "B2R5I5",
                "Q5NV57",
                "Q7M4N3",
                "Q7M4N4",
                "Q92955",
                "Q9HAN4",
                "Q9HAN5",
                "Q9HAN6",
                "Q9HAN7"
            ].iter().map(|s| s.to_string()).collect(),
            "UPD_TRY1_HUMAN".to_string(),
            "Some strange stuff which destroys other stuff".to_string(),
            [
                "SOME NEW UNDISCOVERED GENE",
                "PRSS1",
                "TRP1",
                "TRY1",
                "TRYP1"
            ].iter().map(|s| s.to_string()).collect(),
            0,
            "UP999999999".to_string(),
            true,
            "RUSTISAWESOME".to_string(),
            0,
            #[cfg(feature = "domains")] vec![Domain::new( 23, 243,  "Peptidase S1".to_string(),  "ECO:0000255|PROSITE-ProRule:PRU00274".to_string(), None, None, None, None)]
        );
    }

    #[tokio::test]
    #[serial]
    /// Prepares database for testing and inserts proteins from test file.
    ///
    async fn test_insert() {
        let client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }
        let count_statement = format!(
            "SELECT count(*) FROM {}.{}",
            client.get_database(),
            TABLE_NAME
        );
        let count = client
            .query_unpaged(count_statement, &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .first_row::<(i64,)>()
            .unwrap()
            .0;

        assert_eq!(count, EXPECTED_PROTEINS);
    }

    #[tokio::test]
    #[serial]
    /// Tests selects from database.
    ///
    async fn test_select() {
        let client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        let protein = ProteinTable::select(
            &client,
            "WHERE accession = ?",
            &[&CqlValue::Text(TRYPSIN.get_accession().to_owned())],
        )
        .await
        .unwrap()
        .try_collect::<Vec<Protein>>()
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(protein, TRYPSIN.to_owned());
    }

    #[tokio::test]
    #[serial]
    async fn test_update() {
        let client = Client::new(DATABASE_URL).await.unwrap();

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        ProteinTable::update(&client, &TRYPSIN, &UPDATED_TRYPSIN)
            .await
            .unwrap();

        let actual = ProteinTable::select(
            &client,
            "WHERE accession = ? ",
            &[&CqlValue::Text(UPDATED_TRYPSIN.get_accession().to_owned())],
        )
        .await
        .unwrap()
        .try_collect::<Vec<Protein>>()
        .await
        .unwrap()
        .pop()
        .unwrap();

        assert_eq!(actual, UPDATED_TRYPSIN.to_owned());
    }

    #[tokio::test]
    #[serial]
    async fn test_delete() {
        let client = Client::new(DATABASE_URL).await.unwrap();

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        ProteinTable::delete(&client, &TRYPSIN).await.unwrap();

        let stream = ProteinTable::select(
            &client,
            "WHERE accession = ? ",
            &[&CqlValue::Text(TRYPSIN.get_accession().to_owned())],
        )
        .await
        .unwrap();

        pin!(stream);

        let row_opt = stream.try_next().await.unwrap();

        assert!(row_opt.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn test_get_proteins_of_peptides() {
        let client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        let mut reader = Reader::new(Path::new("test_files/mouse.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        // LDDTERKIK from test_files/mouse.txt
        let peptide = Peptide::new(
            0,
            calc_sequence_mass_int("LDDTERKIK").unwrap(),
            "LDDTERKIK".to_string(),
            2,
            vec![
                "A0A087WRS6".to_string(),
                "G5E886".to_string(),
                "G5E887".to_string(),
            ],
            true,
            true,
            vec![10090],
            vec![],
            vec!["UP000000589".to_string()],
            #[cfg(feature = "domains")]
            vec![],
        )
        .unwrap();

        // Fetch all proteins of the peptide
        let protein_stream = ProteinTable::get_proteins_of_peptide(&client, &peptide)
            .await
            .unwrap();
        pin!(protein_stream);
        let proteins = protein_stream.try_collect::<Vec<Protein>>().await.unwrap();

        // Check if all proteins of the peptide are in the fetched proteins
        assert_eq!(proteins.len(), peptide.get_proteins().len());

        let proteins_map: HashMap<String, Protein> = proteins
            .iter()
            .map(|protein| (protein.get_accession().to_owned(), protein.to_owned()))
            .collect();

        for expected_protein in peptide.get_proteins().iter() {
            assert!(proteins_map.contains_key(expected_protein))
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_tokenawareness() {
        let client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;
        for statement in &[INSERT_STATEMENT.as_str(), DELETE_STATEMENT.as_str()] {
            let prepared_statement = client.get_prepared_statement(statement).await.unwrap();
            assert!(
                prepared_statement.is_token_aware(),
                "Statement `{}` is not token aware",
                statement
            );
        }
    }
}
