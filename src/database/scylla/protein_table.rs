use std::sync::Arc;

use anyhow::Result;
use async_stream::try_stream;
use futures::Stream;
use scylla::statement::batch::Batch;
use scylla::value::CqlValue;

use super::client::Client;
use crate::database::generic_client::GenericClient;
use crate::database::table::Table;
use crate::entities::domain::Domain;
use crate::entities::peptide::Peptide;
use crate::entities::protein::Protein;

const TABLE_NAME: &str = "proteins";

const SELECT_COLS: [&str; 11] = [
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
    "domains",
];

const INSERT_COLS: [&str; 11] = SELECT_COLS;

lazy_static! {
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

/// Tuoel which represents a row of the protein table.
///
type TypedProteinRow = (
    String,
    Vec<String>,
    String,
    String,
    Vec<String>,
    i64,
    String,
    bool,
    String,
    i64,
    Vec<Domain>,
);

impl From<TypedProteinRow> for Protein {
    fn from(row: TypedProteinRow) -> Self {
        Protein::new(
            row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9, row.10,
        )
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
            // Scylla limiting the tuples per IN query. 100 per default.
            // Chunking by 99 to be on the safe side.
            for proteins in peptide.get_proteins().as_slice().chunks(99) {
                for await protein in Self::select(
                    client,
                    "WHERE accession IN ?",
                    &[&CqlValue::List(
                        proteins.iter().map(|x| CqlValue::Text(x.to_owned())).collect(),
                    )],
                ).await? {
                    yield protein?;
                }
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
            .rows_stream::<TypedProteinRow>()?;

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
    use crate::database::scylla::tests::get_test_database_url;
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
            vec![]

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
            vec![Domain::new( 23, 243,  "Peptidase S1".to_string(),  "ECO:0000255|PROSITE-ProRule:PRU00274".to_string(), None, None, None, None)]
        );
    }

    #[tokio::test]
    #[serial]
    /// Prepares database for testing and inserts proteins from test file.
    ///
    async fn test_insert() {
        let client = Client::new(&get_test_database_url()).await.unwrap();
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
        let client = Client::new(&get_test_database_url()).await.unwrap();
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
        let client = Client::new(&get_test_database_url()).await.unwrap();

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
        let client = Client::new(&get_test_database_url()).await.unwrap();

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
        let client = Client::new(&get_test_database_url()).await.unwrap();
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
        let client = Client::new(&get_test_database_url()).await.unwrap();
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
