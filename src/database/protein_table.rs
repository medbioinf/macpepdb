use std::pin::{pin, Pin};
use std::sync::{Arc, LazyLock};

use anyhow::Result;
use async_stream::try_stream;
use futures::Stream;
use tokio_postgres::types::BorrowToSql;
use tokio_postgres::Row;

use crate::database::errors::protein_table_error::ProteinTableError;
use crate::database::table::Table;
use crate::entities::peptide::Peptide;
use crate::entities::protein::Protein;

use super::client::Client;

const TABLE_NAME: &str = "proteins";

const ACCESSION_COL: &str = "accession";
const SECONDARY_ACCESSIONS_COL: &str = "secondary_accessions";
const ENTRY_NAME_COL: &str = "entry_name";
const NAME_COL: &str = "name";
const GENES_COL: &str = "genes";
const TAXONOMY_ID_COL: &str = "taxonomy_id";
const PROTEOME_ID_COL: &str = "proteome_id";
const IS_REVIEWED_COL: &str = "is_reviewed";
const SEQUENCE_COL: &str = "sequence";
const UPDATED_AT_COL: &str = "updated_at";

const SELECT_COLS: [&str; 10] = [
    ACCESSION_COL,
    SECONDARY_ACCESSIONS_COL,
    ENTRY_NAME_COL,
    NAME_COL,
    GENES_COL,
    TAXONOMY_ID_COL,
    PROTEOME_ID_COL,
    IS_REVIEWED_COL,
    SEQUENCE_COL,
    UPDATED_AT_COL,
];

const INSERT_COLS: [&str; 10] = SELECT_COLS;

static SELECT_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("SELECT {} FROM {TABLE_NAME}", SELECT_COLS.join(", ")));

static INSERT_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "INSERT INTO {TABLE_NAME} ({}) VALUES ({})",
        INSERT_COLS.join(", "),
        INSERT_COLS
            .iter()
            .enumerate()
            .map(|(i, _)| format!("${}", i + 1))
            .collect::<Vec<String>>()
            .join(", ")
    )
});

static UPDATE_STATEMENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "UPDATE {TABLE_NAME} SET {} WHERE {ACCESSION_COL} = ${}",
        INSERT_COLS
            .iter()
            .enumerate()
            .map(|(i, col)| format!("{} = ${}", col, i + 1))
            .collect::<Vec<String>>()
            .join(", "),
        INSERT_COLS.len() + 1
    )
});

static DELETE_STATEMENT: LazyLock<String> =
    LazyLock::new(|| format!("DELETE FROM {TABLE_NAME} WHERE {ACCESSION_COL} = $1"));

impl From<Row> for Protein {
    fn from(row: Row) -> Self {
        Protein::new(
            row.get(ACCESSION_COL),
            row.get(SECONDARY_ACCESSIONS_COL),
            row.get(ENTRY_NAME_COL),
            row.get(NAME_COL),
            row.get(GENES_COL),
            row.get(TAXONOMY_ID_COL),
            row.get(PROTEOME_ID_COL),
            row.get(IS_REVIEWED_COL),
            row.get(SEQUENCE_COL),
            row.get(UPDATED_AT_COL),
            vec![], // Domains are not stored in this table
        )
    }
}

pub type FallibleProteinStream = Pin<Box<dyn Stream<Item = Result<Protein>> + Send>>;

pub struct ProteinTable {}

impl ProteinTable {
    pub async fn insert(client: &Client, protein: &Protein) -> Result<(), ProteinTableError> {
        let conn = client.get().await?;

        let prepared_statement = conn.prepare_cached(INSERT_STATEMENT.as_str()).await?;

        conn.execute(
            &prepared_statement,
            &[
                &protein.get_accession(),
                &protein.get_secondary_accessions(),
                &protein.get_entry_name(),
                &protein.get_name(),
                &protein.get_genes(),
                &protein.get_taxonomy_id(),
                &protein.get_proteome_id(),
                &protein.get_is_reviewed(),
                &protein.get_sequence(),
                &protein.get_updated_at(),
            ],
        )
        .await?;
        Ok(())
    }

    pub async fn update(
        client: &Client,
        old_prot: &Protein,
        updated_prot: &Protein,
    ) -> Result<(), ProteinTableError> {
        let conn = client.get().await?;

        let statement = conn.prepare_cached(UPDATE_STATEMENT.as_str()).await?;

        conn.execute(
            &statement,
            &[
                &updated_prot.get_accession(),
                &updated_prot.get_secondary_accessions(),
                &updated_prot.get_entry_name(),
                &updated_prot.get_name(),
                &updated_prot.get_genes(),
                &updated_prot.get_taxonomy_id(),
                &updated_prot.get_proteome_id(),
                &updated_prot.get_is_reviewed(),
                &updated_prot.get_sequence(),
                &updated_prot.get_updated_at(),
                &old_prot.get_accession(),
            ],
        )
        .await?;

        Ok(())
    }

    pub async fn delete(client: &Client, protein: &Protein) -> Result<(), ProteinTableError> {
        let conn = client.get().await?;
        let statement = conn.prepare_cached(DELETE_STATEMENT.as_str()).await?;

        conn.execute(&statement, &[protein.get_accession()]).await?;

        Ok(())
    }

    /// Selects proteins from the database ans streams them back.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `additional` - Additional statement to add to the select statement, e.g. WHERE clause. Placeholders ($1, $2, ...) can be used here (1-based indexing).
    /// * `params` - Parameters for the additional statement
    ///
    pub async fn select<'a, P, I>(
        client: &'a Client,
        additional: &'a str,
        params: I,
    ) -> Result<impl Stream<Item = Result<Protein>> + 'a, ProteinTableError>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = P> + 'a,
        I::IntoIter: ExactSizeIterator,
    {
        Ok(try_stream! {
            let statement = format!("{} {};", SELECT_STATEMENT.as_str(), additional);

            let conn = client.get().await?;
            let prepared_statement = conn.prepare_cached(&statement).await?;

            let stream = conn.query_raw(&prepared_statement, params).await?;
            let stream = pin!(stream);

            for await protein_result in stream  {
                yield protein_result?.into();
            }
        })
    }

    /// Searches for a protein by accession or gene name. Gene name needs to be exact,
    /// attribute is wrapped in a like-query.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `attribute` - Accession or gene name, will be wrapped in a like-query
    ///
    pub async fn search(client: Arc<Client>, attribute: String) -> Result<FallibleProteinStream> {
        Ok(Box::pin(try_stream! {


            let accession_search_attributes = &[attribute];
            for await protein in Self::select(
                client.as_ref(),
                "WHERE accession = $1",
                accession_search_attributes,
            ).await? {
                yield protein?;
            }

            let gene_search_attributes = &[&[accession_search_attributes]];
            for await protein in Self::select(
                client.as_ref(),
                "WHERE genes @> $1",
                gene_search_attributes,
            ).await? {
                yield protein?;
            }
        }))
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
            for proteins in peptide.get_proteins().as_slice().chunks(100) {

                let select_params = &[proteins];
                for await protein in Self::select(
                    client,
                    "WHERE accession = ANY($1)",
                    select_params,
                ).await? {
                    yield protein?;
                }
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

    use super::*;
    use crate::chemistry::amino_acid::calc_sequence_mass_int;
    use crate::database::client::tests::get_test_client;

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
            vec![]
        );
    }

    #[tokio::test]
    #[serial]
    /// Prepares database for testing and inserts proteins from test file.
    ///
    async fn test_insert() {
        let client = get_test_client().await;

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        let conn = client.get().await.unwrap();
        let count_statement = format!("SELECT count(*) FROM {TABLE_NAME}");
        let count: i64 = conn
            .query_one(&count_statement, &[])
            .await
            .unwrap()
            .get("count");

        assert_eq!(count, EXPECTED_PROTEINS);
    }

    #[tokio::test]
    #[serial]
    /// Tests selects from database.
    ///
    async fn test_select() {
        let client = get_test_client().await;

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        let protein =
            ProteinTable::select(&client, "WHERE accession = $1", &[TRYPSIN.get_accession()])
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
        let client = get_test_client().await;

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        ProteinTable::update(&client, &TRYPSIN, &UPDATED_TRYPSIN)
            .await
            .unwrap();

        let actual = ProteinTable::select(
            &client,
            "WHERE accession = $1",
            &[UPDATED_TRYPSIN.get_accession()],
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
        let client = get_test_client().await;

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        ProteinTable::delete(&client, &TRYPSIN).await.unwrap();

        let select_params = &[TRYPSIN.get_accession()];
        let stream = ProteinTable::select(&client, "WHERE accession = $1", select_params)
            .await
            .unwrap();

        let mut stream = pin!(stream);

        let row_opt = stream.try_next().await.unwrap();

        assert!(row_opt.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn test_get_proteins_of_peptides() {
        let client = get_test_client().await;

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
}
