// std imports
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use async_stream::try_stream;
use futures::Stream;
use scylla::batch::Batch;
use scylla::frame::response::result::{CqlValue, Row};
use scylla::transport::errors::QueryError;
use scylla::transport::iterator::RowIterator;
use scylla::transport::query_result::FirstRowError;

// internal imports
use crate::database::selectable_table::SelectableTable as SelectableTableTrait;
use crate::database::table::Table;
use crate::entities::peptide::Peptide;
use crate::entities::protein::Protein;

use super::client::GenericClient;

const TABLE_NAME: &'static str = "proteins";

const SELECT_COLS: &'static str = "accession, secondary_accessions, entry_name, name, \
    genes, taxonomy_id, proteome_id, is_reviewed, sequence, updated_at, domains";

const INSERT_COLS: &'static str = SELECT_COLS;

const UPDATE_COLS: &'static str = "secondary_accessions, entry_name, name, \
    genes, taxonomy_id, proteome_id, is_reviewed, sequence, updated_at, domains";

lazy_static! {
    static ref INSERT_PLACEHOLDERS: String = INSERT_COLS
        .split(", ",)
        .enumerate()
        .map(|(_, _)| "?")
        .collect::<Vec<&str>>()
        .join(", ");
    static ref UPDATE_SET_PLACEHOLDER: String = UPDATE_COLS
        .split(", ",)
        .enumerate()
        .map(|(_, col)| format!("{} = ?", col))
        .collect::<Vec<String>>()
        .join(", ");
}

pub struct ProteinTable {}

impl ProteinTable {
    pub async fn insert<'a, C: GenericClient>(client: &C, protein: &Protein) -> Result<()> {
        let statement = format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            client.get_database(),
            TABLE_NAME,
            INSERT_COLS,
            INSERT_PLACEHOLDERS.as_str()
        );
        client
            .get_session()
            .query(
                statement,
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
        return Ok(());
    }

    pub async fn update<'a, C: GenericClient>(
        client: &C,
        old_prot: &Protein,
        updated_prot: &Protein,
    ) -> Result<()> {
        let mut batch: Batch = Batch::default();

        let delete_statement = format!(
            "DELETE FROM {}.{} WHERE accession = ?",
            client.get_database(),
            TABLE_NAME
        );
        let insert_statement = format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            client.get_database(),
            TABLE_NAME,
            INSERT_COLS,
            INSERT_PLACEHOLDERS.as_str()
        );

        batch.append_statement(delete_statement.as_str());
        batch.append_statement(insert_statement.as_str());

        client
            .get_session()
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

        return Ok(());
    }

    pub async fn delete<'a, C: GenericClient>(client: &C, protein: &Protein) -> Result<()> {
        let statement = format!(
            "DELETE FROM {}.{} WHERE accession = ?",
            client.get_database(),
            TABLE_NAME
        );

        client
            .get_session()
            .query(statement, (protein.get_accession(),))
            .await?;

        return Ok(());
    }

    /// Searches for a protein by accession or gene name. Gene name needs to be exact,
    /// attribute is wrapped in a like-query.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `attribute` - Accession or gene name, will be wrapped in a like-query
    pub async fn search<'a, C>(
        client: Arc<C>,
        attribute: String,
    ) -> Result<impl Stream<Item = Result<Protein>> + 'a>
    where
        C: GenericClient + Unpin + 'a,
    {
        Ok(try_stream! {


            let like_attribute = format!("%{}%", &attribute);
            let attr_cql_value = CqlValue::Text(like_attribute);
            let param = vec![
                &attr_cql_value
            ];


            for await protein in ProteinTable::stream(
                client.as_ref(),
                "WHERE accession LIKE ? ALLOW FILTERING",
                param.as_slice(),
                10000,
            )
            .await? {
                yield protein?;
            }

            let attr_cql_value = CqlValue::Text(attribute);
            let param = vec![
                &attr_cql_value
            ];

            for await protein in ProteinTable::stream(
                client.as_ref(),
                "WHERE genes CONTAINS ? ALLOW FILTERING",
                param.as_slice(),
                10000,
            )
            .await? {
                yield protein?;
            }
        })
    }

    /// get proteins of peptide
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `peptide` - The peptide
    pub async fn get_proteins_of_peptide<'a, C>(
        client: &'a C,
        peptide: &'a Peptide,
    ) -> Result<impl Stream<Item = Result<Protein>> + 'a>
    where
        C: GenericClient + Unpin + 'a,
    {
        Ok(try_stream! {
            let accession_placeholder = peptide
                .get_proteins()
                .iter()
                .map(|_| "?")
                .collect::<Vec<&str>>()
                .join(",");

            let statement_addition = format!("WHERE accession IN ({})", accession_placeholder.as_str());

            let params: Vec<CqlValue> = peptide.get_proteins().iter().map(|accession| CqlValue::Text(accession.to_owned())).collect();
            let param_refs: Vec<&CqlValue> = params.iter().collect();

            for await protein in Self::stream(
                client,
                &statement_addition,
                param_refs.as_slice(),
                10000
            ).await? {
                yield protein?;
            }
        })
    }
}

impl Table for ProteinTable {
    fn table_name() -> &'static str {
        TABLE_NAME
    }
}

impl<'a, C> SelectableTableTrait<'a, C> for ProteinTable
where
    C: GenericClient + Send + Sync + Unpin + 'a,
{
    type Parameter = CqlValue;
    type Record = Row;
    type RecordIter = RowIterator;
    type RecordIterErr = QueryError;
    type Entity = Protein;

    fn select_cols() -> &'static str {
        SELECT_COLS
    }

    async fn raw_select_multiple<'b>(
        client: &C,
        cols: &str,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Vec<Self::Record>> {
        let session = client.get_session();
        let mut statement = format!(
            "SELECT {} FROM {}.{}",
            cols,
            client.get_database(),
            Self::table_name(),
        );
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        return Ok(session.query(statement, params).await?.rows()?);
    }

    async fn raw_select<'b>(
        client: &C,
        cols: &str,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Option<Self::Record>> {
        let session = client.get_session();

        let mut statement = format!(
            "SELECT {} FROM {}.{}",
            cols,
            client.get_database(),
            Self::table_name(),
        );
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }

        let row_res = session.query(statement, params).await?;

        match row_res.first_row() {
            Ok(row) => Ok(Some(row)),
            Err(FirstRowError::RowsEmpty) => Ok(None),
            Err(FirstRowError::RowsExpected(err)) => Err(err.into()),
        }
    }

    async fn select_multiple<'b>(
        client: &C,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Vec<Self::Entity>> {
        let rows: Vec<Row> = Self::raw_select_multiple(
            client,
            <Self as SelectableTableTrait<C>>::select_cols(),
            additional,
            params,
        )
        .await?;
        let mut records = Vec::new();
        for row in rows {
            records.push(Self::Entity::from(row));
        }
        return Ok(records);
    }

    async fn select<'b>(
        client: &C,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Option<Self::Entity>> {
        let row = Self::raw_select(
            client,
            <Self as SelectableTableTrait<C>>::select_cols(),
            additional,
            params,
        )
        .await?;

        if row.is_none() {
            return Ok(None);
        }
        return Ok(Some(Self::Entity::from(row.unwrap())));
    }
    async fn raw_stream(
        client: &'a C,
        cols: &str,
        additional: &str,
        params: &'a [&'a Self::Parameter],
        num_rows: i32,
    ) -> Result<impl Stream<Item = Result<Self::Record>>> {
        let mut statement = format!(
            "SELECT {} FROM {}.{}",
            cols,
            client.get_database(),
            Self::table_name()
        );
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        Ok(try_stream! {
            let mut prepared_statement = client.get_session().prepare(statement).await?;
            prepared_statement.set_page_size(num_rows);
            let row_stream = client.get_session().execute_iter(prepared_statement, params).await?;
            for await row in row_stream {
                yield row?;
            }
        })
    }

    async fn stream(
        client: &'a C,
        additional: &'a str,
        params: &'a [&'a Self::Parameter],
        num_rows: i32,
    ) -> Result<impl Stream<Item = Result<Self::Entity>>> {
        Ok(try_stream! {
            for await row in Self::raw_stream(
                client,
                <Self as SelectableTableTrait<C>>::select_cols(),
                additional,
                params,
                num_rows,
            ).await? {
                yield Self::Entity::from(row?);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    // std imports
    use std::path::Path;

    // external imports
    use fallible_iterator::FallibleIterator;
    use serial_test::serial;

    // internal imports
    use super::*;
    use crate::database::scylla::client::Client;
    use crate::database::scylla::prepare_database_for_tests;
    use crate::database::scylla::tests::DATABASE_URL;
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
        let row = client
            .get_session()
            .query(count_statement, &[])
            .await
            .unwrap()
            .first_row()
            .unwrap();

        let count = row
            .columns
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .as_bigint()
            .unwrap();

        assert_eq!(count, EXPECTED_PROTEINS);
    }

    #[tokio::test]
    #[serial]
    /// Tests selects from database.
    ///
    async fn test_select() {
        let mut client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&client).await;

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        let protein = ProteinTable::select(
            &mut client,
            "WHERE accession = ? ",
            &[&CqlValue::Text(TRYPSIN.get_accession().to_owned())],
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(protein, TRYPSIN.to_owned());
    }

    #[tokio::test]
    #[serial]
    async fn test_update() {
        let mut client = Client::new(DATABASE_URL).await.unwrap();

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        ProteinTable::update(&mut client, &TRYPSIN, &UPDATED_TRYPSIN)
            .await
            .unwrap();

        let actual = ProteinTable::select(
            &mut client,
            "WHERE accession = ? ",
            &[&CqlValue::Text(UPDATED_TRYPSIN.get_accession().to_owned())],
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(actual, UPDATED_TRYPSIN.to_owned());
    }

    #[tokio::test]
    #[serial]
    async fn test_delete() {
        let mut client = Client::new(DATABASE_URL).await.unwrap();

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }

        ProteinTable::delete(&mut client, &TRYPSIN).await.unwrap();

        let row_opt = ProteinTable::raw_select(
            &mut client,
            SELECT_COLS,
            "WHERE accession = ? ",
            &[&CqlValue::Text(TRYPSIN.get_accession().to_owned())],
        )
        .await
        .unwrap();

        assert!(row_opt.is_none());
    }
}
