use anyhow::Result;
use scylla::batch::Batch;
use scylla::frame::response::result::{CqlValue, Row};
use scylla::transport::errors::QueryError;
use scylla::transport::iterator::RowIterator;
use scylla::transport::query_result::FirstRowError;

// internal imports
use crate::database::selectable_table::SelectableTable as SelectableTableTrait;
use crate::database::table::Table;
use crate::entities::protein::Protein;

use super::client::GenericClient;
use super::SCYLLA_KEYSPACE_NAME;

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
            SCYLLA_KEYSPACE_NAME,
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
            SCYLLA_KEYSPACE_NAME, TABLE_NAME
        );
        let insert_statement = format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            SCYLLA_KEYSPACE_NAME,
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
            SCYLLA_KEYSPACE_NAME, TABLE_NAME
        );

        client
            .get_session()
            .query(statement, (protein.get_accession(),))
            .await?;

        return Ok(());
    }
}

impl Table for ProteinTable {
    fn table_name() -> &'static str {
        TABLE_NAME
    }
}

impl<'a, C> SelectableTableTrait<'a, C> for ProteinTable
where
    C: GenericClient + Send + Sync,
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
            SCYLLA_KEYSPACE_NAME,
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
            SCYLLA_KEYSPACE_NAME,
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

    async fn raw_stream<'b>(
        client: &'a C,
        cols: &str,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Self::RecordIter> {
        let session = client.get_session();
        let mut statement = format!(
            "SELECT {} FROM {}.{}",
            cols,
            SCYLLA_KEYSPACE_NAME,
            Self::table_name()
        );
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }

        return Ok(session.query_iter(statement, params).await?);
    }
}

#[cfg(test)]
mod tests {
    // std imports
    use std::path::Path;

    // external imports
    use fallible_iterator::FallibleIterator;
    use scylla::transport::query_result::FirstRowError;
    use serial_test::serial;

    // internal imports
    use super::*;
    use crate::database::scylla::client::GenericClient;
    use crate::database::scylla::{get_client, prepare_database_for_tests};
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
            "Serine protease 1 {ECO:0000312|HGNC:HGNC:9475}".to_string(),
            [
                "PRSS1 {ECO:0000312|HGNC:HGNC:9475}",
                "TRP1",
                "TRY1 {ECO:0000312|HGNC:HGNC:9475}",
                "TRYP1"
            ].iter().map(|s| s.to_string()).collect(),
            9606,
            "UP000005640".to_string(),
            true,
            "MNPLLILTFVAAALAAPFDDDDKIVGGYNCEENSVPYQVSLNSGYHFCGGSLINEQWVVSAGHCYKSRIQVRLGEHNIEVLEGNEQFINAAKIIRHPQYDRKTLNNDIMLIKLSSRAVINARVSTISLPTAPPATGTKCLISGWGNTASSGADYPDELQCLDAPVLSQAKCEASYPGKITSNMFCVGFLEGGKDSCQGDSGGPVVCNGQLQGVVSWGDGCAQKNKPGVYTKVYNYVKWIKNTIAANS".to_string(),
            1677024000,
            Vec::new()
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
                "PRSS1 {ECO:0000312|HGNC:HGNC:9475}",
                "TRP1",
                "TRY1 {ECO:0000312|HGNC:HGNC:9475}",
                "TRYP1"
            ].iter().map(|s| s.to_string()).collect(),
            0,
            "UP999999999".to_string(),
            true,
            "RUSTISAWESOME".to_string(),
            0,
            Vec::new()
        );
    }

    #[tokio::test]
    #[serial]
    /// Prepares database for testing and inserts proteins from test file.
    ///
    async fn test_insert() {
        let client = get_client(None).await.unwrap();
        prepare_database_for_tests(&client).await;

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }
        let count_statement = format!(
            "SELECT count(*) FROM {}.{}",
            SCYLLA_KEYSPACE_NAME, TABLE_NAME
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
        let mut client = get_client(None).await.unwrap();
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
        let mut client = get_client(None).await.unwrap();

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
        let mut client = get_client(None).await.unwrap();

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
