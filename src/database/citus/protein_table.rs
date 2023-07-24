// 3rd party imports
use anyhow::Result;
use tokio_postgres::{
    types::{BorrowToSql, ToSql},
    GenericClient, Row, RowStream,
};

// internal imports
use crate::database::selectable_table::SelectableTable as SelectableTableTrait;
use crate::database::table::Table;
use crate::entities::protein::Protein;
use crate::tools::psql::convert_placeholders;

const TABLE_NAME: &'static str = "proteins";

const SELECT_COLS: &'static str = "accession, secondary_accessions, entry_name, name, \
    genes, taxonomy_id, proteome_id, is_reviewed, sequence, updated_at";

const INSERT_COLS: &'static str = SELECT_COLS;

const UPDATE_COLS: &'static str = SELECT_COLS;

lazy_static! {
    static ref INSERT_PLACEHOLDERS: String = INSERT_COLS
        .split(", ",)
        .enumerate()
        .map(|(i, _)| format!("${}", i + 1))
        .collect::<Vec<String>>()
        .join(", ");
    static ref UPDATE_SET_PLACEHOLDER: String = UPDATE_COLS
        .split(", ",)
        .enumerate()
        .map(|(i, col)| format!("{} = ${}", col, i + 1))
        .collect::<Vec<String>>()
        .join(", ");
    static ref UPDATE_COLS_WHERE_ACCESSION_NUM: usize =
        UPDATE_SET_PLACEHOLDER.matches("=").count() + 1;
}

pub struct ProteinTable {}

impl ProteinTable {
    pub async fn insert<'a, C: GenericClient>(client: &C, protein: &Protein) -> Result<()> {
        let statement = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            TABLE_NAME,
            INSERT_COLS,
            INSERT_PLACEHOLDERS.as_str()
        );
        client
            .execute(
                &statement,
                &[
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
                ],
            )
            .await?;
        return Ok(());
    }

    pub async fn update<'a, C: GenericClient>(
        client: &C,
        old_prot: &Protein,
        updated_prot: &Protein,
    ) -> Result<()> {
        let statement = format!(
            "UPDATE {} SET {} WHERE accession = ${}",
            TABLE_NAME,
            UPDATE_SET_PLACEHOLDER.as_str(),
            UPDATE_COLS_WHERE_ACCESSION_NUM.to_string()
        );

        client
            .execute(
                &statement,
                &[
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
                    old_prot.get_accession(),
                ],
            )
            .await?;
        return Ok(());
    }

    pub async fn delete<'a, C: GenericClient>(client: &C, protein: &Protein) -> Result<()> {
        let statement = format!("DELETE FROM {} WHERE accession = $1", TABLE_NAME);

        client
            .execute(&statement, &[protein.get_accession()])
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
    type Parameter = (dyn ToSql + Sync);
    type Record = Row;
    type RecordIterErr = tokio_postgres::Error;
    type RecordIter = RowStream;
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
        let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
        if additional.len() > 0 {
            statement += " ";
            statement += convert_placeholders(additional)?.as_str();
        }
        return Ok(client.query(&statement, params).await?);
    }

    async fn raw_select<'b>(
        client: &C,
        cols: &str,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Option<Self::Record>> {
        let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
        if additional.len() > 0 {
            statement += " ";
            statement += convert_placeholders(additional)?.as_str();
        }
        return Ok(client.query_opt(&statement, params).await?);
    }

    async fn select_multiple<'b>(
        client: &C,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Vec<Self::Entity>> {
        let rows = Self::raw_select_multiple(
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
        let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
        if additional.len() > 0 {
            statement += " ";
            statement += convert_placeholders(additional)?.as_str();
        }
        return Ok(client
            .query_raw(&statement, params.iter().map(|param| param.borrow_to_sql()))
            .await?);
    }
}

#[cfg(test)]
mod tests {
    // std imports
    use std::path::Path;

    // external imports
    use fallible_iterator::FallibleIterator;
    use serial_test::serial;
    use tracing::error;

    // internal imports
    use super::*;
    use crate::database::citus::tests::{get_client, prepare_database_for_tests};
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
            1677024000
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
            0
        );
    }

    /// Prepares database for testing and inserts proteins from test file.
    /// Unfortunately you cannot call an async test from another async test. So we need outsource the implementation,
    /// as other tests depends on it
    ///
    async fn test_insert_internal() {
        let (mut client, connection) = get_client().await;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });
        prepare_database_for_tests(&mut client).await;

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&client, &protein).await.unwrap();
        }
        let count_statement = format!("SELECT count(*) FROM {}", TABLE_NAME);
        let row = client.query_one(&count_statement, &[]).await.unwrap();
        assert_eq!(row.get::<usize, i64>(0), EXPECTED_PROTEINS);
        connection_handle.abort();
        let _ = connection_handle.await;
    }

    #[tokio::test]
    #[serial]
    /// Prepares database for testing and inserts proteins from test file.
    ///
    async fn test_insert() {
        test_insert_internal().await;
    }

    #[tokio::test]
    #[serial]
    /// Tests selects from database.
    ///
    async fn test_select() {
        test_insert_internal().await;
        let (client, connection) = get_client().await;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        let row = ProteinTable::raw_select(
            &client,
            SELECT_COLS,
            "WHERE accession = $1 ",
            &[TRYPSIN.get_accession()],
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(&row.get::<_, String>("accession"), TRYPSIN.get_accession());
        assert_eq!(
            &row.get::<_, Vec<String>>("secondary_accessions"),
            TRYPSIN.get_secondary_accessions()
        );
        assert_eq!(
            &row.get::<_, String>("entry_name"),
            TRYPSIN.get_entry_name()
        );
        assert_eq!(&row.get::<_, String>("name"), TRYPSIN.get_name());
        assert_eq!(&row.get::<_, Vec<String>>("genes"), TRYPSIN.get_genes());
        assert_eq!(&row.get::<_, i64>("taxonomy_id"), TRYPSIN.get_taxonomy_id());
        assert_eq!(
            &row.get::<_, String>("proteome_id"),
            TRYPSIN.get_proteome_id()
        );
        assert_eq!(row.get::<_, bool>("is_reviewed"), TRYPSIN.get_is_reviewed());
        assert_eq!(&row.get::<_, String>("sequence"), TRYPSIN.get_sequence());
        assert_eq!(row.get::<_, i64>("updated_at"), TRYPSIN.get_updated_at());

        connection_handle.abort();
        let _ = connection_handle.await;
    }

    #[tokio::test]
    #[serial]
    async fn test_update() {
        test_insert_internal().await;
        let (client, connection) = get_client().await;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        ProteinTable::update(&client, &TRYPSIN, &UPDATED_TRYPSIN)
            .await
            .unwrap();

        let row = ProteinTable::raw_select(
            &client,
            SELECT_COLS,
            "WHERE accession = $1 ",
            &[UPDATED_TRYPSIN.get_accession()],
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            &row.get::<_, String>("accession"),
            UPDATED_TRYPSIN.get_accession()
        );
        assert_eq!(
            &row.get::<_, Vec<String>>("secondary_accessions"),
            UPDATED_TRYPSIN.get_secondary_accessions()
        );
        assert_eq!(
            &row.get::<_, String>("entry_name"),
            UPDATED_TRYPSIN.get_entry_name()
        );
        assert_eq!(&row.get::<_, String>("name"), UPDATED_TRYPSIN.get_name());
        assert_eq!(
            &row.get::<_, Vec<String>>("genes"),
            UPDATED_TRYPSIN.get_genes()
        );
        assert_eq!(
            &row.get::<_, i64>("taxonomy_id"),
            UPDATED_TRYPSIN.get_taxonomy_id()
        );
        assert_eq!(
            &row.get::<_, String>("proteome_id"),
            UPDATED_TRYPSIN.get_proteome_id()
        );
        assert_eq!(
            row.get::<_, bool>("is_reviewed"),
            UPDATED_TRYPSIN.get_is_reviewed()
        );
        assert_eq!(
            &row.get::<_, String>("sequence"),
            UPDATED_TRYPSIN.get_sequence()
        );
        assert_eq!(
            row.get::<_, i64>("updated_at"),
            UPDATED_TRYPSIN.get_updated_at()
        );

        connection_handle.abort();
        let _ = connection_handle.await;
    }

    #[tokio::test]
    #[serial]
    async fn test_delete() {
        test_insert_internal().await;
        let (client, connection) = get_client().await;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        let connection_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("connection error: {}", e);
            }
        });

        ProteinTable::delete(&client, &TRYPSIN).await.unwrap();

        let row_opt = ProteinTable::raw_select(
            &client,
            SELECT_COLS,
            "WHERE accession = $1 ",
            &[TRYPSIN.get_accession()],
        )
        .await
        .unwrap();

        assert!(row_opt.is_none());
        connection_handle.abort();
        let _ = connection_handle.await;
    }
}
