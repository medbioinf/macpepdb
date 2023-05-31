use anyhow::Result;
use scylla::Session;

// internal imports
use crate::database::selectable_table::SelectableTable as SelectableTableTrait;
use crate::database::table::Table;
use crate::entities::protein::Protein;

use super::SCYLLA_KEYSPACE_NAME;

const TABLE_NAME: &'static str = "proteins";

const SELECT_COLS: &'static str = "accession, secondary_accessions, entry_name, name, \
    genes, taxonomy_id, proteome_id, is_reviewed, sequence, updated_at";

const INSERT_COLS: &'static str = SELECT_COLS;

const UPDATE_COLS: &'static str = SELECT_COLS;

lazy_static! {
    static ref INSERT_PLACEHOLDERS: String = INSERT_COLS
        .split(", ",)
        .enumerate()
        .map(|(i, _)| "?")
        .collect::<Vec<&str>>()
        .join(", ");
    static ref UPDATE_SET_PLACEHOLDER: String = UPDATE_COLS
        .split(", ",)
        .enumerate()
        .map(|(i, col)| format!("{} = ?", col))
        .collect::<Vec<String>>()
        .join(", ");
}

pub struct ProteinTable {}

impl ProteinTable {
    pub async fn insert<'a>(client: &Session, protein: &Protein) -> Result<()> {
        let statement = format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            SCYLLA_KEYSPACE_NAME,
            TABLE_NAME,
            INSERT_COLS,
            INSERT_PLACEHOLDERS.as_str()
        );
        client
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
                ),
            )
            .await?;
        return Ok(());
    }

    pub async fn update<'a>(
        client: &Session,
        old_prot: &Protein,
        updated_prot: &Protein,
    ) -> Result<()> {
        let statement = format!(
            "UPDATE {}.{} SET {} WHERE accession = ?",
            SCYLLA_KEYSPACE_NAME,
            TABLE_NAME,
            UPDATE_SET_PLACEHOLDER.as_str(),
        );

        client
            .query(
                statement,
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
                    old_prot.get_accession(),
                ),
            )
            .await?;
        return Ok(());
    }

    pub async fn delete<'a>(client: &Session, protein: &Protein) -> Result<()> {
        let statement = format!(
            "DELETE FROM {}.{} WHERE accession = ?",
            SCYLLA_KEYSPACE_NAME, TABLE_NAME
        );

        client.query(statement, (protein.get_accession(),)).await?;
        return Ok(());
    }
}

impl Table for ProteinTable {
    fn table_name() -> &'static str {
        TABLE_NAME
    }
}

// impl<'a, C> SelectableTableTrait<'a, C> for ProteinTable
// where
//     C: GenericClient,
// {
//     type Parameter = (dyn ToSql + Sync);
//     type Record = Row;
//     type RecordIter = RowIter<'a>;
//     type Entity = Protein;

//     fn select_cols() -> &'static str {
//         SELECT_COLS
//     }

//     fn raw_select_multiple<'b>(
//         client: &mut C,
//         cols: &str,
//         additional: &str,
//         params: &[&'b Self::Parameter],
//     ) -> Result<Vec<Self::Record>> {
//         let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
//         if additional.len() > 0 {
//             statement += " ";
//             statement += additional;
//         }
//         return Ok(client.query(&statement, params)?);
//     }

//     fn raw_select<'b>(
//         client: &mut C,
//         cols: &str,
//         additional: &str,
//         params: &[&'b Self::Parameter],
//     ) -> Result<Option<Self::Record>> {
//         let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
//         if additional.len() > 0 {
//             statement += " ";
//             statement += additional;
//         }
//         return Ok(client.query_opt(&statement, params)?);
//     }

//     fn select_multiple<'b>(
//         client: &mut C,
//         additional: &str,
//         params: &[&'b Self::Parameter],
//     ) -> Result<Vec<Self::Entity>> {
//         let rows = Self::raw_select_multiple(
//             client,
//             <Self as SelectableTableTrait<C>>::select_cols(),
//             additional,
//             params,
//         )?;
//         let mut records = Vec::new();
//         for row in rows {
//             records.push(Self::Entity::from(row));
//         }
//         return Ok(records);
//     }

//     fn select<'b>(
//         client: &mut C,
//         additional: &str,
//         params: &[&'b Self::Parameter],
//     ) -> Result<Option<Self::Entity>> {
//         let row = Self::raw_select(
//             client,
//             <Self as SelectableTableTrait<C>>::select_cols(),
//             additional,
//             params,
//         )?;
//         if row.is_none() {
//             return Ok(None);
//         }
//         return Ok(Some(Self::Entity::from(row.unwrap())));
//     }

//     fn raw_stream<'b>(
//         client: &'a mut C,
//         cols: &str,
//         additional: &str,
//         params: &[&'b Self::Parameter],
//     ) -> Result<Self::RecordIter> {
//         let mut statement = format!("SELECT {} FROM {}", cols, Self::table_name());
//         if additional.len() > 0 {
//             statement += " ";
//             statement += additional;
//         }
//         return Ok(client.query_raw(&statement, params.iter().map(|param| param.borrow_to_sql()))?);
//     }
// }

#[cfg(test)]
mod tests {
    // std imports
    use std::path::Path;

    // external imports
    use fallible_iterator::FallibleIterator;
    use serial_test::serial;

    // internal imports
    use super::*;
    use crate::database::scylla::tests::{get_session, prepare_database_for_tests};
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

    #[tokio::test]
    #[serial]
    /// Prepares database for testing and inserts proteins from test file.
    ///
    async fn test_insert() {
        let session = get_session().await;
        prepare_database_for_tests(&session).await;

        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            ProteinTable::insert(&session, &protein).await.unwrap();
        }
        let count_statement = format!(
            "SELECT count(*) FROM {}.{}",
            SCYLLA_KEYSPACE_NAME, TABLE_NAME
        );
        let row = session
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

    // #[test]
    // #[serial]
    // /// Tests selects from database.
    // ///
    // fn test_select() {
    //     test_insert();
    //     let mut client = get_client();

    //     let row = ProteinTable::raw_select(
    //         &mut client,
    //         SELECT_COLS,
    //         "WHERE accession = $1 ",
    //         &[TRYPSIN.get_accession()],
    //     )
    //     .unwrap()
    //     .unwrap();

    //     assert_eq!(&row.get::<_, String>("accession"), TRYPSIN.get_accession());
    //     assert_eq!(
    //         &row.get::<_, Vec<String>>("secondary_accessions"),
    //         TRYPSIN.get_secondary_accessions()
    //     );
    //     assert_eq!(
    //         &row.get::<_, String>("entry_name"),
    //         TRYPSIN.get_entry_name()
    //     );
    //     assert_eq!(&row.get::<_, String>("name"), TRYPSIN.get_name());
    //     assert_eq!(&row.get::<_, Vec<String>>("genes"), TRYPSIN.get_genes());
    //     assert_eq!(&row.get::<_, i64>("taxonomy_id"), TRYPSIN.get_taxonomy_id());
    //     assert_eq!(
    //         &row.get::<_, String>("proteome_id"),
    //         TRYPSIN.get_proteome_id()
    //     );
    //     assert_eq!(row.get::<_, bool>("is_reviewed"), TRYPSIN.get_is_reviewed());
    //     assert_eq!(&row.get::<_, String>("sequence"), TRYPSIN.get_sequence());
    //     assert_eq!(row.get::<_, i64>("updated_at"), TRYPSIN.get_updated_at());
    //     client.close().unwrap();
    // }

    // #[test]
    // #[serial]
    // fn test_update() {
    //     test_insert();
    //     let mut client = get_client();
    //     ProteinTable::update(&mut client, &TRYPSIN, &UPDATED_TRYPSIN).unwrap();

    //     let row = ProteinTable::raw_select(
    //         &mut client,
    //         SELECT_COLS,
    //         "WHERE accession = $1 ",
    //         &[UPDATED_TRYPSIN.get_accession()],
    //     )
    //     .unwrap()
    //     .unwrap();

    //     assert_eq!(
    //         &row.get::<_, String>("accession"),
    //         UPDATED_TRYPSIN.get_accession()
    //     );
    //     assert_eq!(
    //         &row.get::<_, Vec<String>>("secondary_accessions"),
    //         UPDATED_TRYPSIN.get_secondary_accessions()
    //     );
    //     assert_eq!(
    //         &row.get::<_, String>("entry_name"),
    //         UPDATED_TRYPSIN.get_entry_name()
    //     );
    //     assert_eq!(&row.get::<_, String>("name"), UPDATED_TRYPSIN.get_name());
    //     assert_eq!(
    //         &row.get::<_, Vec<String>>("genes"),
    //         UPDATED_TRYPSIN.get_genes()
    //     );
    //     assert_eq!(
    //         &row.get::<_, i64>("taxonomy_id"),
    //         UPDATED_TRYPSIN.get_taxonomy_id()
    //     );
    //     assert_eq!(
    //         &row.get::<_, String>("proteome_id"),
    //         UPDATED_TRYPSIN.get_proteome_id()
    //     );
    //     assert_eq!(
    //         row.get::<_, bool>("is_reviewed"),
    //         UPDATED_TRYPSIN.get_is_reviewed()
    //     );
    //     assert_eq!(
    //         &row.get::<_, String>("sequence"),
    //         UPDATED_TRYPSIN.get_sequence()
    //     );
    //     assert_eq!(
    //         row.get::<_, i64>("updated_at"),
    //         UPDATED_TRYPSIN.get_updated_at()
    //     );
    //     client.close().unwrap();
    // }

    // #[test]
    // #[serial]
    // fn test_delete() {
    //     test_insert();
    //     let mut client = get_client();
    //     ProteinTable::delete(&mut client, &TRYPSIN).unwrap();

    //     let row_opt = ProteinTable::raw_select(
    //         &mut client,
    //         SELECT_COLS,
    //         "WHERE accession = $1 ",
    //         &[TRYPSIN.get_accession()],
    //     )
    //     .unwrap();

    //     assert!(row_opt.is_none());

    //     client.close().unwrap();
    // }
}
