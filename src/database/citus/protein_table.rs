
// 3rd party imports
use anyhow::Result;
use fallible_iterator::FallibleIterator;
use postgres::{Client, Row, RowIter, ToStatement};
use postgres::types::{BorrowToSql, ToSql};

// internal imports
use crate::entities::protein::Protein;

const TABLE_NAME: &'static str = "proteins";

const SELECT_COLS: &'static str = "accession, secondary_accessions, entry_name, name, \
    genes, taxonomy_id, proteome_id, is_reviewed, sequence, updated_at";

const INSERT_COLS: &'static str = SELECT_COLS;

const UPDATE_COLS: &'static str = SELECT_COLS;

lazy_static! {
    static ref INSERT_PLACEHOLDERS: String = INSERT_COLS.split(", ", )
        .enumerate()
        .map(|(i, _)| format!("${}", i + 1))
        .collect::<Vec<String>>()
        .join(", ");

    static ref UPDATE_SET_PLACEHOLDER: String = UPDATE_COLS.split(", ", )
        .enumerate()
        .map(|(i, col)| format!("{} = ${}", col, i + 1))
        .collect::<Vec<String>>()
        .join(", ");

    static ref UPDATE_COLS_WHERE_ACCESSION_NUM: usize = UPDATE_SET_PLACEHOLDER.matches("=").count() + 1; 
}


impl From<Row> for Protein {
    fn from(row: Row) -> Self {
        return Protein::new(
            row.get("accession"),
            row.get("secondary_accessions"),
            row.get("entry_name"),
            row.get("name"),
            row.get("genes"),
            row.get("taxonomy_id"),
            row.get("proteome_id"),
            row.get("is_reviewed"),
            row.get("sequence"),
            row.get("updated_at")
        );
    }
}

pub struct ProteinStream<'a>{
    inner_iter: RowIter<'a>
}

impl FallibleIterator for ProteinStream<'_> {
    type Item = Protein;
    type Error = anyhow::Error;

    fn next(&mut self) -> Result<Option<Self::Item>> {
        if let Some(row) = self.inner_iter.next()? {
            return Ok(Some(Protein::from(row)));
        }
        return Ok(None);
    }
}

pub struct ProteinTable<'a> {
    client: &'a mut Client
}

impl<'a> ProteinTable<'a> {
    /// Creates a new instance of the ProteinTable
    pub fn new(client: &'a mut Client) -> Self {
        return ProteinTable {
            client
        };
    }

    /// Selects proteins and returns them as rows.
    /// 
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// 
    pub fn raw_select_multiple(&mut self, cols: &str, additional: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>> {
        let mut statement = format!("SELECT {} FROM {}", cols, TABLE_NAME);
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        return Ok(self.client.query(&statement, params)?);
    }

    /// Selects a protein and returns it as row. If no protein is found, None is returned.
    /// 
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// 
    pub fn raw_select(&mut self, cols: &str, additional: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<Row>> {
        let mut statement = format!("SELECT {} FROM {}", cols, TABLE_NAME);
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        return Ok(self.client.query_opt(&statement, params)?);
    }

    /// Selects proteins and returns them.
    /// 
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// 
    pub fn select_multiple(&mut self, additional: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Protein>> {
        let rows = self.raw_select_multiple(SELECT_COLS, additional, params)?;
        let mut proteins = Vec::new();
        for row in rows {
            proteins.push(Protein::from(row));
        }
        return Ok(proteins);
    }

    /// Selects a protein and returns it. If no protein is found, None is returned.
    /// 
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// 
    pub fn select(&mut self, additional: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<Protein>> {
        let row = self.raw_select(SELECT_COLS, additional, params)?;
        if row.is_none() {
            return Ok(None);
        }
        return Ok(Some(Protein::from(row.unwrap())));
    }

    /// Selects proteins and returns it them row iterator.
    /// 
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// 
    pub fn raw_stream<T, P, I>(&mut self, cols: &str, additional: &str, params: I) -> Result<RowIter<'_>>
        where T: ?Sized + ToStatement, P: BorrowToSql, I : IntoIterator<Item = P>, I::IntoIter: ExactSizeIterator {
        let mut statement = format!("SELECT {} FROM {}", cols, TABLE_NAME);
        if additional.len() > 0 {
            statement += " ";
            statement += additional;
        }
        return Ok(self.client.query_raw(&statement, params)?);
    }

    /// Selects proteins and returns them as iterator.
    /// 
    /// # Arguments
    /// * `cols` - The columns to select
    /// * `additional` - Additional SQL to add to the query , e.g "WHERE accession = $1"
    /// * `params` - The parameters to use in the query
    /// 
    pub fn stream<T, P, I>(&mut self, additional: &str, params: I) -> Result<ProteinStream<'_>>
        where T: ?Sized + ToStatement, P: BorrowToSql, I : IntoIterator<Item = P>, I::IntoIter: ExactSizeIterator {
        return Ok(ProteinStream {
            inner_iter: self.raw_stream::<T, P, I>(SELECT_COLS, additional, params)?
        });
    }

    pub fn insert(&mut self, protein: &Protein) -> Result<()> {
        let statement = format!(
            "INSERT INTO {} ({}) VALUES ({})", 
            TABLE_NAME,
            INSERT_COLS,
            INSERT_PLACEHOLDERS.as_str()
        );
        self.client.execute(&statement, &[
            protein.get_accession(),
            protein.get_secondary_accessions(),
            protein.get_entry_name(),
            protein.get_name(),
            protein.get_genes(),
            protein.get_taxonomy_id(),
            protein.get_proteome_id(),
            &protein.get_is_reviewed(),
            protein.get_sequence(),
            &protein.get_updated_at()
        ])?;
        return Ok(());
    }

    pub fn update(&mut self, old_prot: &Protein, updated_prot: &Protein) -> Result<()> {
        let statement = format!(
            "UPDATE {} SET {} WHERE accession = ${}",
            TABLE_NAME,
            UPDATE_SET_PLACEHOLDER.as_str(),
            UPDATE_COLS_WHERE_ACCESSION_NUM.to_string()
        );
        
        self.client.execute(&statement, &[
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
            old_prot.get_accession()
        ])?;
        return Ok(());
    }

    pub fn delete(&mut self, protein: &Protein) -> Result<()> {
        let statement = format!(
            "DELETE FROM {} WHERE accession = $1",
            TABLE_NAME
        );
        
        self.client.execute(&statement, &[
            protein.get_accession()
        ])?;
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    // std imports
    use std::path::Path;

    // external imports
    use serial_test::serial;

    // internal imports
    use crate::database::citus::tests::{prepare_database_for_tests, get_client};
    use crate::io::uniprot_text::reader::Reader;
    use super::*;

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

    #[test]
    #[serial]
    /// Prepares database for testing and inserts proteins from test file.
    /// 
    fn test_insert() {
        prepare_database_for_tests();
        let mut client = get_client();

        let mut protein_table = ProteinTable::new(&mut client);
        let mut reader = Reader::new(Path::new("test_files/uniprot.txt"), 1024).unwrap();
        while let Some(protein) = reader.next().unwrap() {
            protein_table.insert(&protein).unwrap();
        }
        let count_statement = format!("SELECT count(*) FROM {}", TABLE_NAME);
        let row = client.query_one(&count_statement, &[]).unwrap();
        assert_eq!(row.get::<usize, i64>(0), EXPECTED_PROTEINS);
        client.close().unwrap();
    }

    #[test]
    #[serial]
    /// Tests selects from database.
    /// 
    fn test_select() {
        test_insert();
        let mut client = get_client();

        let mut protein_table = ProteinTable::new(&mut client);
        let row = protein_table.raw_select(
            SELECT_COLS, 
            "WHERE accession = $1 ", 
            &[TRYPSIN.get_accession()]
        ).unwrap().unwrap();

        assert_eq!(&row.get::<_, String>("accession"), TRYPSIN.get_accession());
        assert_eq!(&row.get::<_, Vec<String>>("secondary_accessions"), TRYPSIN.get_secondary_accessions());
        assert_eq!(&row.get::<_, String>("entry_name"), TRYPSIN.get_entry_name());
        assert_eq!(&row.get::<_, String>("name"), TRYPSIN.get_name());
        assert_eq!(&row.get::<_, Vec<String>>("genes"), TRYPSIN.get_genes());
        assert_eq!(&row.get::<_, i64>("taxonomy_id"), TRYPSIN.get_taxonomy_id());
        assert_eq!(&row.get::<_, String>("proteome_id"), TRYPSIN.get_proteome_id());
        assert_eq!(row.get::<_, bool>("is_reviewed"), TRYPSIN.get_is_reviewed());
        assert_eq!(&row.get::<_, String>("sequence"), TRYPSIN.get_sequence());
        assert_eq!(row.get::<_, i64>("updated_at"), TRYPSIN.get_updated_at());
        client.close().unwrap();
    }

    #[test]
    #[serial]
    fn test_update() {
        test_insert();
        let mut client = get_client();
        let mut protein_table = ProteinTable::new(&mut client);
        protein_table.update(&TRYPSIN, &UPDATED_TRYPSIN).unwrap();
        
        let row = protein_table.raw_select(
            SELECT_COLS, 
            "WHERE accession = $1 ", 
            &[UPDATED_TRYPSIN.get_accession()]
        ).unwrap().unwrap();

        assert_eq!(&row.get::<_, String>("accession"), UPDATED_TRYPSIN.get_accession());
        assert_eq!(&row.get::<_, Vec<String>>("secondary_accessions"), UPDATED_TRYPSIN.get_secondary_accessions());
        assert_eq!(&row.get::<_, String>("entry_name"), UPDATED_TRYPSIN.get_entry_name());
        assert_eq!(&row.get::<_, String>("name"), UPDATED_TRYPSIN.get_name());
        assert_eq!(&row.get::<_, Vec<String>>("genes"), UPDATED_TRYPSIN.get_genes());
        assert_eq!(&row.get::<_, i64>("taxonomy_id"), UPDATED_TRYPSIN.get_taxonomy_id());
        assert_eq!(&row.get::<_, String>("proteome_id"), UPDATED_TRYPSIN.get_proteome_id());
        assert_eq!(row.get::<_, bool>("is_reviewed"), UPDATED_TRYPSIN.get_is_reviewed());
        assert_eq!(&row.get::<_, String>("sequence"), UPDATED_TRYPSIN.get_sequence());
        assert_eq!(row.get::<_, i64>("updated_at"), UPDATED_TRYPSIN.get_updated_at());
        client.close().unwrap();
    }

    #[test]
    #[serial]
    fn test_delete() {
        test_insert();
        let mut client = get_client();
        let mut protein_table = ProteinTable::new(&mut client);
        protein_table.delete(&TRYPSIN).unwrap();
        
        let row_opt = protein_table.raw_select(
            SELECT_COLS, 
            "WHERE accession = $1 ", 
            &[TRYPSIN.get_accession()]
        ).unwrap();

        assert!(row_opt.is_none());

        client.close().unwrap();
    }
}