// 3rd party imports
use anyhow::Result;
use postgres::{
    GenericClient,
    types::ToSql
};

// internal imports
use crate::entities::peptide::Peptide;
use crate::database::citus::table::Table;

const TABLE_NAME: &'static str = "peptides";

const SELECT_COLS: &'static str = "partition, mass, sequence, missed_cleavages, aa_counts, proteins, is_swiss_prot, is_trembl, taxonomy_ids, unique_taxonomy_ids, proteome_ids";

const INSERT_COLS: &'static str = SELECT_COLS;

const UPDATE_COLS: &'static str = SELECT_COLS;

const NUM_PRIMARY_KEY_COLS: usize = 3;

lazy_static! {
    pub static ref NUM_INSERT_COLS: usize = INSERT_COLS.split(", ", ).count();

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


pub struct PeptideTable {}

impl PeptideTable {
    fn create_chunked_array_with_placeholders(num_placeholders: usize, placeholders_per_chunk: usize, start: usize) -> String {
        let end = num_placeholders + start;
        // Generate vec of placeholders from `start` to `num_placeholders`
        let placeholders = (start..end).into_iter()
                .map(|i| format!("${}", i))
                .collect::<Vec<String>>();
        // Group placeholders into chunks of `placeholders_per_chunk` and join them with ", "
        // Result ($1, ..., $placeholders_per_chunk), ($placeholders_per_chunk + 1, ..., $placeholders_per_chunk * 2), ...
        placeholders
            .chunks(placeholders_per_chunk)
            .into_iter()
            .map(|chunk| format!("({})", chunk.join(", ")))
            .collect::<Vec<String>>()
            .join(", ")
    }

    pub fn bulk_insert<'a, C, T>(client: &mut C, peptides: T) -> Result<()> where 
        C: GenericClient,
        T: Iterator<Item=&'a Peptide> + ExactSizeIterator 
    {
        let num_placeholders = peptides.len() * *NUM_INSERT_COLS;
        // Generate vec of placeholders from $1 to $NUM_INSERT_COLS * num_placeholders
        let placeholders = Self::create_chunked_array_with_placeholders(
            num_placeholders, 
            *NUM_INSERT_COLS,
            1
        ); 

        // Build insert statement
        let statement = format!(
            "INSERT INTO {table_name} ({}) VALUES {} ON CONFLICT (partition, mass, sequence) DO UPDATE SET proteins = array_cat({table_name}.proteins, EXCLUDED.proteins)", 
            INSERT_COLS,
            placeholders.as_str(),
            table_name = TABLE_NAME
        );
        // Build insert values
        let mut insert_values: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(num_placeholders);
        for peptide in peptides {
            insert_values.push(peptide.get_partition_as_ref());
            insert_values.push(peptide.get_mass_as_ref());
            insert_values.push(peptide.get_sequence());
            insert_values.push(peptide.get_missed_cleavages_as_ref());
            insert_values.push(peptide.get_aa_counts());
            insert_values.push(peptide.get_proteins());
            insert_values.push(peptide.get_is_swiss_prot_as_ref());
            insert_values.push(peptide.get_is_trembl_as_ref());
            insert_values.push(peptide.get_taxonomy_ids());
            insert_values.push(peptide.get_unique_taxonomy_ids());
            insert_values.push(peptide.get_proteome_ids());
        }
        client.execute(&statement, insert_values.as_slice())?;
        return Ok(());
    }

    /// Updates the protein accessions of the given peptides.
    /// If new_protein_accession is given a metadata flaged as outdated.
    /// 
    /// # Arguments
    /// * `client` - Database client or open transaction
    /// * `peptides` - Iterator over peptides to update
    /// * `old_protein_accession` - Old protein accession to remove
    /// * `new_protein_accession` - New protein accession to add (optional)
    /// 
    pub fn update_protein_accession<'a, C, T>(
        client: &mut C, peptides: &mut T,
        old_protein_accession: &str, new_protein_accession: Option<&str>
    ) -> Result<()> where 
        C: GenericClient,
        T: Iterator<Item=&'a Peptide> + ExactSizeIterator 
    {

        let num_placeholders = peptides.len() * 3;

        let mut num_update_values = num_placeholders + 1;
        let mut placeholder_offset = 2;

        if new_protein_accession.is_some() {
            num_update_values += 1;
            placeholder_offset += 1;    // need to start at 3 when a new protein accession is given
        }

        let placeholders = Self::create_chunked_array_with_placeholders(
            num_placeholders, 
            NUM_PRIMARY_KEY_COLS,
            placeholder_offset
        );

        // if a new protein accession is given add it, otherwise set is_metadata_updated to false
        let set_statement = if new_protein_accession.is_some() {
            "proteins = array_append(array_remove(proteins, $1), $2)".to_string()
        } else {
            "proteins = array_remove(proteins, $1), is_metadata_updated = false".to_string()
        };

        let statement = format!(
            "UPDATE {} SET {} WHERE (partition, mass, sequence) in ({})",
            TABLE_NAME,
            set_statement,
            placeholders.as_str()
        );

        let mut update_values: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(num_update_values);
        update_values.push(&old_protein_accession);
        
        if new_protein_accession.is_some() {
            update_values.push(&new_protein_accession);
        }

        for peptide in peptides {
            update_values.push(peptide.get_partition_as_ref());
            update_values.push(peptide.get_mass_as_ref());
            update_values.push(peptide.get_sequence());
        }
        client.execute(&statement, update_values.as_slice())?;
        return Ok(());
    }

    pub fn unset_is_metadata_updated<'a, C, T>(
        client: &mut C, peptides: &mut T,
    ) -> Result<()> where 
        C: GenericClient,
        T: Iterator<Item=&'a Peptide> + ExactSizeIterator 
    {
        let num_placeholders = peptides.len() * 3;

        let num_update_values = num_placeholders + 1;

        let placeholders = Self::create_chunked_array_with_placeholders(
            num_placeholders, 
            3,
            1
        );

        let statement = format!(
            "UPDATE {} SET is_metadata_updated = false WHERE (partition, mass, sequence) in ({})",
            TABLE_NAME,
            placeholders.as_str()
        );

        let mut update_values: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(num_update_values);
        for peptide in peptides {
            update_values.push(peptide.get_partition_as_ref());
            update_values.push(peptide.get_mass_as_ref());
            update_values.push(peptide.get_sequence());
        }

        client.execute(&statement, update_values.as_slice())?;

        return Ok(());
    }
}

impl Table<Peptide> for PeptideTable {
    fn table_name() -> &'static str {
        return TABLE_NAME;
    }

    fn select_cols() -> &'static str {
        return SELECT_COLS;
    }
}



#[cfg(test)]
mod tests {
    // std imports
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;

    // external imports
    use fallible_iterator::FallibleIterator;
    use serial_test::serial;

    use crate::biology::digestion_enzyme::functions::{
        get_enzyme_by_name,
        create_peptides_entities_from_digest
    };
    // internal imports
    use crate::database::citus::table::Table;
    use crate::database::citus::tests::{prepare_database_for_tests, get_client};
    use crate::io::uniprot_text::reader::Reader;
    use super::*;

    const CONFLICTING_PEPTIDE_PROTEIN_ACCESSION : &'static str = "P41159";

    lazy_static!{
        // Peptides for Leptin (UniProt accession Q257X2, with KP on first position) digested with 3 missed cleavages, length 6 - 50
        // Tested with https://web.expasy.org/peptide_mass/
        static ref EXPECTED_PEPTIDES: HashMap<String, i16> = collection! {
            "MDQTLAIYQQILASLPSRNVIQISNDLENLRDLLHLLAASKSCPLPQVR".to_string() => 3,
            "VTGLDFIPGLHPLLSLSKMDQTLAIYQQILASLPSRNVIQISNDLENLR".to_string() => 2,
            "SCPLPQVRALESLESLGVVLEASLYSTEVVALSRLQGSLQDMLR".to_string() => 2,
            "ALESLESLGVVLEASLYSTEVVALSRLQGSLQDMLRQLDLSPGC".to_string() => 2,
            "DLLHLLAASKSCPLPQVRALESLESLGVVLEASLYSTEVVALSR".to_string() => 2,
            "MDQTLAIYQQILASLPSRNVIQISNDLENLRDLLHLLAASK".to_string() => 2,
            "QRVTGLDFIPGLHPLLSLSKMDQTLAIYQQILASLPSR".to_string() => 2,
            "TIVTRINDISHTQSVSSKQRVTGLDFIPGLHPLLSLSK".to_string() => 3,
            "VTGLDFIPGLHPLLSLSKMDQTLAIYQQILASLPSR".to_string() => 1,
            "ALESLESLGVVLEASLYSTEVVALSRLQGSLQDMLR".to_string() => 1,
            "CGPLYRFLWLWPYLSYVEAVPIRKVQDDTK".to_string() => 3,
            "SCPLPQVRALESLESLGVVLEASLYSTEVVALSR".to_string() => 1,
            "INDISHTQSVSSKQRVTGLDFIPGLHPLLSLSK".to_string() => 2,
            "MDQTLAIYQQILASLPSRNVIQISNDLENLR".to_string() => 1,
            "NVIQISNDLENLRDLLHLLAASKSCPLPQVR".to_string() => 2,
            "FLWLWPYLSYVEAVPIRKVQDDTKTLIK".to_string() => 3,
            "KPMRCGPLYRFLWLWPYLSYVEAVPIRK".to_string() => 3,
            "KPMRCGPLYRFLWLWPYLSYVEAVPIR".to_string() => 2,
            "VQDDTKTLIKTIVTRINDISHTQSVSSK".to_string() => 3,
            "CGPLYRFLWLWPYLSYVEAVPIRK".to_string() => 2,
            "FLWLWPYLSYVEAVPIRKVQDDTK".to_string() => 2,
            "CGPLYRFLWLWPYLSYVEAVPIR".to_string() => 1,
            "ALESLESLGVVLEASLYSTEVVALSR".to_string() => 0,
            "TLIKTIVTRINDISHTQSVSSKQR".to_string() => 3,
            "NVIQISNDLENLRDLLHLLAASK".to_string() => 1,
            "TLIKTIVTRINDISHTQSVSSK".to_string() => 2,
            "FLWLWPYLSYVEAVPIRK".to_string() => 1,
            "TIVTRINDISHTQSVSSKQR".to_string() => 2,
            "QRVTGLDFIPGLHPLLSLSK".to_string() => 1,
            "FLWLWPYLSYVEAVPIR".to_string() => 0,
            "MDQTLAIYQQILASLPSR".to_string() => 0,
            "TIVTRINDISHTQSVSSK".to_string() => 1,
            "LQGSLQDMLRQLDLSPGC".to_string() => 1,
            "DLLHLLAASKSCPLPQVR".to_string() => 1,
            "VTGLDFIPGLHPLLSLSK".to_string() => 0,
            "KVQDDTKTLIKTIVTR".to_string() => 3,
            "VQDDTKTLIKTIVTR".to_string() => 2,
            "INDISHTQSVSSKQR".to_string() => 1,
            "NVIQISNDLENLR".to_string() => 0,
            "INDISHTQSVSSK".to_string() => 0,
            "KVQDDTKTLIK".to_string() => 2,
            "VQDDTKTLIK".to_string() => 1,
            "LQGSLQDMLR".to_string() => 0,
            "DLLHLLAASK".to_string() => 0,
            "TLIKTIVTR".to_string() => 1,
            "KPMRCGPLYR".to_string() => 1,
            "SCPLPQVR".to_string() => 0,
            "KVQDDTK".to_string() => 1,
            "QLDLSPGC".to_string() => 0,
            "CGPLYR".to_string() => 0,
            "VQDDTK".to_string() => 0
        };

        static ref PEPTIDE_LIMITS: Vec<i64> = fs::read_to_string("./test_files/partition_limits.txt")
            .unwrap()
            .split("\n")
            .map(|element| element.trim().to_owned())
            .filter(|element| !element.is_empty())
            .map(|element| element.parse::<i64>())
            .collect::<Result<Vec<i64>, _>>()
            .unwrap();

    }

    /// Test inserting
    /// 
    #[test]
    #[serial]
    fn test_insert() {
        prepare_database_for_tests();
        let mut client = get_client();

        let mut reader = Reader::new(Path::new("test_files/leptin.txt"), 1024).unwrap();
        let leptin = reader.next().unwrap().unwrap();

        let digestion_enzyme = get_enzyme_by_name("Trypsin", 3, 6, 50).unwrap();

        let digest = digestion_enzyme.digest(leptin.get_sequence());

        let peptides = create_peptides_entities_from_digest::<Vec<Peptide>> (
            &digest,
            &PEPTIDE_LIMITS,
            Some(&leptin)
        ).unwrap();

        assert_eq!(peptides.len(), EXPECTED_PEPTIDES.len());

        // Create a conflicting peptide which is associated with another protein
        // to trigger conflict handling when inserting the peptides.
        let conflicting_peptides = vec![Peptide::new(
            peptides[0].get_partition(),
            peptides[0].get_mass(),
            peptides[0].get_sequence().to_owned(),
            peptides[0].get_missed_cleavages(),
            vec![CONFLICTING_PEPTIDE_PROTEIN_ACCESSION.to_owned()],
            peptides[0].get_is_swiss_prot(),
            peptides[0].get_is_trembl(),
            vec![9925],
            vec![9925],
            vec!["UP000291000".to_owned()]
        ).unwrap()];

        PeptideTable::bulk_insert(
            &mut client,
            &mut conflicting_peptides.iter()
        ).unwrap();

        PeptideTable::bulk_insert(
            &mut client,
            &mut peptides.iter()
        ).unwrap();

        let count_statement = format!("SELECT count(*) FROM {}", PeptideTable::table_name());
        let row = client.query_one(&count_statement, &[]).unwrap();
        assert_eq!(row.get::<usize, i64>(0) as usize, EXPECTED_PEPTIDES.len());

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let row = PeptideTable::raw_select(
            &mut client,
            "proteins",
            "WHERE partition = $1 AND mass = $2 and sequence = $3", 
            &[
                conflicting_peptides[0].get_partition_as_ref(),
                conflicting_peptides[0].get_mass_as_ref(),
                &conflicting_peptides[0].get_sequence()
            ]
        ).unwrap().unwrap();

        let associated_proteins: Vec<String> = row.get("proteins");
        assert_eq!(associated_proteins.len(), 2);
        assert!(associated_proteins.contains(&leptin.get_accession().to_owned()));
        assert!(associated_proteins.contains(&CONFLICTING_PEPTIDE_PROTEIN_ACCESSION.to_owned()));

        // Check if metadata is marked as not updated.
        let rows = PeptideTable::raw_select_multiple(
            &mut client,
            "is_metadata_updated",
            "",
            &[]
        ).unwrap();

        // Check if accession was updated and not appended for all peptides.
        for row in rows.iter() {
            assert!(!row.get::<_, bool>("is_metadata_updated"));
        }

        client.close().unwrap();
    }

    /// Tests protein accession update and disassociation (removal of protein accession from peptide)
    /// 
    #[test]
    #[serial]
    fn test_accession_update() {
        prepare_database_for_tests();
        let mut client = get_client();

        let mut reader = Reader::new(Path::new("test_files/leptin.txt"), 1024).unwrap();
        let leptin = reader.next().unwrap().unwrap();

        let digestion_enzyme = get_enzyme_by_name("Trypsin", 3, 6, 50).unwrap();

        let digest = digestion_enzyme.digest(leptin.get_sequence());

        let peptides = create_peptides_entities_from_digest::<Vec<Peptide>> (
            &digest,
            &PEPTIDE_LIMITS,
            Some(&leptin)
        ).unwrap();

        assert_eq!(peptides.len(), EXPECTED_PEPTIDES.len());

        PeptideTable::bulk_insert(
            &mut client,
            &mut peptides.iter()
        ).unwrap();

        PeptideTable::update_protein_accession(
            &mut client, 
            &mut peptides.iter(), 
            leptin.get_accession(), 
            Some(CONFLICTING_PEPTIDE_PROTEIN_ACCESSION)
        ).unwrap();

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let rows = PeptideTable::raw_select_multiple(
            &mut client,
            "proteins",
            "",
            &[]
        ).unwrap();

        // Check if accession was updated and not appended for all peptides.
        for row in rows.iter() {
            let protein_accessions = row.get::<_, Vec<String>>("proteins");
            assert_eq!(protein_accessions.len(), 1);
            assert_eq!(protein_accessions[0], CONFLICTING_PEPTIDE_PROTEIN_ACCESSION);
        }

        PeptideTable::update_protein_accession(
            &mut client, 
            &mut peptides.iter(), 
            leptin.get_accession(), 
            None
        ).unwrap();

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let rows = PeptideTable::raw_select_multiple(
            &mut client,
            "proteins",
            "",
            &[]
        ).unwrap();

        // Check if accession was updated and not appended for all peptides.
        for row in rows.iter() {
            let protein_accessions = row.get::<_, Vec<String>>("proteins");
            assert_eq!(protein_accessions.len(), 1);
            assert_eq!(protein_accessions[0], CONFLICTING_PEPTIDE_PROTEIN_ACCESSION);
        }

        client.close().unwrap();
    }

    /// Test update flagging peptides for metadata update
    /// 
    #[test]
    #[serial]
    fn test_flagging_for_metadata_update() {
        prepare_database_for_tests();
        let mut client = get_client();

        let mut reader = Reader::new(Path::new("test_files/leptin.txt"), 1024).unwrap();
        let leptin = reader.next().unwrap().unwrap();

        let digestion_enzyme = get_enzyme_by_name("Trypsin", 3, 6, 50).unwrap();

        let digest = digestion_enzyme.digest(leptin.get_sequence());

        let peptides = create_peptides_entities_from_digest::<Vec<Peptide>> (
            &digest,
            &PEPTIDE_LIMITS,
            Some(&leptin)
        ).unwrap();

        PeptideTable::update_protein_accession(
            &mut client, 
            &mut peptides.iter(), 
            leptin.get_accession(), 
            Some(CONFLICTING_PEPTIDE_PROTEIN_ACCESSION)
        ).unwrap();

        let statement = format!("UPDATE {} SET is_metadata_updated = true", PeptideTable::table_name());

        client.execute(&statement, &[]).unwrap();

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let rows = PeptideTable::raw_select_multiple(
            &mut client,
            "is_metadata_updated",
            "",
            &[]
        ).unwrap();

        for row in rows.iter() {
            assert!(row.get::<_, bool>("is_metadata_updated"));
        }

        PeptideTable::unset_is_metadata_updated(
            &mut client,
            &mut peptides.iter()
        ).unwrap();


        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let rows = PeptideTable::raw_select_multiple(
            &mut client,
            "is_metadata_updated",
            "",
            &[]
        ).unwrap();

        for row in rows.iter() {
            assert!(!row.get::<_, bool>("is_metadata_updated"));
        }
    }
}