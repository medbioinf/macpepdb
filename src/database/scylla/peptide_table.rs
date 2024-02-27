// std imports
use std::collections::HashSet;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use async_stream::try_stream;
use dihardts_omicstools::proteomics::peptide::calculate_mass_of_peptide_sequence;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use dihardts_omicstools::proteomics::proteases::protease::Protease;
use fallible_iterator::FallibleIterator;
use futures::future::join_all;
use futures::Stream;
use scylla::frame::response::result::{CqlValue, Row};
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::errors::QueryError;
use scylla::transport::iterator::RowIterator;
use scylla::transport::query_result::FirstRowError;

// internal imports
use crate::database::generic_client::GenericClient;
use crate::database::scylla::peptide_search::{FalliblePeptideStream, MultiTaskSearch, Search};
use crate::database::selectable_table::SelectableTable as SelectableTableTrait;
use crate::database::table::Table;
use crate::entities::configuration::Configuration;
use crate::entities::peptide::Peptide;
use crate::entities::protein::Protein;
use crate::mass::convert::to_int as mass_to_int;
use crate::tools::omicstools::convert_to_internal_dummy_peptide;
use crate::tools::peptide_partitioner::get_mass_partition;

use crate::database::scylla::client::Client;

pub const TABLE_NAME: &'static str = "peptides";

pub const SELECT_COLS: &'static str = "partition, mass, sequence, missed_cleavages, aa_counts, proteins, is_swiss_prot, is_trembl, taxonomy_ids, unique_taxonomy_ids, proteome_ids, domains";

const INSERT_COLS: &'static str = SELECT_COLS;

const UPDATE_COLS: &'static str = "missed_cleavages, aa_counts, proteins, is_swiss_prot, is_trembl, taxonomy_ids, unique_taxonomy_ids, proteome_ids, domains";

lazy_static! {
    static ref INSERT_PLACEHOLDERS: String = INSERT_COLS
        .split(", ",)
        .enumerate()
        .map(|(_, _)| "?")
        .collect::<Vec<&str>>()
        .join(", ");
    pub static ref UPDATE_SET_PLACEHOLDER: String = UPDATE_COLS
        .split(", ",)
        .enumerate()
        .map(|(_, col)| {
            if col == "proteins" {
                return format!("{} = {} + ?", col, col);
            }
            format!("{} = ?", col)
        })
        .collect::<Vec<String>>()
        .join(", ");
}

pub struct PeptideTable;

impl PeptideTable {
    /// Inserts multiple peptides into the database.
    /// On conflict the proteins array is updated with the new protein accessions and the metadata flag is set to false.
    ///
    /// # Arguments
    /// * `client` - Database client or open transaction
    /// * `peptides` - Iterator over peptides to insert
    ///
    pub async fn bulk_insert<'a, T>(
        client: &Client,
        peptides: T,
        prepared: &PreparedStatement,
    ) -> Result<()>
    where
        T: Iterator<Item = &'a Peptide> + ExactSizeIterator,
    {
        if peptides.len() == 0 {
            return Ok(());
        }

        // Update has upsert functionality in Scylla. protein accessions are added to the set (see UPDATE_SET_PLACEHOLDERS)
        // Alternative: always execute two lightweight transactions update ... if exists, update ... if not exists
        // Alternative: select then check in application code then upsert
        let insertion_futures = peptides
            .map(|x| {
                client.execute(
                    &prepared,
                    (
                        x.get_missed_cleavages(),
                        x.get_aa_counts(),
                        x.get_proteins(),
                        x.get_is_swiss_prot(),
                        x.get_is_trembl(),
                        x.get_taxonomy_ids(),
                        x.get_unique_taxonomy_ids(),
                        x.get_proteome_ids(),
                        x.get_domains(),
                        x.get_partition(),
                        x.get_mass(),
                        x.get_sequence(),
                    ),
                )
            })
            .collect::<Vec<_>>();

        join_all(insertion_futures).await;

        return Ok(());
    }

    /// Updates the protein accessions of the given peptides.
    /// If new_protein_accession is given a metadata flaged as outdated.
    /// The protein of new_protein_accession is assumed to be identical to the old_protein in all fields but the accession.
    /// Therefore is_metadata_updated is not set to false in the update case.
    ///
    /// # Arguments
    /// * `client` - Database client or open transaction
    /// * `peptides` - Iterator over peptides to update
    /// * `old_protein_accession` - Old protein accession to remove
    /// * `new_protein_accession` - New protein accession to add (optional)
    ///
    pub async fn update_protein_accession<'a, T>(
        client: &Client,
        peptides: &mut T,
        old_protein_accession: &str,
        new_protein_accession: Option<&str>,
    ) -> Result<()>
    where
        T: Iterator<Item = &'a Peptide> + ExactSizeIterator,
    {
        // if a new protein accession is given add it, otherwise set is_metadata_updated to false
        let set_statement = if new_protein_accession.is_some() {
            format!(
                "proteins = proteins - {{'{}'}}, proteins = proteins + {{'{}'}}",
                old_protein_accession,
                new_protein_accession.unwrap()
            )
            .to_string()
        } else {
            format!(
                "proteins = proteins - {{'{}'}}, is_metadata_updated = false",
                old_protein_accession,
            )
            .to_string()
        };

        let statement = format!(
            "UPDATE {}.{} SET {} WHERE partition = ? and mass = ? and sequence = ?",
            client.get_database(),
            TABLE_NAME,
            set_statement,
        );

        let prepared = client.prepare(statement).await?;

        for peptide in peptides {
            client
                .execute(
                    &prepared,
                    (
                        peptide.get_partition(),
                        peptide.get_mass(),
                        peptide.get_sequence(),
                    ),
                )
                .await?;
        }

        return Ok(());
    }

    pub async fn unset_is_metadata_updated<'a, T>(client: &Client, peptides: &mut T) -> Result<()>
    where
        T: Iterator<Item = &'a Peptide> + ExactSizeIterator,
    {
        let statement = format!(
            "UPDATE {}.{} SET is_metadata_updated = false WHERE partition = ? and mass = ? and sequence = ?",
            client.get_database(), TABLE_NAME
        );
        let prepared = client.prepare(statement).await?;

        for peptide in peptides {
            client
                .execute(
                    &prepared,
                    (
                        peptide.get_partition(),
                        peptide.get_mass(),
                        peptide.get_sequence(),
                    ),
                )
                .await?;
        }

        return Ok(());
    }

    /// Returns a fallible stream over the filtered peptides.
    /// (Combines stream for multiple PTM conditions)
    ///
    /// # Arguments
    /// * `client` - The database client in an Arc, so it can owned by the stream
    /// * `configuration` - MaCPepDB configuration in an Arc, so it can owned by the stream
    /// * `mass` - The mass to search for
    /// * `lower_mass_tolerance_ppm` - The lower mass tolerance in ppm
    /// * `upper_mass_tolerance_ppm` - The upper mass tolerance in ppm
    /// * `max_variable_modifications` - The maximum number of variable modifications
    /// * `taxonomy_id` - Optional: The taxonomy id to filter for
    /// * `proteome_id` - Optional: The proteome id to filter for
    /// * `is_reviewed` - Optional: If the peptides should be reviewed or unreviewed
    /// * `ptms` - The PTMs to consider
    /// * `matching_peptides` - A bloom filter to check if a peptide was already found
    ///
    pub async fn search<'a>(
        client: Arc<Client>,
        configuration: Arc<Configuration>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: i16,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptms: Vec<PTM>,
    ) -> Result<FalliblePeptideStream> {
        let partition_limits = Arc::new(configuration.get_partition_limits().clone());
        MultiTaskSearch::search(
            client,
            partition_limits,
            mass,
            lower_mass_tolerance_ppm,
            upper_mass_tolerance_ppm,
            max_variable_modifications,
            true,
            taxonomy_ids,
            proteome_ids,
            is_reviewed,
            ptms,
            None,
        )
        .await
    }

    /// Checks if the given sequence is an existing peptide in the database.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `sequence` - The sequence to check
    /// * `configuration` - MaCPepDB configuration
    ///
    pub async fn exists_by_sequence(
        client: &Client,
        sequence: &str,
        configuration: &Configuration,
    ) -> Result<bool> {
        let sequence = sequence.to_uppercase();
        let mass = mass_to_int(calculate_mass_of_peptide_sequence(sequence.as_str())?);
        let partition = get_mass_partition(configuration.get_partition_limits(), mass)?;

        let peptide_opt = PeptideTable::select(
            client,
            "WHERE partition = ? AND mass = ? and sequence = ?",
            &[
                &CqlValue::BigInt(partition as i64),
                &CqlValue::BigInt(mass),
                &CqlValue::Text(sequence),
            ],
        )
        .await?;

        Ok(peptide_opt.is_some())
    }

    /// Returns peptides of protein
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `protein` - The protein
    ///
    pub async fn get_peptides_of_proteins<'a>(
        client: &'a Client,
        protein: &'a Protein,
        protease: &'a dyn Protease,
        partition_limits: &'a Vec<i64>,
    ) -> Result<impl Stream<Item = Result<Peptide>> + 'a> {
        Ok(try_stream! {
            // First digest the protein
            let dummy_peptides: HashSet<Peptide> = convert_to_internal_dummy_peptide(
                Box::new(protease.cleave(protein.get_sequence())?),
                &partition_limits,
            )
            .collect()?;

            // Sort the peptides by partition as Scylla do not allow a IN statement with multiple columns on the
            // partition key
            let mut peptides_by_partition: Vec<Vec<&Peptide>> = vec![vec![]; partition_limits.len()];

            for dummy_pep in dummy_peptides.iter() {
                let partition = get_mass_partition(&partition_limits, dummy_pep.get_mass())?;
                peptides_by_partition[partition].push(dummy_pep);
            }

            // Query each partition separately
            for (partition, dummy_peptides) in peptides_by_partition.iter().enumerate() {
                if dummy_peptides.is_empty() {
                    continue;
                }

                // Create a flat array with all the partition key and pairs of mass and sequence
                let mut params: Vec<CqlValue> = Vec::with_capacity(dummy_peptides.len() * 2 + 1);
                params.push(CqlValue::BigInt(partition as i64));
                for dummy_pep in dummy_peptides.iter() {
                    params.push(CqlValue::BigInt(dummy_pep.get_mass()));
                    params.push(CqlValue::Text(dummy_pep.get_sequence().to_owned()));
                }
                // Make it an array of references
                let params_refs: Vec<&CqlValue> = params.iter().collect();

                // Create placeholder for mass and sequence pairs
                let mass_seq_placeholders = (0..dummy_peptides.len())
                    .map(|_| "(?, ?)".to_owned())
                    .collect::<Vec<String>>()
                    .join(", ");

                // Create statement addition
                let statement_addition = &format!(
                    "WHERE partition = ? AND (mass, sequence) IN ({})",
                    mass_seq_placeholders
                );

                for await peptide in PeptideTable::stream(client, &statement_addition, params_refs.as_slice(), 10000).await? {
                    yield peptide?
                }
            }
        })
    }
}

impl Table for PeptideTable {
    fn table_name() -> &'static str {
        TABLE_NAME
    }
}

impl<'a> SelectableTableTrait<'a, Client> for PeptideTable {
    type Parameter = CqlValue;
    type Record = Row;
    type RecordIter = RowIterator;
    type RecordIterErr = QueryError;
    type Entity = Peptide;

    fn select_cols() -> &'static str {
        SELECT_COLS
    }

    async fn raw_select_multiple<'b>(
        client: &Client,
        cols: &str,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Vec<Self::Record>> {
        let session = client;
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
        client: &Client,
        cols: &str,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Option<Self::Record>> {
        let session = client;
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
        client: &Client,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Vec<Self::Entity>> {
        let rows =
            Self::raw_select_multiple(client, Self::select_cols(), additional, params).await?;
        let mut records = Vec::new();
        for row in rows {
            records.push(Self::Entity::from(row));
        }
        return Ok(records);
    }

    async fn select<'b>(
        client: &Client,
        additional: &str,
        params: &[&'b Self::Parameter],
    ) -> Result<Option<Self::Entity>> {
        let row = Self::raw_select(client, Self::select_cols(), additional, params).await?;
        if row.is_none() {
            return Ok(None);
        }
        return Ok(Some(Self::Entity::from(row.unwrap())));
    }

    async fn raw_stream(
        client: &'a Client,
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
        let mut prepared_statement = client.prepare(statement).await?;
        prepared_statement.set_page_size(num_rows);
        Ok(try_stream! {
            let row_stream = client.execute_iter(prepared_statement, params).await?;
            for await row in row_stream {
                yield row?;
            }
        })
    }

    async fn stream(
        client: &'a Client,
        additional: &'a str,
        params: &'a [&'a Self::Parameter],
        num_rows: i32,
    ) -> Result<impl Stream<Item = Result<Self::Entity>>> {
        let raw_stream =
            Self::raw_stream(client, Self::select_cols(), additional, params, num_rows).await?;
        Ok(try_stream! {
            for await row in raw_stream {
                yield Self::Entity::from(row?);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    // std imports
    use std::collections::{HashMap, HashSet};
    use std::path::Path;

    // external imports
    use dihardts_omicstools::proteomics::proteases::functions::get_by_name as get_protease_by_name;
    use fallible_iterator::FallibleIterator;
    use serial_test::serial;

    // internal imports
    use super::*;
    use crate::database::scylla::client::Client;
    use crate::database::scylla::prepare_database_for_tests;
    use crate::database::scylla::tests::DATABASE_URL;
    use crate::io::uniprot_text::reader::Reader;
    use crate::tools::omicstools::convert_to_internal_peptide;

    const CONFLICTING_PEPTIDE_PROTEIN_ACCESSION: &'static str = "P41159";

    lazy_static! {
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

        static ref PARTITION_LIMITS: Vec<i64> = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .from_path("test_files/mouse_partitioning.tsv")
            .unwrap()
            .deserialize().map(|line| line.unwrap())
            .collect();

    }

    /// Test inserting
    ///
    #[tokio::test]
    #[serial]
    async fn test_insert() {
        let mut client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&mut client).await;

        let mut reader = Reader::new(Path::new("test_files/leptin.txt"), 1024).unwrap();
        let leptin = reader.next().unwrap().unwrap();

        let protease = get_protease_by_name("trypsin", Some(6), Some(50), Some(3)).unwrap();

        let peptides = Vec::from_iter(
            convert_to_internal_peptide(
                Box::new(protease.cleave(leptin.get_sequence()).unwrap()),
                &PARTITION_LIMITS,
                &leptin,
            )
            .collect::<HashSet<Peptide>>()
            .unwrap(),
        );

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
            vec!["UP000291000".to_owned()],
            vec![],
        )
        .unwrap()];

        let statement = format!(
            "UPDATE {}.{} SET {}, is_metadata_updated = false WHERE partition = ? and mass = ? and sequence = ?",
            client.get_database(),
            TABLE_NAME,
            UPDATE_SET_PLACEHOLDER.as_str()
        );
        let prepared = client.prepare(statement).await.unwrap();

        PeptideTable::bulk_insert(&client, &mut conflicting_peptides.iter(), &prepared)
            .await
            .unwrap();

        PeptideTable::bulk_insert(&client, &mut peptides.iter(), &prepared)
            .await
            .unwrap();

        let count_statement = format!(
            "SELECT count(*) FROM {}.{}",
            client.get_database(),
            PeptideTable::table_name()
        );
        let row = client
            .query(count_statement, &[])
            .await
            .unwrap()
            .first_row()
            .unwrap();

        assert_eq!(
            row.columns
                .first()
                .unwrap()
                .as_ref()
                .unwrap()
                .as_bigint()
                .unwrap() as usize,
            EXPECTED_PEPTIDES.len()
        );

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let row = PeptideTable::raw_select(
            &mut client,
            "proteins",
            "WHERE partition = ? AND mass = ? and sequence = ?",
            &[
                &CqlValue::BigInt(conflicting_peptides[0].get_partition()),
                &CqlValue::BigInt(conflicting_peptides[0].get_mass()),
                &CqlValue::Text(conflicting_peptides[0].get_sequence().to_owned()),
            ],
        )
        .await
        .unwrap()
        .unwrap();

        let associated_proteins: Vec<String> = row
            .columns
            .get(0)
            .unwrap()
            .to_owned()
            .unwrap()
            .into_vec()
            .unwrap()
            .into_iter()
            .map(|cql_val| cql_val.as_text().unwrap().to_owned())
            .collect();

        assert_eq!(associated_proteins.len(), 2);
        assert!(associated_proteins.contains(&leptin.get_accession().to_owned()));
        assert!(associated_proteins.contains(&CONFLICTING_PEPTIDE_PROTEIN_ACCESSION.to_owned()));

        // Check if metadata is marked as not updated.
        let rows = PeptideTable::raw_select_multiple(&mut client, "is_metadata_updated", "", &[])
            .await
            .unwrap();

        // Check if accession was updated and not appended for all peptides.
        for row in rows.iter() {
            let is_metadata_updated_opt = row.columns.get(0).unwrap().to_owned();
            if is_metadata_updated_opt.is_some() {
                assert!(!is_metadata_updated_opt.unwrap().as_boolean().unwrap());
            }
        }
    }

    /// Tests protein accession update and disassociation (removal of protein accession from peptide)
    ///
    #[tokio::test]
    #[serial]
    async fn test_accession_update() {
        let mut client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&mut client).await;

        let mut reader = Reader::new(Path::new("test_files/leptin.txt"), 1024).unwrap();
        let leptin = reader.next().unwrap().unwrap();

        let protease = get_protease_by_name("Trypsin", Some(6), Some(50), Some(3)).unwrap();

        let peptides = convert_to_internal_peptide(
            Box::new(protease.cleave(leptin.get_sequence()).unwrap()),
            &PARTITION_LIMITS,
            &leptin,
        )
        .collect::<HashSet<Peptide>>()
        .unwrap();

        assert_eq!(peptides.len(), EXPECTED_PEPTIDES.len());

        let statement = format!(
            "UPDATE {}.{} SET {}, is_metadata_updated = false WHERE partition = ? and mass = ? and sequence = ?",
            client.get_database(),
            TABLE_NAME,
            UPDATE_SET_PLACEHOLDER.as_str()
        );

        let prepared = client.prepare(statement).await.unwrap();

        PeptideTable::bulk_insert(&client, &mut peptides.iter(), &prepared)
            .await
            .unwrap();

        PeptideTable::update_protein_accession(
            &client,
            &mut peptides.iter(),
            leptin.get_accession(),
            Some(CONFLICTING_PEPTIDE_PROTEIN_ACCESSION),
        )
        .await
        .unwrap();

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let rows = PeptideTable::raw_select_multiple(&client, "proteins", "", &[])
            .await
            .unwrap();

        // Check if accession was updated and not appended for all peptides.
        for row in rows.iter() {
            let protein_accessions: Vec<String> = row
                .columns
                .get(0)
                .unwrap()
                .to_owned()
                .unwrap()
                .into_vec()
                .unwrap()
                .into_iter()
                .map(|cql_val| cql_val.as_text().unwrap().to_owned())
                .collect();
            assert_eq!(protein_accessions.len(), 1);
            assert_eq!(protein_accessions[0], CONFLICTING_PEPTIDE_PROTEIN_ACCESSION);
        }

        PeptideTable::update_protein_accession(
            &client,
            &mut peptides.iter(),
            CONFLICTING_PEPTIDE_PROTEIN_ACCESSION,
            None,
        )
        .await
        .unwrap();

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let rows = PeptideTable::raw_select_multiple(&client, "proteins", "", &[])
            .await
            .unwrap();

        // Check if accession was updated and not appended for all peptides.
        for row in rows.iter() {
            let protein_accessions_opt = row.columns.get(0).unwrap();
            assert!(protein_accessions_opt.is_none());
        }
    }

    /// Test update flagging peptides for metadata update
    /// ToDo: Split into three tests: update_protein_accession with new, update_protein_accession without new, unset_metadata
    ///
    #[tokio::test]
    #[serial]
    async fn test_flagging_for_metadata_update() {
        let mut client = Client::new(DATABASE_URL).await.unwrap();
        prepare_database_for_tests(&mut client).await;

        let mut reader = Reader::new(Path::new("test_files/leptin.txt"), 1024).unwrap();
        let leptin = reader.next().unwrap().unwrap();

        let protease = get_protease_by_name("Trypsin", Some(6), Some(50), Some(3)).unwrap();

        let peptides: HashSet<Peptide> = convert_to_internal_peptide(
            Box::new(protease.cleave(leptin.get_sequence()).unwrap()),
            &PARTITION_LIMITS,
            &leptin,
        )
        .collect()
        .unwrap();

        let statement = format!(
            "UPDATE {}.{} SET {}, is_metadata_updated = false WHERE partition = ? and mass = ? and sequence = ?",
            client.get_database(),
            TABLE_NAME,
            UPDATE_SET_PLACEHOLDER.as_str()
        );

        let prepared = client.prepare(statement).await.unwrap();

        PeptideTable::bulk_insert(&client, &mut peptides.iter(), &prepared)
            .await
            .unwrap();

        let statement = format!(
            "UPDATE {}.{} SET is_metadata_updated = true WHERE partition = ? and mass = ? and sequence = ?",
            client.get_database(),
            PeptideTable::table_name()
        );

        for peptide in &peptides {
            client
                .query(
                    statement.to_owned(),
                    (
                        peptide.get_partition(),
                        peptide.get_mass(),
                        peptide.get_sequence(),
                    ),
                )
                .await
                .unwrap();
        }
        PeptideTable::update_protein_accession(
            &client,
            &mut peptides.iter(),
            leptin.get_accession(),
            Some(CONFLICTING_PEPTIDE_PROTEIN_ACCESSION),
        )
        .await
        .unwrap();

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let rows = PeptideTable::raw_select_multiple(&client, "is_metadata_updated", "", &[])
            .await
            .unwrap();

        for row in rows.iter() {
            assert!(row
                .columns
                .get(0)
                .unwrap()
                .to_owned()
                .unwrap()
                .as_boolean()
                .unwrap());
        }

        PeptideTable::update_protein_accession(
            &client,
            &mut peptides.iter(),
            CONFLICTING_PEPTIDE_PROTEIN_ACCESSION,
            None,
        )
        .await
        .unwrap();

        let rows = PeptideTable::raw_select_multiple(&client, "is_metadata_updated", "", &[])
            .await
            .unwrap();

        for row in rows.iter() {
            assert!(!row
                .columns
                .get(0)
                .unwrap()
                .to_owned()
                .unwrap()
                .as_boolean()
                .unwrap());
        }

        for peptide in &peptides {
            client
                .query(
                    statement.to_owned(),
                    (
                        peptide.get_partition(),
                        peptide.get_mass(),
                        peptide.get_sequence(),
                    ),
                )
                .await
                .unwrap();
        }

        PeptideTable::unset_is_metadata_updated(&client, &mut peptides.iter())
            .await
            .unwrap();

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let rows = PeptideTable::raw_select_multiple(&client, "is_metadata_updated", "", &[])
            .await
            .unwrap();

        for row in rows.iter() {
            assert!(!row
                .columns
                .get(0)
                .unwrap()
                .to_owned()
                .unwrap()
                .as_boolean()
                .unwrap());
        }
    }
}
