use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{bail, Result};
use async_stream::try_stream;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use dihardts_omicstools::proteomics::proteases::protease::Protease;
use fallible_iterator::FallibleIterator;
use futures::future::join_all;
use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use scylla::frame::response::result::CqlValue;
use scylla::transport::errors::QueryError;
use tokio::pin;
use tokio::task::JoinSet;

use crate::chemistry::amino_acid::calc_sequence_mass_int;
use crate::database::generic_client::GenericClient;
use crate::database::scylla::peptide_search::{FalliblePeptideStream, MultiTaskSearch, Search};
use crate::database::table::Table;
use crate::entities::configuration::Configuration;
use crate::entities::domain::Domain;
use crate::entities::peptide::Peptide;
use crate::entities::protein::Protein;
use crate::tools::omicstools::convert_to_internal_dummy_peptide;
use crate::tools::peptide_partitioner::get_mass_partition;

use crate::database::scylla::client::Client;

pub const TABLE_NAME: &str = "peptides";

pub const SELECT_COLS: [&str; 12] = [
    "partition",
    "mass",
    "sequence",
    "missed_cleavages",
    "aa_counts",
    "proteins",
    "is_swiss_prot",
    "is_trembl",
    "taxonomy_ids",
    "unique_taxonomy_ids",
    "proteome_ids",
    "domains",
];

const INSERT_COLS: [&str; 12] = SELECT_COLS;

const UPDATE_COLS: [&str; 9] = [
    "missed_cleavages",
    "aa_counts",
    "proteins",
    "is_swiss_prot",
    "is_trembl",
    "taxonomy_ids",
    "unique_taxonomy_ids",
    "proteome_ids",
    "domains",
];

lazy_static! {
    /// Select statement with any additional where-clause
    ///
    static ref SELECT_STATEMENT: String =
        format!("SELECT {} FROM :KEYSPACE:.{}", SELECT_COLS.join(", "), TABLE_NAME);

    /// Insert statement
    /// Takes the columns stated in INSERT_COLS in the same order
    ///
    static ref INSERT_STATEMENT: String = format!(
        "INSERT INTO :KEYSPACE:.{} ({}) VALUES ({})",
        TABLE_NAME,
        INSERT_COLS.join(", "),
        vec!["?"; INSERT_COLS.len()].join(", ")
    );

    /// Upsert statement
    /// Takes the columns in UPDATE_COLS in the same order and peptide partition, mass and sequence as primary key
    ///
    static ref UPSERT_STATEMENT: String = format!(
        "UPDATE :KEYSPACE:.{} SET {}, is_metadata_updated = false WHERE partition = ? and mass = ? and sequence = ?",
        TABLE_NAME,
        UPDATE_COLS
            .iter()
            .map(|col| {
                if *col != "proteins" {
                    format!("{} = ?", col)
                } else {
                    format!("{} = {} + ?", col, col)
                }
            })
            .collect::<Vec<String>>()
            .join(", ")
    );

    /// Statement to set the metadatata_update column to false
    /// Takes peptide partition, mass and sequence
    ///
    static ref SET_METADATA_TO_FALSE_STATEMENT: String = format!(
        "UPDATE :KEYSPACE:.{} SET is_metadata_updated = false WHERE partition = ? and mass = ? and sequence = ?",
        TABLE_NAME
    );

    /// Statements for removing protein accession to a peptide
    /// Takes vector of proteins accessions and peptide partition, mass and sequence
    ///
    static ref REMOVE_PROTEINS_STATEMENT: String = format!("UPDATE :KEYSPACE:.{} SET proteins = proteins - ? WHERE partition = ? and mass = ? and sequence = ?", TABLE_NAME);

    /// Statements for adding protein accession to a peptide
    /// Takes vector of proteins accessions and peptide partition, mass and sequence
    ///
    static ref ADD_PROTEINS_STATEMENT: String = format!("UPDATE :KEYSPACE:.{} SET proteins = proteins + ? WHERE partition = ? and mass = ? and sequence = ?", TABLE_NAME);

    /// Statement to update the metadata of a peptide
    ///
    static ref UPDATE_METADATA_STATEMENT: String = format!(
        "UPDATE :KEYSPACE:.{} SET is_metadata_updated = true, is_swiss_prot = ?, is_trembl = ?, taxonomy_ids = ?, unique_taxonomy_ids = ?, proteome_ids = ?, domains = ? WHERE partition = ? AND mass = ? and sequence = ?",
        TABLE_NAME
    );

    /// Statement to select peptides by mass range
    ///
    static ref SELECT_BY_MASS_RANGE_STATEMENT: String = format!(
        "{} WHERE partition = ? AND mass >= ? AND mass <= ?",
        SELECT_STATEMENT.as_str(),
    );

}

/// Type alias for typed peptide rows
///
pub type TypedPeptideRow = (
    i64,
    i64,
    String,
    i16,
    Vec<i16>,
    HashSet<String>,
    bool,
    bool,
    HashSet<i64>,
    HashSet<i64>,
    HashSet<String>,
    HashSet<Domain>,
);

/// Implementation to convert TypedPeptideRow into a Peptide
///
impl From<TypedPeptideRow> for Peptide {
    fn from(row: TypedPeptideRow) -> Self {
        Peptide::new_full(
            row.0,
            row.1,
            row.2,
            row.3,
            row.4,
            row.5.into_iter().collect(),
            row.6,
            row.7,
            row.8.into_iter().collect(),
            row.9.into_iter().collect(),
            row.10.into_iter().collect(),
            row.11.into_iter().collect(),
        )
    }
}

/// Struct to access and manipulate the peptide
pub struct PeptideTable;

impl PeptideTable {
    /// Inserts multiple peptides into the database.
    /// On conflict the proteins array is updated with the new protein accessions and the metadata flag is set to false.
    ///
    /// # Arguments
    /// * `client` - Database client or open transaction
    /// * `peptides` - Iterator over peptides to insert
    ///
    pub async fn bulk_upsert<'a, T>(client: &Client, peptides: T) -> Result<()>
    where
        T: Iterator<Item = &'a Peptide> + ExactSizeIterator,
    {
        if peptides.len() == 0 {
            return Ok(());
        }

        let prepared_statement = client.get_prepared_statement(&UPSERT_STATEMENT).await?;

        // Update has upsert functionality in Scylla. protein accessions are added to the set (see UPDATE_SET_PLACEHOLDERS)
        // Alternative: always execute two lightweight transactions update ... if exists, update ... if not exists
        // Alternative: select then check in application code then upsert
        let insertion_futures = peptides
            .map(|x| {
                client.execute_unpaged(
                    &prepared_statement,
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

        match join_all(insertion_futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, QueryError>>()
        {
            Ok(_) => Ok(()),
            Err(e) => bail!("Error while upserting peptides into the database: {:?}", e),
        }
    }

    /// Updates the protein accessions of the given peptides.
    /// If new_protein_accession is given a metadata flagged as outdated.
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
                .execute_unpaged(
                    &prepared,
                    (
                        peptide.get_partition(),
                        peptide.get_mass(),
                        peptide.get_sequence(),
                    ),
                )
                .await?;
        }

        Ok(())
    }

    pub async fn add_proteins<'a, T>(
        client: &Client,
        peptides: &mut T,
        proteins: HashSet<String>,
    ) -> Result<()>
    where
        T: Iterator<Item = &'a Peptide> + ExactSizeIterator,
    {
        let prepared_statement = client
            .get_prepared_statement(&ADD_PROTEINS_STATEMENT)
            .await?;

        for peptide in peptides {
            client
                .execute_unpaged(
                    &prepared_statement,
                    (
                        &proteins,
                        peptide.get_partition(),
                        peptide.get_mass(),
                        &peptide.get_sequence(),
                    ),
                )
                .await?;
        }

        Ok(())
    }

    pub async fn remove_proteins<'a, T>(
        client: &Client,
        peptides: &mut T,
        proteins: HashSet<String>,
    ) -> Result<()>
    where
        T: Iterator<Item = &'a Peptide> + ExactSizeIterator,
    {
        let prepared_statement = client
            .get_prepared_statement(&REMOVE_PROTEINS_STATEMENT)
            .await?;

        for peptide in peptides {
            client
                .execute_unpaged(
                    &prepared_statement,
                    (
                        &proteins,
                        peptide.get_partition(),
                        peptide.get_mass(),
                        &peptide.get_sequence(),
                    ),
                )
                .await?;
        }

        Ok(())
    }

    pub async fn unset_is_metadata_updated<'a, T>(client: &Client, peptides: &mut T) -> Result<()>
    where
        T: Iterator<Item = &'a Peptide> + ExactSizeIterator,
    {
        let prepared_statement = client
            .get_prepared_statement(&SET_METADATA_TO_FALSE_STATEMENT)
            .await?;

        for peptide in peptides {
            client
                .execute_unpaged(
                    &prepared_statement,
                    (
                        peptide.get_partition(),
                        peptide.get_mass(),
                        peptide.get_sequence(),
                    ),
                )
                .await?;
        }

        Ok(())
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
    #[allow(clippy::too_many_arguments)]
    pub async fn search(
        client: Arc<Client>,
        configuration: Arc<Configuration>,
        mass: i64,
        lower_mass_tolerance_ppm: i64,
        upper_mass_tolerance_ppm: i64,
        max_variable_modifications: i16,
        taxonomy_ids: Option<Vec<i64>>,
        proteome_ids: Option<Vec<String>>,
        is_reviewed: Option<bool>,
        ptms: &[PTM],
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

        let stream = PeptideTable::select(
            client,
            "WHERE partition = ? AND mass = ? and sequence = ? LIMIT 1",
            &[
                &CqlValue::BigInt(partition as i64),
                &CqlValue::BigInt(mass),
                &CqlValue::Text(sequence),
            ],
        )
        .await?;

        pin!(stream);

        Ok(stream.try_next().await?.is_some())
    }

    /// Returns peptides of protein
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `protein` - The protein
    ///
    pub async fn get_peptides_of_proteins<'a>(
        client: Arc<Client>,
        protein: &'a Protein,
        protease: &'a dyn Protease,
        partition_limits: &'a [i64],
    ) -> Result<impl Stream<Item = Result<Peptide>> + 'a> {
        Ok(try_stream! {
            // First digest the protein
            let dummy_peptides: Vec<Peptide> = convert_to_internal_dummy_peptide(
                Box::new(protease.cleave(protein.get_sequence())?),
                partition_limits,
            )
            .collect::<HashSet<Peptide>>()?
            .into_iter()
            .collect();

            // Chunking the peptides to 2 * number of nodes which seem to be a good
            // value of concurrent peptide-SELECT. For sure this should not be linear.
            let chunk_size = dummy_peptides.len() / client.get_num_nodes() * 2;
            let dummy_peptides: Vec<Vec<Peptide>> = dummy_peptides.into_iter()
                .chunks(chunk_size)
                .into_iter()
                .map(Iterator::collect)
                .collect();

            for peptide_chunk in dummy_peptides {
                let mut join: JoinSet<Result<Option<Peptide>>> = JoinSet::new();
                for peptide in peptide_chunk {
                    let task_client = client.clone();
                    join.spawn(async move {
                        let stream = PeptideTable::select(
                            task_client.as_ref(),
                            "WHERE partition = ? AND mass = ? and sequence = ? LIMIT 1",
                            &[
                                &CqlValue::BigInt(peptide.get_partition()),
                                &CqlValue::BigInt(peptide.get_mass()),
                                &CqlValue::Text(peptide.get_sequence().to_owned()),
                            ],
                        ).await?;
                        pin!(stream);

                        stream.try_next().await
                    });
                }
                while let Some(peptide) = join.join_next().await {
                    yield match peptide?? {
                        Some(peptide) => peptide,
                        None => continue,
                    }
                }
            }
        })
    }

    /// Selects peptides from the database and streams them back.
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
    ) -> Result<impl Stream<Item = Result<Peptide>>> {
        let statement = format!("{} {};", SELECT_STATEMENT.as_str(), additional);
        let prepared_statement = client.get_prepared_statement(&statement).await?;

        let stream = client
            .execute_iter(prepared_statement, params)
            .await?
            .rows_stream::<TypedPeptideRow>()?;

        Ok(try_stream! {
            for await peptide_result in stream {
                yield peptide_result?.into();
            }
        })
    }

    /// Updates the metadata of the given peptides.
    ///
    /// # Arguments
    /// * `client` - Database client or open transaction
    /// * `peptide` - Peptide to update
    /// * `is_swiss_prot` - If the peptide is from SwissProt
    /// * `is_trembl` - If the peptide is from TrEMBL
    /// * `taxonomy_ids` - Taxonomy ids of the peptide
    /// * `unique_taxonomy_ids` - Unique taxonomy ids of the peptide
    /// * `proteome_ids` - Proteome ids of the peptide
    /// * `domains` - Domains of the peptide
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn update_metadata(
        client: &Client,
        peptide: &Peptide,
        is_swiss_prot: bool,
        is_trembl: bool,
        taxonomy_ids: &Vec<i64>,
        unique_taxonomy_ids: &Vec<i64>,
        proteome_ids: &Vec<String>,
        domains: &Vec<Domain>,
    ) -> Result<()> {
        let prepared_statement = client
            .get_prepared_statement(&UPDATE_METADATA_STATEMENT)
            .await?;
        client
            .execute_unpaged(
                &prepared_statement,
                (
                    &is_swiss_prot,
                    &is_trembl,
                    &taxonomy_ids,
                    &unique_taxonomy_ids,
                    &proteome_ids,
                    &domains,
                    peptide.get_partition(),
                    peptide.get_mass(),
                    peptide.get_sequence(),
                ),
            )
            .await?;
        Ok(())
    }

    /// Selectes peptides between the given mass range
    /// and streams them back.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `lower_mass` - The lower mass limit
    /// * `upper_mass` - The upper mass limit
    ///
    ///
    pub async fn select_by_mass_range<'a>(
        client: &'a Client,
        lower_mass: i64,
        upper_mass: i64,
        partition_limits: &'a [i64],
    ) -> Result<impl Stream<Item = Result<Peptide>> + 'a> {
        let prepared_statement = client
            .get_prepared_statement(&SELECT_BY_MASS_RANGE_STATEMENT)
            .await?;

        // Check which partitions are affected
        let lower_partition_index = get_mass_partition(partition_limits, lower_mass)?;
        let upper_partition_index = get_mass_partition(partition_limits, upper_mass)?;

        Ok(try_stream! {

            if lower_partition_index == upper_partition_index {
                // If the mass range is within one partition query it directly
                let stream = client
                    .execute_iter(prepared_statement, (lower_partition_index as i64, lower_mass, upper_mass))
                    .await?
                    .rows_stream::<TypedPeptideRow>()?;
                for await peptide_result in stream {
                    yield peptide_result?.into();
                }
            } else {
                // If the mass range spans multiple partitions query each partition
                #[allow(clippy::needless_range_loop)]
                for partition in lower_partition_index..=upper_partition_index {
                    let partition_limit = partition_limits[partition];
                    let lower_mass = if partition == lower_partition_index {
                        lower_mass
                    } else {
                        partition_limit
                    };
                    let upper_mass = if partition == upper_partition_index {
                        upper_mass
                    } else {
                        partition_limit
                    };

                    let stream = client
                        .execute_iter(prepared_statement.clone(), (lower_partition_index as i64, lower_mass, upper_mass))
                        .await?
                        .rows_stream::<TypedPeptideRow>()?;
                    for await peptide_result in stream {
                        yield peptide_result?.into();
                    }
                }
            }
        })
    }

    /// Selects a peptide by its sequence.
    ///
    /// # Arguments
    /// * `client` - The database client
    /// * `sequence` - The sequence of the peptide
    /// * `partition_limits` - The partition limits
    ///
    pub async fn select_by_sequence(
        client: &Client,
        sequence: &str,
        partition_limits: &[i64],
    ) -> Result<Option<Peptide>> {
        let mass = calc_sequence_mass_int(sequence)?;
        let partition = get_mass_partition(partition_limits, mass)?;

        let stream = PeptideTable::select(
            client,
            "WHERE partition = ? AND mass = ? AND sequence = ? LIMIT 1",
            &[
                &CqlValue::BigInt(partition as i64),
                &CqlValue::BigInt(mass),
                &CqlValue::Text(sequence.to_string()),
            ],
        )
        .await?;

        pin!(stream);

        stream.try_next().await
    }
}

impl Table for PeptideTable {
    fn table_name() -> &'static str {
        TABLE_NAME
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
    use crate::database::generic_client::GenericClient;
    use crate::database::scylla::client::Client;
    use crate::database::scylla::prepare_database_for_tests;
    use crate::database::scylla::tests::get_test_database_url;
    use crate::io::uniprot_text::reader::Reader;
    use crate::tools::omicstools::convert_to_internal_peptide;

    const CONFLICTING_PEPTIDE_PROTEIN_ACCESSION: &str = "P41159";

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
        let client = Client::new(&get_test_database_url()).await.unwrap();
        prepare_database_for_tests(&client).await;

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
        let conflicting_peptides = [Peptide::new(
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

        PeptideTable::bulk_upsert(&client, &mut conflicting_peptides.iter())
            .await
            .unwrap();

        PeptideTable::bulk_upsert(&client, &mut peptides.iter())
            .await
            .unwrap();

        let count_statement = format!(
            "SELECT count(*) FROM {}.{}",
            client.get_database(),
            PeptideTable::table_name()
        );
        let (count,) = client
            .query_unpaged(count_statement, &[])
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .first_row::<(i64,)>()
            .unwrap();

        assert_eq!(count as usize, EXPECTED_PEPTIDES.len());

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let stream = PeptideTable::select(
            &client,
            "WHERE partition = ? AND mass = ? and sequence = ? LIMIT 1",
            &[
                &CqlValue::BigInt(conflicting_peptides[0].get_partition()),
                &CqlValue::BigInt(conflicting_peptides[0].get_mass()),
                &CqlValue::Text(conflicting_peptides[0].get_sequence().to_owned()),
            ],
        )
        .await
        .unwrap();

        pin!(stream);

        let peptide = stream.try_next().await.unwrap().unwrap();

        assert_eq!(peptide.get_proteins().len(), 2);
        assert!(peptide
            .get_proteins()
            .contains(&leptin.get_accession().to_owned()));
        assert!(peptide
            .get_proteins()
            .contains(&CONFLICTING_PEPTIDE_PROTEIN_ACCESSION.to_owned()));

        let statement = format!(
            "SELECT is_metadata_updated FROM {}.{}",
            client.get_database(),
            PeptideTable::table_name()
        );

        let stream = client
            .query_iter(statement, [])
            .await
            .unwrap()
            .rows_stream::<(bool,)>()
            .unwrap();

        pin!(stream);

        while let Some(row) = stream.try_next().await.unwrap() {
            assert!(!row.0);
        }
    }

    /// Tests protein accession update and disassociation (removal of protein accession from peptide)
    ///
    #[tokio::test]
    #[serial]
    async fn test_accession_update() {
        let client = Client::new(&get_test_database_url()).await.unwrap();
        prepare_database_for_tests(&client).await;

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

        PeptideTable::bulk_upsert(&client, &mut peptides.iter())
            .await
            .unwrap();

        PeptideTable::remove_proteins(
            &client,
            &mut peptides.iter(),
            vec![leptin.get_accession().to_owned()]
                .into_iter()
                .collect(),
        )
        .await
        .unwrap();

        PeptideTable::add_proteins(
            &client,
            &mut peptides.iter(),
            vec![CONFLICTING_PEPTIDE_PROTEIN_ACCESSION.to_owned()]
                .into_iter()
                .collect(),
        )
        .await
        .unwrap();

        // Check that the conflicting peptide was inserted correctly with two protein accessions.
        let stream = PeptideTable::select(&client, "", &[]).await.unwrap();

        pin!(stream);

        while let Some(peptide) = stream.try_next().await.unwrap() {
            assert_eq!(peptide.get_proteins().len(), 1);
            assert_eq!(
                peptide.get_proteins()[0],
                CONFLICTING_PEPTIDE_PROTEIN_ACCESSION
            );
        }

        PeptideTable::remove_proteins(
            &client,
            &mut peptides.iter(),
            vec![CONFLICTING_PEPTIDE_PROTEIN_ACCESSION.to_owned()]
                .into_iter()
                .collect(),
        )
        .await
        .unwrap();

        let stream = PeptideTable::select(&client, "", &[]).await.unwrap();

        pin!(stream);

        while let Some(peptide) = stream.try_next().await.unwrap() {
            assert_eq!(peptide.get_proteins().len(), 0);
        }
    }

    /// Test update flagging peptides for metadata update
    /// ToDo: Split into three tests: update_protein_accession with new, update_protein_accession without new, unset_metadata
    ///
    #[tokio::test]
    #[serial]
    async fn test_flagging_for_metadata_update() {
        let client = Client::new(&get_test_database_url()).await.unwrap();
        prepare_database_for_tests(&client).await;

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

        PeptideTable::bulk_upsert(&client, &mut peptides.iter())
            .await
            .unwrap();

        let set_metadata_update_to_true_statement = format!(
            "UPDATE {}.{} SET is_metadata_updated = true WHERE partition = ? and mass = ? and sequence = ?",
            client.get_database(),
            PeptideTable::table_name()
        );

        let select_metadata_flag_statement = format!(
            "SELECT is_metadata_updated FROM {}.{}",
            client.get_database(),
            PeptideTable::table_name()
        );

        for peptide in peptides.iter() {
            client
                .query_unpaged(
                    set_metadata_update_to_true_statement.as_str(),
                    &(
                        peptide.get_partition(),
                        peptide.get_mass(),
                        peptide.get_sequence(),
                    ),
                )
                .await
                .unwrap();
        }

        let stream = client
            .query_iter(select_metadata_flag_statement.as_str(), [])
            .await
            .unwrap()
            .rows_stream::<(bool,)>()
            .unwrap();

        pin!(stream);

        while let Some(row) = stream.try_next().await.unwrap() {
            assert!(row.0);
        }

        // Update the protein accession which should set the metadata flag to false
        PeptideTable::update_protein_accession(
            &client,
            &mut peptides.iter(),
            CONFLICTING_PEPTIDE_PROTEIN_ACCESSION,
            None,
        )
        .await
        .unwrap();

        let stream = client
            .query_iter(select_metadata_flag_statement.as_str(), [])
            .await
            .unwrap()
            .rows_stream::<(bool,)>()
            .unwrap();

        pin!(stream);

        while let Some(row) = stream.try_next().await.unwrap() {
            assert!(!row.0);
        }

        for peptide in peptides.iter() {
            client
                .query_unpaged(
                    set_metadata_update_to_true_statement.as_str(),
                    &(
                        peptide.get_partition(),
                        peptide.get_mass(),
                        peptide.get_sequence(),
                    ),
                )
                .await
                .unwrap();
        }

        // Unset the metadata flag using the unset function
        PeptideTable::unset_is_metadata_updated(&client, &mut peptides.iter())
            .await
            .unwrap();

        let stream = client
            .query_iter(select_metadata_flag_statement.as_str(), &[])
            .await
            .unwrap()
            .rows_stream::<(bool,)>()
            .unwrap();

        pin!(stream);

        while let Some(row) = stream.try_next().await.unwrap() {
            assert!(!row.0);
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_select_by_sequence() {
        let client = Client::new(&get_test_database_url()).await.unwrap();
        prepare_database_for_tests(&client).await;

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

        PeptideTable::bulk_upsert(&client, &mut peptides.iter())
            .await
            .unwrap();

        let peptides = Vec::from_iter(peptides.into_iter());
        // this will pick the 10. peptide, which is a bit random as
        // the order of the peptides is not guaranteed when the HashSet is cast
        // to Vec.
        let unsafed_peptide_sequence = peptides[10].get_sequence().to_string();

        let peptide =
            PeptideTable::select_by_sequence(&client, &unsafed_peptide_sequence, &PARTITION_LIMITS)
                .await
                .unwrap()
                .unwrap();

        assert_eq!(peptide.get_sequence(), &unsafed_peptide_sequence);
    }

    #[tokio::test]
    #[serial]
    async fn test_metadata_update() {
        // Prepare DB and
        let client = Client::new(&get_test_database_url()).await.unwrap();
        prepare_database_for_tests(&client).await;

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

        PeptideTable::bulk_upsert(&client, &mut peptides.iter())
            .await
            .unwrap();

        // Convert to vector
        let peptides = Vec::from_iter(peptides.into_iter());

        // this will pick the 10. peptide, which is a bit random as
        // the order of the peptides is not guaranteed when the HashSet is cast
        // to Vec. But the updates should work regardless of the peptide
        // as only dummy data is used.
        let peptide = PeptideTable::select_by_sequence(
            &client,
            peptides[10].get_sequence(),
            &PARTITION_LIMITS,
        )
        .await
        .unwrap()
        .unwrap();

        PeptideTable::update_metadata(
            &client,
            &peptide,
            true,
            false,
            &vec![9606],
            &vec![9606],
            &vec!["UP000005640".to_owned()],
            &vec![], // no domains
        )
        .await
        .unwrap();

        let peptide = PeptideTable::select_by_sequence(
            &client,
            peptides[10].get_sequence(),
            &PARTITION_LIMITS,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(peptide.get_is_swiss_prot());
        assert!(!peptide.get_is_trembl());
        assert_eq!(peptide.get_taxonomy_ids(), &vec![9606]);
        assert_eq!(peptide.get_unique_taxonomy_ids(), &vec![9606]);
        assert_eq!(peptide.get_proteome_ids(), &vec!["UP000005640".to_owned()]);
        assert_eq!(peptide.get_domains(), &vec![]);

        let is_metadata_up_to_date_query = format!(
            "SELECT is_metadata_updated FROM {}.{} WHERE partition = ? AND mass = ? AND sequence = ?;",
            client.get_database(),
            PeptideTable::table_name()
        );

        let is_metadata_up_to_date_query_params = vec![
            CqlValue::BigInt(peptide.get_partition()),
            CqlValue::BigInt(peptide.get_mass()),
            CqlValue::Text(peptide.get_sequence().to_owned()),
        ];

        let is_metadata_up_to_date = client
            .query_unpaged(
                is_metadata_up_to_date_query,
                is_metadata_up_to_date_query_params.as_slice(),
            )
            .await
            .unwrap()
            .into_rows_result()
            .unwrap()
            .first_row::<(bool,)>()
            .unwrap()
            .0;

        assert!(is_metadata_up_to_date);
    }

    #[tokio::test]
    #[serial]
    async fn test_tokenawareness() {
        let client = Client::new(&get_test_database_url()).await.unwrap();
        prepare_database_for_tests(&client).await;
        for statement in &[
            INSERT_STATEMENT.as_str(),
            UPSERT_STATEMENT.as_str(),
            SET_METADATA_TO_FALSE_STATEMENT.as_str(),
            REMOVE_PROTEINS_STATEMENT.as_str(),
            ADD_PROTEINS_STATEMENT.as_str(),
            UPDATE_METADATA_STATEMENT.as_str(),
            SELECT_BY_MASS_RANGE_STATEMENT.as_str(),
        ] {
            let prepared_statement = client.get_prepared_statement(statement).await.unwrap();
            assert!(
                prepared_statement.is_token_aware(),
                "Statement `{}` is not token aware",
                statement
            );
        }
    }
}
