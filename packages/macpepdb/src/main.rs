use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufReader},
    num::NonZeroUsize,
    path::PathBuf,
};

use clap::{Parser, ValueEnum};
use fallible_iterator::FallibleIterator;
use futures::future::join_all;
use lru::LruCache;
use macpepdb::{
    mass_index::MassIndex,
    peptide::Peptide,
    protease::Protease,
    sequence::{
        BitSequence, IsSequence, byte_array_sequence::ByteArraySequence,
        string_sequence::StringSequence,
    },
};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use tokio_postgres::NoTls;
use uniprot_reader::reader::IndexedReader;

/// Seqeunce typey to use
///
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum DatabaseTypes {
    Sql,
    Cql,
}

/// Seqeunce typey to use
///
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum SequenceTypes {
    Bit,
    ByteArray,
    String,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional path to save index
    #[arg(short, long)]
    index_file_path: Option<PathBuf>,
    /// Batch size of peptides to insert
    #[arg(short, long, default_value_t = 1000)]
    peptides_insert_batch_size: usize,
    /// Batch size of peptides to insert
    #[arg(short, long, default_value_t = NonZeroUsize::new(1000).unwrap())]
    protien_reader_cache_size: NonZeroUsize,
    /// Types of sequence to use, multiple are possible
    #[arg(short, long, value_enum, action = clap::ArgAction::Append)]
    sequence_type: Vec<SequenceTypes>,
    #[arg(short, long, value_enum, default_value_t = DatabaseTypes::Sql)]
    database_type: DatabaseTypes,
    /// Protein files
    #[arg(value_delimiter = ' ', num_args = 0..)]
    protein_file_paths: Vec<PathBuf>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let bytea_protease =
        Protease::<ByteArraySequence>::get_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();
    let now = std::time::Instant::now();
    let index = build_mass_index(
        &bytea_protease,
        &cli.protein_file_paths,
        cli.index_file_path,
    )
    .await;
    let elapsed = now.elapsed();
    println!(
        "Index: build = {:.2?} s; #masses = {}; #peptides = {}",
        elapsed.as_secs_f32(),
        index.len(),
        index.entry_len()
    );
    drop(bytea_protease);

    if cli.database_type == DatabaseTypes::Sql {
        if cli.sequence_type.contains(&SequenceTypes::Bit) {
            let bit_protease =
                Protease::<BitSequence>::get_by_name("trypsin", Some(6), Some(50), Some(2))
                    .unwrap();
            let now = std::time::Instant::now();
            psql_build_db(
                &bit_protease,
                &index,
                cli.peptides_insert_batch_size,
                cli.protien_reader_cache_size,
            )
            .await;
            let elapsed = now.elapsed();
            println!("DB (bitseq): build = {:.2?} s;", elapsed.as_secs_f32(),);
        }

        if cli.sequence_type.contains(&SequenceTypes::String) {
            let str_protease =
                Protease::<StringSequence>::get_by_name("trypsin", Some(6), Some(50), Some(2))
                    .unwrap();
            let now = std::time::Instant::now();
            psql_build_db(
                &str_protease,
                &index,
                cli.peptides_insert_batch_size,
                cli.protien_reader_cache_size,
            )
            .await;
            let elapsed = now.elapsed();
            println!("DB (strseq): build = {:.2?} s;", elapsed.as_secs_f32(),);
        }

        if cli.sequence_type.contains(&SequenceTypes::ByteArray) {
            let bytea_protease =
                Protease::<ByteArraySequence>::get_by_name("trypsin", Some(6), Some(50), Some(2))
                    .unwrap();
            let now = std::time::Instant::now();
            psql_build_db(
                &bytea_protease,
                &index,
                cli.peptides_insert_batch_size,
                cli.protien_reader_cache_size,
            )
            .await;
            let elapsed = now.elapsed();
            println!("DB (strseq): build = {:.2?} s;", elapsed.as_secs_f32(),);
        }
    } else {
        if cli.sequence_type.contains(&SequenceTypes::Bit) {
            let bit_protease =
                Protease::<BitSequence>::get_by_name("trypsin", Some(6), Some(50), Some(2))
                    .unwrap();
            let now = std::time::Instant::now();
            cssndr_build_db(
                &bit_protease,
                &index,
                cli.peptides_insert_batch_size,
                cli.protien_reader_cache_size,
            )
            .await;
            let elapsed = now.elapsed();
            println!("DB (bitseq): build = {:.2?} s;", elapsed.as_secs_f32(),);
        }

        if cli.sequence_type.contains(&SequenceTypes::String) {
            let str_protease =
                Protease::<StringSequence>::get_by_name("trypsin", Some(6), Some(50), Some(2))
                    .unwrap();
            let now = std::time::Instant::now();
            cssndr_build_db(
                &str_protease,
                &index,
                cli.peptides_insert_batch_size,
                cli.protien_reader_cache_size,
            )
            .await;
            let elapsed = now.elapsed();
            println!("DB (strseq): build = {:.2?} s;", elapsed.as_secs_f32(),);
        }

        if cli.sequence_type.contains(&SequenceTypes::ByteArray) {
            let bytea_protease =
                Protease::<ByteArraySequence>::get_by_name("trypsin", Some(6), Some(50), Some(2))
                    .unwrap();
            let now = std::time::Instant::now();
            cssndr_build_db(
                &bytea_protease,
                &index,
                cli.peptides_insert_batch_size,
                cli.protien_reader_cache_size,
            )
            .await;
            let elapsed = now.elapsed();
            println!("DB (strseq): build = {:.2?} s;", elapsed.as_secs_f32(),);
        }
    }
}

async fn build_mass_index<S: IsSequence>(
    protease: &Protease<S>,
    protein_file_paths: &[PathBuf],
    output_path: Option<PathBuf>,
) -> MassIndex {
    let index = MassIndex::create(protein_file_paths, protease).unwrap();

    if let Some(output_path) = output_path {
        let buf_writer = std::io::BufWriter::new(std::fs::File::create(output_path).unwrap());
        serde_json::to_writer(buf_writer, &index).unwrap();
    }

    index
}

async fn psql_build_db<'a, S: IsSequence>(
    protease: &Protease<S>,
    index: &'a MassIndex,
    peptides_insert_batch_size: usize,
    protien_reader_cache_size: NonZeroUsize,
) {
    let (mut client, connection) =
        tokio_postgres::connect("postgresql://postgres@127.0.0.1:5432/postgres", NoTls)
            .await
            .unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let insert_statement = client
        .prepare(&Peptide::<S>::psql_insert_statement())
        .await
        .unwrap();

    let mut protein_reader_cache: LruCache<&'a PathBuf, IndexedReader<BufReader<File>>> =
        LruCache::new(protien_reader_cache_size);

    let mut masses = index.masses().collect::<Vec<_>>();
    masses.sort();

    let mut peptide_buffer: HashSet<Peptide<S>> = HashSet::with_capacity(1000);

    for mass in masses.into_iter() {
        let entries = index.map().get(mass).unwrap();

        // Get relevant paths
        let file_paths = entries
            .iter()
            .map(|entry| {
                (
                    entry.file_idx(),
                    index.files().get(entry.file_idx()).unwrap(),
                )
            })
            .collect::<HashMap<usize, &PathBuf>>();

        for entry in entries {
            let path = file_paths.get(&entry.file_idx()).unwrap();
            let reader = protein_reader_cache
                .try_get_or_insert_mut(path, || {
                    let file = File::open(path)?;
                    Ok::<IndexedReader<_>, io::Error>(IndexedReader::new(BufReader::new(file)))
                })
                .unwrap();
            let entry = reader.read(entry.offset()).unwrap();

            let mut peptides = protease
                .cleave(entry.sequence(), true)
                .unwrap()
                .filter(|peptide| Ok(peptide.mass() == *mass))
                .collect::<HashSet<_>>()
                .unwrap();

            peptide_buffer.extend(peptides.drain());

            if peptide_buffer.len() >= peptides_insert_batch_size {
                let transaction = client.transaction().await.unwrap();
                for peptide in peptide_buffer.drain() {
                    peptide
                        .psql_insert_with_preped_statement(&transaction, &insert_statement)
                        .await
                        .unwrap();
                }
                transaction.commit().await.unwrap();
            }
        }
    }
    if !peptide_buffer.is_empty() {
        let transaction = client.transaction().await.unwrap();
        for peptide in peptide_buffer.drain() {
            peptide.psql_insert(&transaction).await.unwrap();
        }
        transaction.commit().await.unwrap();
    }
}

async fn cssndr_build_db<'a, S: IsSequence>(
    protease: &Protease<S>,
    index: &'a MassIndex,
    peptides_insert_batch_size: usize,
    protien_reader_cache_size: NonZeroUsize,
) {
    let mut db_secs: f64 = 0.0;

    let client: Session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .known_node("127.0.0.1:9043")
        .use_keyspace("macpepdb", true)
        .build()
        .await
        .unwrap();

    let insert_statement = client
        .prepare(Peptide::<S>::cssndr_insert_statement())
        .await
        .unwrap();

    let mut protein_reader_cache: LruCache<&'a PathBuf, IndexedReader<BufReader<File>>> =
        LruCache::new(protien_reader_cache_size);

    let mut masses = index.masses().collect::<Vec<_>>();
    masses.sort();

    let mut peptide_buffer: HashSet<Peptide<S>> = HashSet::with_capacity(1000);

    for mass in masses.into_iter() {
        let entries = index.map().get(mass).unwrap();

        // Get relevant paths
        let file_paths = entries
            .iter()
            .map(|entry| {
                (
                    entry.file_idx(),
                    index.files().get(entry.file_idx()).unwrap(),
                )
            })
            .collect::<HashMap<usize, &PathBuf>>();

        for entry in entries {
            let path = file_paths.get(&entry.file_idx()).unwrap();
            let reader = protein_reader_cache
                .try_get_or_insert_mut(path, || {
                    let file = File::open(path)?;
                    Ok::<IndexedReader<_>, io::Error>(IndexedReader::new(BufReader::new(file)))
                })
                .unwrap();
            let entry = reader.read(entry.offset()).unwrap();

            let mut peptides = protease
                .cleave(entry.sequence(), true)
                .unwrap()
                .filter(|peptide| Ok(peptide.mass() == *mass))
                .collect::<HashSet<_>>()
                .unwrap();

            peptide_buffer.extend(peptides.drain());

            if peptide_buffer.len() >= peptides_insert_batch_size {
                let inserts_future = peptide_buffer.drain().map(|peptide| {
                    peptide.cssndr_insert_with_preped_statement_owned(&client, &insert_statement)
                });

                let db_now = std::time::Instant::now();
                join_all(inserts_future)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                db_secs += db_now.elapsed().as_secs_f64();
            }
        }
    }
    if !peptide_buffer.is_empty() {
        let inserts_future = peptide_buffer.drain().map(|peptide| {
            peptide.cssndr_insert_with_preped_statement_owned(&client, &insert_statement)
        });

        let db_now = std::time::Instant::now();
        join_all(inserts_future)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        db_secs += db_now.elapsed().as_secs_f64();
    }

    println!("Total DB insert time: {:.2} s", db_secs);
}

#[cfg(test)]
mod tests {
    use bitvec::prelude::*;
    use deku::prelude::*;

    static BIT_CODE_LEN: usize = 5;

    type Bit5 = BitArr!(for BIT_CODE_LEN, in u8, Lsb0);

    // const A: Bit5 = {
    //     type This = ::bitvec::array::BitArray<[u8; ::bitvec::mem::elts::<u8>(BIT_CODE_LEN)], Lsb0>;
    //     This {
    //         data: 1_u8..This::ZERO,
    //     }
    // };
    // const B: Bit5 = Bit5::new([0b00010]);

    // struct Seq(BitArray<u8, Lsb0>);

    #[test]
    fn test_bitvec() {
        let bit5 = Bit5::new([33]);
        println!("bit5: {bit5:?}");

        bit5.iter().for_each(|b| println!("{b}"));
    }

    #[derive(Debug, Eq, PartialEq, DekuRead, DekuWrite)]
    #[deku(id_type = "u8", bits = "5")]
    enum Aa {
        #[deku(id = 0)]
        A,
        #[deku(id = 1)]
        B,
    }

    #[derive(Debug, Eq, PartialEq, DekuRead, DekuWrite)]
    struct DSeq {
        #[deku(update = "self.items.len() as u8")]
        count: u8,
        #[deku(count = "count")]
        items: Vec<Aa>,
    }

    impl DSeq {
        fn new(aas: Vec<Aa>) -> Self {
            Self {
                count: aas.len() as u8,
                items: aas,
            }
        }
    }

    #[test]
    fn tes_deku() {
        let seq = DSeq::new(vec![
            Aa::A,
            Aa::B,
            Aa::A,
            Aa::A,
            Aa::B,
            Aa::A,
            Aa::A,
            Aa::B,
            Aa::A,
        ]);

        let bytes = seq.to_bytes().unwrap();
        println!("{:?}", bytes);

        let (_, seq2) = DSeq::from_bytes((bytes.as_slice(), 0)).unwrap();

        println!("seq2: {seq2:?}");
        assert_eq!(seq, seq2);
    }
}
