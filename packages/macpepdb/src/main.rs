use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufReader},
    num::NonZeroUsize,
    path::PathBuf,
};

use clap::Parser;
use fallible_iterator::FallibleIterator;
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
use tokio_postgres::NoTls;
use uniprot_reader::reader::IndexedReader;

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
    /// Protein files
    #[arg(value_delimiter = ' ', num_args = 0..)]
    protein_file_paths: Vec<PathBuf>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let bit_protease =
        Protease::<BitSequence>::get_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();
    let str_protease =
        Protease::<StringSequence>::get_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();
    let bytea_protease =
        Protease::<ByteArraySequence>::get_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();

    let now = std::time::Instant::now();
    let index = build_mass_index(&bit_protease, &cli.protein_file_paths, cli.index_file_path).await;
    let elapsed = now.elapsed();
    println!(
        "Index: build = {:.2?} s; #masses = {}; #peptides = {}",
        elapsed.as_secs_f32(),
        index.len(),
        index.entry_len()
    );

    let now = std::time::Instant::now();
    build_db(
        &bit_protease,
        &index,
        cli.peptides_insert_batch_size,
        cli.protien_reader_cache_size,
    )
    .await;
    let elapsed = now.elapsed();
    println!("DB (bitseq): build = {:.2?} s;", elapsed.as_secs_f32(),);

    let now = std::time::Instant::now();
    build_db(
        &str_protease,
        &index,
        cli.peptides_insert_batch_size,
        cli.protien_reader_cache_size,
    )
    .await;
    let elapsed = now.elapsed();
    println!("DB (strseq): build = {:.2?} s;", elapsed.as_secs_f32(),);

    let now = std::time::Instant::now();
    build_db(
        &bytea_protease,
        &index,
        cli.peptides_insert_batch_size,
        cli.protien_reader_cache_size,
    )
    .await;
    let elapsed = now.elapsed();
    println!("DB (strseq): build = {:.2?} s;", elapsed.as_secs_f32(),);
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

async fn build_db<'a, S: IsSequence>(
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
                    peptide.insert(&transaction).await.unwrap();
                }
                transaction.commit().await.unwrap();
            }
        }
    }
    if !peptide_buffer.is_empty() {
        let transaction = client.transaction().await.unwrap();
        for peptide in peptide_buffer.drain() {
            peptide.insert(&transaction).await.unwrap();
        }
        transaction.commit().await.unwrap();
    }
}
