use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufReader},
    num::NonZeroUsize,
    path::PathBuf,
};

use clap::Parser;
use fallible_iterator::FallibleIterator;
use futures::{StreamExt, future::join_all};
use lru::LruCache;
use macpepdb::{
    mass_index::{IsMassIndexMap, MassIndex, MassIndexDbMap},
    peptide::Peptide,
    protease::Protease,
};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
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

    let protease = Protease::get_by_name("trypsin", Some(6), Some(50), Some(2)).unwrap();
    let now = std::time::Instant::now();
    let index = build_mass_index(&protease, &cli.protein_file_paths).await;

    let elapsed = now.elapsed();
    println!(
        "Index: build = {:.2?} s; #masses = {}; #peptides = {}",
        elapsed.as_secs_f32(),
        index.len().await.unwrap(),
        index.entries_len().await.unwrap()
    );

    let now = std::time::Instant::now();
    cssndr_build_db(
        &protease,
        &index,
        cli.peptides_insert_batch_size,
        cli.protien_reader_cache_size,
    )
    .await;
    let elapsed = now.elapsed();
    println!("DB (strseq): build = {:.2?} s;", elapsed.as_secs_f32(),);
}

async fn build_mass_index(
    protease: &Protease,
    protein_file_paths: &[PathBuf],
) -> MassIndex<MassIndexDbMap> {
    let client: Session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .known_node("127.0.0.1:9043")
        .use_keyspace("macpepdb", true)
        .build()
        .await
        .unwrap();

    let mass_index_map = MassIndexDbMap::new(client);
    let mut index = MassIndex::new(protein_file_paths.to_vec(), mass_index_map);
    index.build(protease).await.unwrap();

    index
}

async fn cssndr_build_db<'a>(
    protease: &Protease,
    index: &'a MassIndex<MassIndexDbMap>,
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
        .prepare(Peptide::cssndr_insert_statement())
        .await
        .unwrap();

    let mut protein_reader_cache: LruCache<&'a PathBuf, IndexedReader<BufReader<File>>> =
        LruCache::new(protien_reader_cache_size);

    let mut masses = index
        .masses()
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, <MassIndexDbMap as IsMassIndexMap>::Error>>()
        .unwrap();
    masses.sort();

    #[allow(clippy::mutable_key_type)]
    let mut peptide_buffer: HashSet<Peptide> = HashSet::with_capacity(1000);

    for mass in masses.into_iter() {
        let entries = index.get(*mass).await.unwrap().unwrap();

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

        for entry in entries.iter() {
            let path = file_paths.get(&entry.file_idx()).unwrap();
            let reader = protein_reader_cache
                .try_get_or_insert_mut(path, || {
                    let file = File::open(path)?;
                    Ok::<IndexedReader<_>, io::Error>(IndexedReader::new(BufReader::new(file)))
                })
                .unwrap();
            let entry = reader.read(entry.offset()).unwrap();

            #[allow(clippy::mutable_key_type)]
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
    use deku::prelude::*;

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
