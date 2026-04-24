use std::{
    collections::{HashSet, VecDeque},
    fs::File,
    io::BufReader,
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
};

use clap::Parser;
use fallible_iterator::FallibleIterator;
use futures::StreamExt;
use itertools::Itertools;
use macpepdb::{
    client::Client,
    mass_counter::MassCounter,
    mass_index::MassIndex,
    peptide::Peptide,
    protease::Protease,
    protein::Protein,
    sequence::{IsSequence, PeptideSequence},
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional path to save index
    // #[arg(short, long)]
    // index_file_path: Option<PathBuf>,
    /// Database URL, format `scylla://[<user:string>[:<url-safe-password:string>]@]<host[:<port>]>[,<host[:<port>]>...]/<keyspace>`
    #[arg(short, long, default_value_t = String::from("scylla://127.0.0.1:9042,127.0.0.1:9043/macpepdb"))]
    database_url: String,
    /// Batch size of records to insert concurrently
    #[arg(short, long, default_value_t = NonZeroUsize::new(16).unwrap())]
    threads: NonZeroUsize,
    /// Batch size of records to insert concurrently
    #[arg(short, long, default_value_t = NonZeroUsize::new(1000).unwrap())]
    insert_batch_size: NonZeroUsize,
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

    let client = Arc::new(Client::new(&cli.database_url).await.unwrap());
    let protease = Protease::get_by_name(
        "trypsin",
        Some(PeptideSequence::MIN_LENGTH.get()),
        Some(PeptideSequence::MAX_LENGTH.get()),
        Some(2),
    )
    .unwrap();

    build_db(
        client,
        &cli.protein_file_paths,
        cli.insert_batch_size,
        &protease,
        cli.threads,
    )
    .await;
}

async fn build_db(
    client: Arc<Client>,
    protein_file_paths: &[PathBuf],
    insert_batch_size: NonZeroUsize,
    protease: &Protease,
    num_threads: NonZeroUsize,
) {
    // 1. set insert proteins
    let now = std::time::Instant::now();
    let protein_ctr =
        build_db_proteins(client.as_ref(), protein_file_paths, insert_batch_size).await;
    println!(
        "db proteins: time = {:.2?} s; #proteins = {protein_ctr}",
        now.elapsed().as_secs_f32(),
    );

    // 2. step create mass to protein index
    let now = std::time::Instant::now();
    build_db_mass_index(client.clone(), insert_batch_size, protease, num_threads).await;
    println!(
        "db mass index: time = {:.2?} s;",
        now.elapsed().as_secs_f32(),
    );

    // 2.1 count masses for partitions
    let now = std::time::Instant::now();
    let (mass_ctr, peptide_ctr) =
        build_db_mass_counter(client.clone(), insert_batch_size, protease, num_threads).await;
    println!(
        "db mass counter: time = {:.2?} s; #masses = {mass_ctr}; #peptides = {peptide_ctr}",
        now.elapsed().as_secs_f32(),
    );

    // 2.1 caluclate partitioning by going through masses and count peptides
    // partitioning is currently not implemented, let's see first if we need it.

    // Third step go through masses and digest the proteins collect distinct peptides and insert
    let now = std::time::Instant::now();
    build_db_peptides(client.clone(), insert_batch_size, protease).await;
    println!("db peptides = {:.2?} s;", now.elapsed().as_secs_f32(),);
}

async fn build_db_proteins(
    client: &Client,
    protein_file_paths: &[PathBuf],
    insert_batch_size: NonZeroUsize,
) -> usize {
    let mut protein_ctr: usize = 0;
    let mut buffer: VecDeque<Protein> = VecDeque::with_capacity(insert_batch_size.get());

    for protein_file_path in protein_file_paths {
        let mut buf_reader = BufReader::new(File::open(protein_file_path).unwrap());
        let entry_reader = uniprot_reader::reader::Reader::new(&mut buf_reader);

        for entry in entry_reader {
            let protein = Protein::try_from(entry.unwrap().entry()).unwrap();
            buffer.push_back(protein);
            if buffer.len() == insert_batch_size.get() {
                protein_ctr += buffer.len();
                Protein::insert_batch(client, buffer.drain(..))
                    .await
                    .unwrap();
            }
        }
    }

    protein_ctr += buffer.len();
    Protein::insert_batch(client, buffer.drain(..))
        .await
        .unwrap();

    protein_ctr
}

async fn build_db_mass_index(
    client: Arc<Client>,
    insert_batch_size: NonZeroUsize,
    protease: &Protease,
    num_threads: NonZeroUsize,
) {
    let index = MassIndex::new(client);

    index
        .build_concurrently(protease, insert_batch_size, num_threads)
        .await
        .unwrap()
}

async fn build_db_mass_counter(
    client: Arc<Client>,
    insert_batch_size: NonZeroUsize,
    protease: &Protease,
    threads: NonZeroUsize,
) -> (usize, usize) {
    let counter = MassCounter::new(client);

    counter
        .count_concurrently(protease, insert_batch_size, threads)
        .await
        .unwrap()
}

async fn build_db_peptides(
    client: Arc<Client>,
    insert_batch_size: NonZeroUsize,
    protease: &Protease,
) {
    let index = MassIndex::new(client.clone());
    let now = std::time::Instant::now();
    let mut masses = index
        .masses()
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    masses.sort();
    println!(
        "db peptides: get masses = {:.2?} s;",
        now.elapsed().as_secs_f32(),
    );

    for mass in masses.into_iter() {
        let now = std::time::Instant::now();
        #[allow(clippy::mutable_key_type)]
        let mut peptides: HashSet<Peptide> = HashSet::new();
        let mass_entry = match index.get(mass).await.unwrap() {
            Some(mass_entry) => mass_entry,
            None => continue,
        };

        let accession_ref_vec = mass_entry.proteins().iter().collect::<Vec<_>>();

        let mut proteins = Protein::select(
            client.as_ref(),
            Some("WHERE accession IN ?"),
            (accession_ref_vec,),
        )
        .await
        .unwrap();

        while let Some(protein) = proteins.next().await {
            let protein = protein.unwrap();
            peptides.extend(
                protease
                    .cleave(protein.sequence().to_string().as_str(), true)
                    .unwrap()
                    .collect::<Vec<_>>()
                    .unwrap(),
            );
        }
        println!(
            "\tdb peptides: get proteins and digest = {:.2?} s; mass = {mass}; peptides = {}",
            now.elapsed().as_secs_f32(),
            peptides.len(),
        );

        let now = std::time::Instant::now();
        let peptides = peptides.into_iter().collect::<Vec<_>>();
        for peptide_chunk in &peptides.into_iter().chunks(insert_batch_size.get()) {
            Peptide::insert_batch(client.as_ref(), peptide_chunk)
                .await
                .unwrap();
        }
        println!(
            "\tdb peptides: insert peptides = {:.2?} s; mass = {mass}",
            now.elapsed().as_secs_f32(),
        );
    }
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
