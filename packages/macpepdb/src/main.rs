use std::{
    collections::VecDeque,
    fs::File,
    io::BufReader,
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
};

use clap::Parser;
use macpepdb::{
    blob::Blob,
    client::Client,
    mass_counter::MassCounter,
    mass_index::MassIndex,
    mass_partitioner::{MassPartitioner, Partitioning},
    peptide_table::PeptideTable,
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
    #[arg(short, long, default_value_t = NonZeroUsize::new(1000).unwrap())]
    insert_batch_size: NonZeroUsize,
    /// Number of mass partition
    #[arg(short, long, default_value_t = NonZeroU32::new(1000).unwrap())]
    partitions: NonZeroU32,
    /// Batch size of peptides to insert
    #[arg(short, long, default_value_t = NonZeroUsize::new(1000).unwrap())]
    protien_reader_cache_size: NonZeroUsize,
    /// Batch size of records to insert concurrently
    #[arg(short, long, default_value_t = NonZeroUsize::new(16).unwrap())]
    threads: NonZeroUsize,
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
        cli.partitions,
    )
    .await;
}

async fn build_db(
    client: Arc<Client>,
    protein_file_paths: &[PathBuf],
    insert_batch_size: NonZeroUsize,
    protease: &Protease,
    num_threads: NonZeroUsize,
    num_partitions: NonZeroU32,
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

    // 3. count masses for partitions
    let now = std::time::Instant::now();
    let (mass_ctr, peptide_ctr) =
        build_db_mass_counter(client.clone(), insert_batch_size, protease, num_threads).await;
    println!(
        "db mass counter: time = {:.2?} s; #masses = {mass_ctr}; #peptides = {peptide_ctr}",
        now.elapsed().as_secs_f32(),
    );

    // 4. caluclate partitioning by going through masses and count peptides
    // partitioning is currently not implemented, let's see first if we need it.
    let now = std::time::Instant::now();
    let partitioning =
        build_db_parititoning(client.clone(), insert_batch_size, num_partitions).await;
    println!(
        "db partitioning: time = {:.2?} s;",
        now.elapsed().as_secs_f32(),
    );

    // 5. go through masses and digest the proteins collect distinct peptides and upsert them with proteins
    let now = std::time::Instant::now();
    build_db_peptides(
        client.clone(),
        insert_batch_size,
        protease,
        &partitioning,
        num_threads,
    )
    .await;
    println!("db peptides = {:.2?} s;", now.elapsed().as_secs_f32(),);

    // 6. Collect metadata
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

async fn build_db_parititoning(
    client: Arc<Client>,
    insert_batch_size: NonZeroUsize,
    num_partitions: NonZeroU32,
) -> Partitioning {
    match Blob::select(client.as_ref(), Partitioning::BLOB_KEY)
        .await
        .unwrap()
    {
        Some(partitioning) => partitioning,
        None => {
            let partitioning = MassPartitioner::new(client.clone())
                .build(num_partitions)
                .await
                .unwrap();
            Blob::insert(client.as_ref(), &partitioning, insert_batch_size)
                .await
                .unwrap();

            partitioning
        }
    }
}

async fn build_db_peptides(
    client: Arc<Client>,
    insert_batch_size: NonZeroUsize,
    protease: &Protease,
    partitioning: &Partitioning,
    num_threads: NonZeroUsize,
) {
    PeptideTable::new(client)
        .build_concurrently(protease, insert_batch_size, num_threads, partitioning)
        .await
        .unwrap();
}
