use std::{collections::VecDeque, fs::File, io::BufReader, num::NonZeroUsize, path::PathBuf};

use clap::Parser;
use macpepdb::{client::Client, protein::Protein};

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

    let client = Client::new(&cli.database_url).await.unwrap();

    let now = std::time::Instant::now();
    build_db(&client, &cli.protein_file_paths, cli.insert_batch_size).await;
    let elapsed = now.elapsed();
    println!("DB (strseq): build = {:.2?} s;", elapsed.as_secs_f32(),);
}

async fn build_db(
    client: &Client,
    protein_file_paths: &[PathBuf],
    insert_batch_size: NonZeroUsize,
) {
    // First set insert proteins
    build_db_proteins(client, protein_file_paths, insert_batch_size).await;
    // Second step create mass to protein index
    // Third step whent through masses and digest the proteins collect distingt peptdes and insert
}

async fn build_db_proteins(
    client: &Client,
    protein_file_paths: &[PathBuf],
    insert_batch_size: NonZeroUsize,
) {
    for protein_file_path in protein_file_paths {
        let mut buf_reader = BufReader::new(File::open(protein_file_path).unwrap());
        let entry_reader = uniprot_reader::reader::Reader::new(&mut buf_reader);

        let mut buffer: VecDeque<Protein> = VecDeque::with_capacity(insert_batch_size.get());

        for entry in entry_reader {
            let protein = Protein::try_from(entry.unwrap().entry()).unwrap();
            buffer.push_back(protein);
            if buffer.len() == 1000 {
                Protein::insert_batch(client, buffer.drain(..))
                    .await
                    .unwrap();
            }
        }

        Protein::insert_batch(client, buffer.drain(..))
            .await
            .unwrap();
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
