use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::{fs::read_to_string, path::Path};

// 3rd party imports
use anyhow::{bail, Result};
use clap::Parser;
use crossbeam_queue::ArrayQueue;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification as PTM;
use futures::StreamExt;
use indicatif::ProgressStyle;
use rand::prelude::SliceRandom;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::runtime::Builder;
use tokio::time::sleep;
use tokio_util::io::StreamReader;
use tracing::{error, Level};
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

// local imports
use macpepdb::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use macpepdb::database::generic_client::GenericClient;
use macpepdb::database::scylla::client::Client;
use macpepdb::database::scylla::configuration_table::ConfigurationTable;
use macpepdb::database::scylla::peptide_search::{MultiTaskSearch, Search};
use macpepdb::entities::configuration::Configuration;
use macpepdb::io::post_translational_modification_csv::reader::Reader as PtmReader;
use macpepdb::mass::convert::{to_float as mass_to_float, to_int as mass_to_int};
use macpepdb::tools::metrics_logger::MetricsLogger;
use macpepdb::tools::progress_monitor::ProgressMonitor;
use macpepdb::tools::queue_monitor::QueueMonitor;

const STOP_FLAG_CHECK_TIMEOUT: u64 = 1000;

#[derive(serde::Serialize)]
struct JsonArraySerializablePtm {
    amino_acid: String,
    mass_delta: f64,
    mod_type: String,
    position: String,
}

#[derive(Debug, Parser)]
#[command(name = "performance")]
struct Cli {
    /// Verbosity level
    /// 0 - Error
    /// 1 - Warn
    /// 2 - Info
    /// 3 - Debug
    /// > 3 - Trace
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    /// Log interval for metrics in seconds
    #[arg(long, default_value_t = 60)]
    metrics_log_interval: u64,
    /// Use only x first masses
    #[arg(long)]
    only_n_masses: Option<usize>,
    /// Number of threads for queued multi-threaded filter
    #[arg(long, default_value_t = 1)]
    threads: usize,
    /// Shuffle masses if set
    #[arg(long, action = clap::ArgAction::SetTrue)]
    shuffle: bool,
    /// MaCPepDB URL to connect e.g. scylla://host1,host2/keyspace or http(s)://host:port
    macpepdb_url: String,
    /// Input file with masses to query
    masses_file: String,
    /// PTM file
    ptm_file: String,
    /// Lower mass tolerance (ppm)
    lower_mass_tolerance: i64,
    /// Upper mass tolerance (ppm)
    upper_mass_tolerance: i64,
    /// Maximum number of variable modifications
    max_variable_modifications: i16,
    /// Folder to log
    log_folder: String,
}

async fn db_searching(
    _thread_id: usize,
    database_url: String,
    mass_queue: Arc<ArrayQueue<i64>>,
    processed_masses: Arc<AtomicUsize>,
    matching_peptides: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: i16,
    ptms: Vec<PTM>,
    _log_folder: PathBuf,
) -> Result<()> {
    let client = Arc::new(Client::new(&database_url).await?);
    let config: Configuration = ConfigurationTable::select(&client).await?;
    let partition_limits = Arc::new(config.get_partition_limits().clone());
    drop(config);

    loop {
        let mass = match mass_queue.pop() {
            Some(mass) => mass,
            None => break,
        };
        tracing::debug!("Processing mass: {}", mass);

        let mut filtered_stream = MultiTaskSearch::search(
            client.clone(),
            partition_limits.clone(),
            mass,
            lower_mass_tolerance_ppm,
            upper_mass_tolerance_ppm,
            max_variable_modifications,
            true,
            None,
            None,
            None,
            ptms.clone(),
            None,
        )
        .await?;
        while let Some(peptide) = filtered_stream.next().await {
            match peptide {
                Ok(_) => matching_peptides.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                Err(_) => errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            };
        }
        tracing::debug!("Finished mass: {}", mass);
        processed_masses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(())
}

async fn web_searching(
    thread_id: usize,
    web_url: String,
    mass_queue: Arc<ArrayQueue<i64>>,
    processed_masses: Arc<AtomicUsize>,
    matching_peptides: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: i16,
    ptms: Vec<PTM>,
    log_folder: PathBuf,
) -> Result<()> {
    let client = reqwest::Client::new();

    let endpoint = format!("{}/api/peptides/search", web_url);

    loop {
        let mass = match mass_queue.pop() {
            Some(mass) => mass,
            None => break,
        };
        tracing::debug!("Processing mass: {}", mass);
        let fasta_file_path = log_folder.join(format!("web_{}_{}.fasta", thread_id, mass));
        let mut fasta_file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(fasta_file_path)
            .await?;

        let mut was_successful = false;

        while !was_successful {
            was_successful = true;
            let peptide_response = client
                .post(&endpoint)
                .header("Connection", "close")
                .header("Accept", "text/plain")
                .json(&serde_json::json!({
                    "mass": mass_to_float(mass),
                    "lower_mass_tolerance_ppm": lower_mass_tolerance_ppm,
                    "upper_mass_tolerance_ppm": upper_mass_tolerance_ppm,
                    "max_variable_modifications": max_variable_modifications,
                    "modifications": &ptms,
                }))
                .send()
                .await?;

            if !peptide_response.status().is_success() {
                error!(
                    "Request was not successfull: {}, {:?} => Retry",
                    mass,
                    peptide_response.text().await?
                );
                was_successful = false;
                continue;
            }

            let peptide_stream = peptide_response.bytes_stream().map(|result| {
                result.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
            });
            let mut read = StreamReader::new(peptide_stream);

            let mut line = String::new();
            loop {
                line.clear();
                match read.read_line(&mut line).await {
                    Ok(len) => {
                        if len == 0 {
                            break;
                        }
                        matching_peptides.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        fasta_file.write_all(line.as_bytes()).await?;
                        len
                    }
                    Err(err) => {
                        errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        error!("Error while processing mass: {}, {:?} => Retry", mass, err);
                        was_successful = false;
                        break;
                    }
                };
            }
        }
        tracing::debug!("Finished mass: {}", mass);
        processed_masses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(())
}

fn search_thread(
    thread_id: usize,
    macpepdb_url: String,
    mass_queue: Arc<ArrayQueue<i64>>,
    processed_masses: Arc<AtomicUsize>,
    matching_peptides: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: i16,
    ptms: Vec<PTM>,
    log_folder: PathBuf,
) -> Result<()> {
    if macpepdb_url.starts_with("scylla://") {
        Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(db_searching(
                thread_id,
                macpepdb_url,
                mass_queue,
                processed_masses,
                matching_peptides,
                errors,
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                max_variable_modifications,
                ptms,
                log_folder,
            ))?;
    } else if macpepdb_url.starts_with("http://") || macpepdb_url.starts_with("https://") {
        Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(web_searching(
                thread_id,
                macpepdb_url,
                mass_queue,
                processed_masses,
                matching_peptides,
                errors,
                lower_mass_tolerance_ppm,
                upper_mass_tolerance_ppm,
                max_variable_modifications,
                ptms,
                log_folder,
            ))?;
    }

    Ok(())
}

async fn monitoring(
    mass_queue: Arc<ArrayQueue<i64>>,
    processed_masses: Arc<AtomicUsize>,
    matching_peptides: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
    total_masses: u64,
    log_folder: PathBuf,
    log_interval: u64,
    stop_flag: Arc<AtomicBool>,
) -> Result<()> {
    let metrics_file_path = log_folder.join("metrics.csv");
    let mut metrics_logger = MetricsLogger::new(
        vec![
            processed_masses.clone(),
            matching_peptides.clone(),
            errors.clone(),
        ],
        vec![
            "masses".to_string(),
            "matching peptides".to_string(),
            "errors".to_string(),
        ],
        metrics_file_path,
        log_interval,
    )?;

    let mut progress_monitor = ProgressMonitor::new(
        "",
        vec![processed_masses, matching_peptides, errors],
        vec![Some(total_masses), None, None],
        vec![
            "masses".to_string(),
            "matching peptides".to_string(),
            "errors".to_string(),
        ],
        None,
    )?;

    let mut queue_monitor = QueueMonitor::new(
        "",
        vec![mass_queue],
        vec![total_masses],
        vec!["mass queue".to_string()],
        None,
    )?;

    while !stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
        sleep(tokio::time::Duration::from_millis(STOP_FLAG_CHECK_TIMEOUT)).await;
    }

    progress_monitor.stop().await?;
    metrics_logger.stop().await?;
    queue_monitor.stop().await?;

    Ok(())
}

fn monitor_thread(
    mass_queue: Arc<ArrayQueue<i64>>,
    processed_masses: Arc<AtomicUsize>,
    matching_peptides: Arc<AtomicUsize>,
    errors: Arc<AtomicUsize>,
    total_masses: u64,
    log_folder: PathBuf,
    log_interval: u64,
    stop_flag: Arc<AtomicBool>,
) -> Result<()> {
    Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(monitoring(
            mass_queue,
            processed_masses,
            matching_peptides,
            errors,
            total_masses,
            log_folder,
            log_interval,
            stop_flag,
        ))?;

    Ok(())
}

fn main() -> Result<()> {
    // Parser CLI arguments
    let args = Cli::parse();

    // Set verbosity
    let verbosity = match args.verbose {
        0 => Level::ERROR,
        1 => Level::WARN,
        2 => Level::INFO,
        3 => Level::DEBUG,
        _ => Level::TRACE,
    };

    let filter = EnvFilter::from_default_env()
        .add_directive(verbosity.into())
        .add_directive("scylla=info".parse().unwrap())
        .add_directive("tokio_postgres=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap());

    let indicatif_layer = IndicatifLayer::new()
    .with_progress_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} {span_child_prefix} {span_name} {span_fields} {wide_msg} {elapsed}",
        )
        .unwrap(),
    )
    .with_span_child_prefix_symbol("↳ ")
    .with_span_child_prefix_indent(" ");

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
        .with(indicatif_layer)
        .with(filter)
        .init();

    let mut masses: Vec<i64> = read_to_string(Path::new(&args.masses_file))?
        .split("\n")
        .filter_map(|line| {
            if !line.is_empty() {
                let mass = mass_to_int(line.parse::<f64>().unwrap());
                Some(mass)
            } else {
                None
            }
        })
        .collect();

    if args.shuffle {
        masses.shuffle(&mut rand::thread_rng());
    }

    if let Some(limit) = args.only_n_masses {
        masses.truncate(limit);
    }

    let total_masses = masses.len() as u64;

    let ptms = PtmReader::read(Path::new(&args.ptm_file))?;
    let log_folder = Path::new(&args.log_folder).to_path_buf();

    let mass_queue = Arc::new(ArrayQueue::new(masses.len()));
    for mass in masses.into_iter() {
        match mass_queue.push(mass) {
            Ok(_) => {}
            Err(err) => bail!("Failed to push mass to queue, {:?}", err),
        }
    }

    let processed_masses = Arc::new(AtomicUsize::new(0));
    let matching_peptides = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));

    let stop_flag = Arc::new(AtomicBool::new(false));

    let monitor_mass_queue = mass_queue.clone();
    let monitor_processed_masses = processed_masses.clone();
    let monitor_matching_peptides = matching_peptides.clone();
    let monitor_errors = errors.clone();
    let monitor_total_masses = total_masses;
    let monitor_stop_flag = stop_flag.clone();
    let monitor_log_interval = args.metrics_log_interval;
    let monitor_log_folder = log_folder.clone();

    let monitor_handle = std::thread::spawn(move || {
        monitor_thread(
            monitor_mass_queue,
            monitor_processed_masses,
            monitor_matching_peptides,
            monitor_errors,
            monitor_total_masses,
            monitor_log_folder,
            monitor_log_interval,
            monitor_stop_flag,
        )?;
        Ok::<(), anyhow::Error>(())
    });

    let search_handles: Vec<std::thread::JoinHandle<Result<(), anyhow::Error>>> = (0..args.threads)
        .map(|thread_id| {
            let mass_queue = mass_queue.clone();
            let processed_masses = processed_masses.clone();
            let matching_peptides = matching_peptides.clone();
            let errors = errors.clone();
            let macpepdb_url = args.macpepdb_url.clone();
            let lower_mass_tolerance_ppm = args.lower_mass_tolerance;
            let upper_mass_tolerance_ppm = args.upper_mass_tolerance;
            let max_variable_modifications = args.max_variable_modifications;
            let ptms: Vec<PTM> = ptms.clone();
            let log_folder = log_folder.clone();
            std::thread::spawn(move || {
                search_thread(
                    thread_id,
                    macpepdb_url,
                    mass_queue,
                    processed_masses,
                    matching_peptides,
                    errors,
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
                    max_variable_modifications,
                    ptms,
                    log_folder,
                )?;
                Ok::<(), anyhow::Error>(())
            })
        })
        .collect();

    for handle in search_handles {
        match handle.join() {
            Ok(res) => {
                if let Err(err) = res {
                    bail!("Search thread error, {:?}", err)
                }
            }
            Err(err) => bail!("Failed to join search thread, {:?}", err),
        }
    }

    stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);

    match monitor_handle.join() {
        Ok(_) => {}
        Err(err) => bail!("Failed to join monitor thread, {:?}", err),
    }

    Ok(())
}
