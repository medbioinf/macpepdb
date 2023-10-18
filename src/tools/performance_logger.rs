// std imports
use std::{
    cmp,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::sleep,
    time::Duration,
};

use anyhow::Result;
use log::error;
use tokio::sync::mpsc::Receiver;
use tokio::{fs::File, time::Instant};
use tracing::{info, info_span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::entities::protein::Protein;

macro_rules! async_writeln {
    ($dst: expr) => {
        {
            tokio::io::AsyncWriteExt::write_all(&mut $dst, b"\n").await
        }
    };
    ($dst: expr, $fmt: expr) => {
        {
            use std::io::Write;
            let mut buf = Vec::<u8>::new();
            writeln!(buf, $fmt)?;
            tokio::io::AsyncWriteExt::write_all(&mut $dst, &buf).await
        }
    };
    ($dst: expr, $fmt: expr, $($arg: tt)*) => {
        {
            use std::io::Write;
            let mut buf = Vec::<u8>::new();
            writeln!(buf, $fmt, $( $arg )*)?;
            tokio::io::AsyncWriteExt::write_all(&mut $dst, &buf).await
        }
    };
}

pub async fn performance_log_receiver(
    mut receiver: Receiver<u64>,
    num_processed: Arc<Mutex<u64>>,
) -> Result<()> {
    loop {
        match receiver.recv().await {
            Some(count) => {
                let mut i = num_processed.lock().unwrap();
                *i += count;
            }
            None => break,
        }
    }

    Ok(())
}

pub async fn performance_log_thread(
    num_proteins_processed: Arc<Mutex<u64>>,
    num_peptides_processed: Arc<Mutex<u64>>,
    protein_queue_arc: Arc<Mutex<Vec<Protein>>>,
    stop_flag: Arc<AtomicBool>,
) -> Result<()> {
    let protein_performance_span = info_span!("protein_performance");
    let protein_performance_span_enter = protein_performance_span.enter();
    let peptide_performance_span = info_span!("peptide_performance");
    let peptide_performance_span_enter = peptide_performance_span.enter();

    let mut prev_num_proteins_processed = 0;
    let mut prev_num_peptides_processed = 0;

    let interval = Duration::from_secs(1);
    let mut next_time = Instant::now() + interval;
    let start_time = Instant::now();

    // This loop runs exactly every second
    loop {
        let num_proteins_processed = *num_proteins_processed.lock().unwrap();
        let delta_proteins = num_proteins_processed - prev_num_proteins_processed;
        let seconds_expired = (Instant::now() - start_time).as_secs();
        let proteins_per_second = num_proteins_processed / cmp::max(1, seconds_expired);

        let num_peptides_processed = *num_peptides_processed.lock().unwrap();
        let delta_peptides = num_peptides_processed - prev_num_peptides_processed;
        let seconds_expired = (Instant::now() - start_time).as_secs();
        let peptides_per_second = num_peptides_processed / cmp::max(1, seconds_expired);
        // num_proteins_processed_last_interval += delta;

        if stop_flag.load(Ordering::Relaxed) {
            break;
        }

        let protein_queue_size = {
            let protein_queue = match protein_queue_arc.lock() {
                Ok(protein_queue) => protein_queue,
                Err(err) => {
                    error!("Could not lock protein queue {}", err);
                    continue;
                }
            };

            protein_queue.len()
        };

        protein_performance_span.pb_set_message(
            format!(
                "Just processed {}\tThroughput: {}\tTotal processed: {}\tQueue size: {}",
                delta_proteins, proteins_per_second, num_proteins_processed, protein_queue_size,
            )
            .as_str(),
        );
        peptide_performance_span.pb_set_message(
            format!(
                "Just processed {}\tThroughput: {}\tTotal processed: {}",
                delta_peptides, peptides_per_second, num_peptides_processed
            )
            .as_str(),
        );
        prev_num_proteins_processed = num_proteins_processed;
        prev_num_peptides_processed = num_peptides_processed;

        sleep(next_time - Instant::now());
        next_time += interval;
    }

    let num_proteins_processed = *num_proteins_processed.lock().unwrap();
    let num_peptides_processed = *num_peptides_processed.lock().unwrap();
    let total_seconds = (Instant::now() - start_time).as_secs();
    info!("Processed {} proteins", num_proteins_processed);
    info!("Processed {} peptides", num_peptides_processed);
    info!("Finished in {} seconds", total_seconds);
    info!(
        "Overall {} Prot/sec",
        num_proteins_processed / total_seconds
    );
    info!("Overall {} Pep/sec", num_peptides_processed / total_seconds);

    std::mem::drop(protein_performance_span_enter);
    std::mem::drop(protein_performance_span);
    std::mem::drop(peptide_performance_span_enter);
    std::mem::drop(peptide_performance_span);

    Ok(())
}

pub async fn metadata_update_performance_log_thread(
    num_peptides_processed: Arc<Mutex<u64>>,
    stop_flag: Arc<AtomicBool>,
) -> Result<()> {
    let peptide_performance_span = info_span!("metadata_update_performance");
    let peptide_performance_span_enter = peptide_performance_span.enter();

    let mut prev_num_peptides_processed = 0;

    let interval = Duration::from_secs(1);
    let mut next_time = Instant::now() + interval;
    let start_time = Instant::now();

    // This loop runs exactly every second
    loop {
        let num_peptides_processed = *num_peptides_processed.lock().unwrap();
        let delta_peptides = num_peptides_processed - prev_num_peptides_processed;
        let seconds_expired = (Instant::now() - start_time).as_secs();
        let peptides_per_second = num_peptides_processed / cmp::max(1, seconds_expired);
        // num_proteins_processed_last_interval += delta;

        if stop_flag.load(Ordering::Relaxed) {
            break;
        }

        peptide_performance_span.pb_set_message(
            format!(
                "(Pep) Just updated {}\tThroughput: {}\tTotal processed: {}",
                delta_peptides, peptides_per_second, num_peptides_processed
            )
            .as_str(),
        );
        prev_num_peptides_processed = num_peptides_processed;

        sleep(next_time - Instant::now());
        next_time += interval;
    }

    let num_peptides_processed = *num_peptides_processed.lock().unwrap();
    let total_seconds = (Instant::now() - start_time).as_secs();
    info!("Updated {} peptides", num_peptides_processed);
    info!("Finished in {} seconds", total_seconds);
    info!("Overall {} Pep/sec", num_peptides_processed / total_seconds);

    std::mem::drop(peptide_performance_span_enter);
    std::mem::drop(peptide_performance_span);

    Ok(())
}

pub async fn performance_csv_logger(
    num_proteins_processed: Arc<Mutex<u64>>,
    num_peptides_processed: Arc<Mutex<u64>>,
    log_folder: &str,
    stop_flag: Arc<AtomicBool>,
) -> Result<()> {
    const LOG_INTERVAL_SECONDS: u64 = 5;
    let interval = Duration::from_secs(LOG_INTERVAL_SECONDS);
    let mut next_time = Instant::now() + interval;
    let start_time = Instant::now();

    let mut prev_num_proteins_processed = 0;
    let mut prev_num_peptides_processed = 0;

    let mut file = File::create(format!("{}/performance.csv", log_folder)).await?;
    async_writeln!(
        file,
        "seconds,processed_proteins,processed_peptides,proteins/sec,peptides/sec"
    )?;

    loop {
        let seconds_expired = (Instant::now() - start_time).as_secs();

        let num_proteins_processed = *num_proteins_processed.lock().unwrap();
        let delta_proteins = num_proteins_processed - prev_num_proteins_processed;
        let proteins_per_second = delta_proteins / LOG_INTERVAL_SECONDS;

        let num_peptides_processed = *num_peptides_processed.lock().unwrap();
        let delta_peptides = num_peptides_processed - prev_num_peptides_processed;
        let peptides_per_second = delta_peptides / LOG_INTERVAL_SECONDS;

        async_writeln!(
            file,
            "{},{},{},{},{}",
            seconds_expired,
            num_proteins_processed,
            num_peptides_processed,
            proteins_per_second,
            peptides_per_second
        )?;

        if stop_flag.load(Ordering::Relaxed) {
            break;
        }

        prev_num_proteins_processed = num_proteins_processed;
        prev_num_peptides_processed = num_peptides_processed;

        sleep(next_time - Instant::now());
        next_time += interval;
    }
    Ok(())
}
