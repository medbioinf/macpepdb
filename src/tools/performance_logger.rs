use std::{
    cmp,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::sleep,
    time::Duration,
};

use anyhow::bail;
use anyhow::Result;
use log::error;
use tokio::{fs::File, io::AsyncWriteExt, time::Instant};
use tracing::{info, info_span, Span};
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

pub async fn performance_log_thread(
    num_proteins: &usize,
    num_proteins_processed: Arc<Mutex<u64>>,
    protein_queue_arc: Arc<Mutex<Vec<Protein>>>,
    stop_flag: Arc<AtomicBool>,
) -> Result<()> {
    let performance_span = info_span!("insertion_performance");
    let performance_span_enter = performance_span.enter();

    let mut prev_num_proteins_processed = 0;

    let interval = Duration::from_secs(1);
    let mut next_time = Instant::now() + interval;
    let start_time = Instant::now();

    let mut i = 1;
    const FILE_LOG_INTERVAL: u64 = 10;
    let mut num_proteins_processed_last_interval = 0;
    let mut file = File::create("performance.csv").await?;
    async_writeln!(file, "seconds,processed_proteins,proteins/sec")?;

    // This loop runs exactly every second
    loop {
        let num_proteins_processed = *num_proteins_processed.lock().unwrap();
        let delta = num_proteins_processed - prev_num_proteins_processed;
        let seconds_expired = (Instant::now() - start_time).as_secs();
        let proteins_per_second = num_proteins_processed / cmp::max(1, seconds_expired);
        num_proteins_processed_last_interval += delta;

        if stop_flag.load(Ordering::Relaxed) {
            async_writeln!(
                file,
                "{},{},{}",
                i,
                num_proteins_processed,
                num_proteins_processed_last_interval / FILE_LOG_INTERVAL
            )?;
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

        if i % FILE_LOG_INTERVAL == 0 {
            async_writeln!(
                file,
                "{},{},{}",
                i,
                num_proteins_processed,
                num_proteins_processed_last_interval / FILE_LOG_INTERVAL
            )?;
            num_proteins_processed_last_interval = 0;
        }

        performance_span.pb_set_message(
            format!(
                "Just processed: {}\tP/sec: {}\tTotal processed: {}\tQueue size: {}",
                delta, proteins_per_second, num_proteins_processed, protein_queue_size
            )
            .as_str(),
        );

        prev_num_proteins_processed = num_proteins_processed;

        sleep(next_time - Instant::now());
        next_time += interval;
        i += 1;
    }

    let num_proteins_processed = *num_proteins_processed.lock().unwrap();
    let total_seconds = (Instant::now() - start_time).as_secs();
    info!("Processed {} proteins", num_proteins_processed);
    info!("Finished in {} seconds", total_seconds);
    info!("Overall {} P/sec", num_proteins_processed / total_seconds);

    std::mem::drop(performance_span_enter);
    std::mem::drop(performance_span);

    Ok(())
}
