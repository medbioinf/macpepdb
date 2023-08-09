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
use log::error;
use tokio::time::Instant;
use tracing::{info, info_span, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::entities::protein::Protein;

pub async fn performance_log_thread(
    num_proteins: &usize,
    num_proteins_processed: Arc<Mutex<u64>>,
    protein_queue_arc: Arc<Mutex<Vec<Protein>>>,
    stop_flag: Arc<AtomicBool>,
) {
    let performance_span = info_span!("insertion_performance");
    let performance_span_enter = performance_span.enter();

    let mut prev_num_proteins_processed = 0;

    let interval = Duration::from_secs(1);
    let mut next_time = Instant::now() + interval;
    let start_time = Instant::now();
    // This loop runs exactly every second
    loop {
        if stop_flag.load(Ordering::Relaxed) {
            break;
        }

        let num_proteins_processed = *num_proteins_processed.lock().unwrap();

        let delta = num_proteins_processed - prev_num_proteins_processed;
        let seconds_expired = (Instant::now() - start_time).as_secs();

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

        performance_span.pb_set_message(
            format!(
                "Processed {} new proteins\t{} P/sec\t{} in queue",
                delta,
                num_proteins_processed / cmp::max(1, seconds_expired),
                protein_queue_size
            )
            .as_str(),
        );
        prev_num_proteins_processed = num_proteins_processed;

        sleep(next_time - Instant::now());
        next_time += interval;
    }

    let num_proteins_processed = *num_proteins_processed.lock().unwrap();
    let total_seconds = (Instant::now() - start_time).as_secs();
    info!("Processed {} proteins", num_proteins_processed);
    info!("Finished in {} seconds", total_seconds);
    info!("Overall {} P/sec", num_proteins_processed / total_seconds);

    std::mem::drop(performance_span_enter);
    std::mem::drop(performance_span);
}
