use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use indicatif::ProgressStyle;
use tokio::time::{sleep, Instant};
use tracing::{info_span, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

pub async fn performance_log_thread(
    num_proteins: &usize,
    num_proteins_processed: Arc<Mutex<i32>>,
    stop_flag: Arc<AtomicBool>,
) {
    let performance_span = info_span!("insertion_performance");
    performance_span.pb_set_style(&ProgressStyle::default_bar());
    performance_span.pb_set_length(*num_proteins as u64);
    let performance_span_enter = performance_span.enter();

    let mut prev_num_proteins_processed = 0;

    let interval = Duration::from_secs(1);
    let mut next_time = Instant::now() + interval;
    // This loop runs exactly every second
    loop {
        if stop_flag.load(Ordering::Relaxed) {
            break;
        }

        let num_proteins_processed = *num_proteins_processed.lock().unwrap();

        let delta = num_proteins_processed - prev_num_proteins_processed;
        Span::current().pb_inc(delta as u64);
        prev_num_proteins_processed = num_proteins_processed;

        sleep(next_time - Instant::now());
        next_time += interval;
    }

    std::mem::drop(performance_span_enter);
    std::mem::drop(performance_span);
}
