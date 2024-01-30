// std imports
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

// 3rd party imports
use anyhow::{bail, Result};
use indicatif::ProgressStyle;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{info_span, Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

/// update interval for the progress bar in ms
///
const UPDATE_INTERVAL: u64 = 1000;

/// Progress style, used when no maximum value is given
///
const THROUGHPUT_STYLE: &'static str = "        {msg} {len}, {pos}/s";

/// Creates a tracing span shown the throughput of given metrics
pub struct ThroughputMonitor {
    thread_handle: Option<JoinHandle<Result<()>>>, // Wrapped in Option to be able to take it when await join
    stop_flag: Arc<AtomicBool>,
}

impl ThroughputMonitor {
    /// Creates a progress view for the given progress values
    /// Make sure to call it with '.instrument(<span>)' to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `progresses` - Vector of progress values
    /// * `progresses_max` - Vector of maximum values for the progress bars (if None, the progress is displayed as value)
    /// * `labels` - Vector of labels for the progress bars
    /// * `update_interval_override` - Override for the update interval (default: [UPDATE_INTERVAL])
    ///
    pub fn new(title: &str, metrics: Vec<Arc<AtomicUsize>>, labels: Vec<String>) -> Result<Self> {
        if metrics.len() != labels.len() {
            bail!("Number of progresses, progress max and labels must be equal")
        }
        let stop_flag = Arc::new(AtomicBool::new(false));
        let progress_span = info_span!("");
        progress_span.pb_set_message(title);
        let thread_handle = Some(tokio::spawn(
            Self::view(metrics, labels, stop_flag.clone()).instrument(progress_span),
        ));

        Ok(Self {
            thread_handle,
            stop_flag,
        })
    }

    /// Creates a progress bar for the given progress values
    /// Make sure to call it with '.instrument(<span>)' to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `metrics` - Vector of metrics
    /// * `labels` - Vector of labels for the progress bars
    /// * `stop_flag` - Flag to stop the progress bar
    /// * `update_interval_override` - Override for the update interval (default: 1000ms)
    ///
    async fn view(
        metrics: Vec<Arc<AtomicUsize>>,
        labels: Vec<String>,
        stop_flag: Arc<AtomicBool>,
    ) -> Result<()> {
        let _ = Span::current().enter();
        let throughput_spans = metrics
            .iter()
            .enumerate()
            .map(|(progress_idx, _)| {
                let progress_span = info_span!("");
                progress_span.pb_set_message(&labels[progress_idx]);
                progress_span.pb_set_position(0);
                progress_span.pb_set_style(&ProgressStyle::with_template(THROUGHPUT_STYLE)?);
                Ok(progress_span)
            })
            .collect::<Result<Vec<Span>>>()?;

        let mut previous_metrics = metrics
            .iter()
            .map(|metric| metric.load(std::sync::atomic::Ordering::Relaxed))
            .collect::<Vec<_>>();

        while stop_flag.load(std::sync::atomic::Ordering::Relaxed) == false {
            let current_metrics = metrics
                .iter()
                .map(|metric| metric.load(std::sync::atomic::Ordering::Relaxed))
                .collect::<Vec<_>>();

            for (metrics_idx, (span, previous_metric)) in throughput_spans
                .iter()
                .zip(previous_metrics.iter_mut())
                .enumerate()
            {
                let _ = span.enter();
                let current_metric = current_metrics[metrics_idx];
                span.pb_set_length(current_metric as u64);
                span.pb_set_position((current_metric - *previous_metric) as u64);
                *previous_metric = current_metric;
            }
            previous_metrics = current_metrics;
            tokio::time::sleep(Duration::from_millis(UPDATE_INTERVAL)).await;
        }
        Ok(())
    }

    /// Stops the progress bar
    ///
    pub async fn stop(&mut self) -> Result<()> {
        self.stop_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        match self.thread_handle.take() {
            Some(handle) => handle.await??,
            None => {}
        }
        Ok(())
    }
}
