// std imports
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use indicatif::ProgressStyle;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{info_span, Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

// internal imports
use crate::database::scylla::client::Client;

/// Progress bar style. Used when a maximum value is given
///
const PROGRESS_BAR_STYLE: &str = "        {msg} {pos} ";

/// update interval for the progress bar in ms
///
const UPDATE_INTERVAL: u64 = 1000;

/// Labels
const LABELS: [&str; 7] = [
    "Queries requested",
    "Iter queries requested",
    "Errors occurred",
    "Iter errors occurred",
    "Average latency",
    "99.9 latency percentile",
    "Retries",
];

/// Creates a tracing span with multiple progress (bars)
pub struct ScyllaClientMetricsMonitor {
    thread_handle: Option<JoinHandle<Result<()>>>, // Wrapped in Option to be able to take it when await join
    stop_flag: Arc<AtomicBool>,
}

impl ScyllaClientMetricsMonitor {
    /// Creates a progress view for the given progress values
    /// Make sure to call it with '.instrument(<span>)' to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `title` - Title for the progress bar
    /// * `client` - The scylla client
    /// * `update_interval_override` - Override for the update interval (default: [UPDATE_INTERVAL])
    ///
    pub fn new<T>(
        title: &str,
        client: Arc<Client>,
        update_interval_override: Option<u64>,
    ) -> Result<Self>
    where
        T: Send + Sync + 'static,
    {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let update_interval = update_interval_override.unwrap_or(UPDATE_INTERVAL);
        let progress_span = info_span!("");
        progress_span.pb_set_message(title);
        let thread_handle = Some(tokio::spawn(
            Self::view(client, stop_flag.clone(), update_interval).instrument(progress_span),
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
    /// * `client` - The scylla client
    /// * `stop_flag` - Flag to stop the progress bar
    /// * `update_interval_override` - Override for the update interval (default: 1000ms)
    ///
    async fn view(
        client: Arc<Client>,
        stop_flag: Arc<AtomicBool>,
        update_interval: u64,
    ) -> Result<()> {
        let _ = Span::current().enter();
        let status_spans: Vec<Span> = LABELS
            .iter()
            .map(|label| {
                let progress_span = info_span!("");
                progress_span.pb_set_message(label);
                progress_span.pb_set_position(0);
                progress_span.pb_set_style(&ProgressStyle::with_template(PROGRESS_BAR_STYLE)?);

                Ok(progress_span)
            })
            .collect::<Result<Vec<Span>>>()?;

        while !stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
            let metrics = Self::get_metrics_vec(client.as_ref());
            for (span_idx, metric) in metrics.iter().enumerate() {
                let progress_span = &status_spans[span_idx];
                let _ = progress_span.enter();
                progress_span.pb_set_position(*metric);
            }
            tokio::time::sleep(Duration::from_millis(update_interval)).await;
        }
        Ok(())
    }

    pub fn get_metrics_vec(client: &Client) -> Vec<u64> {
        let metrics = client.get_metrics();
        vec![
            metrics.get_queries_num(),
            metrics.get_queries_iter_num(),
            metrics.get_errors_num(),
            metrics.get_errors_iter_num(),
            metrics.get_latency_avg_ms().unwrap_or(2 * 64),
            metrics.get_latency_percentile_ms(99.9).unwrap_or(2 * 64),
            metrics.get_retries_num(),
        ]
    }

    /// Stops the progress bar
    ///
    pub async fn stop(&mut self) -> Result<()> {
        self.stop_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(handle) = self.thread_handle.take() {
            handle.await??
        }
        Ok(())
    }
}
