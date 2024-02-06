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

/// Progress bar style. Used when a maximum value is given
///
const PROGRESS_BAR_STYLE: &'static str = "        {msg} {wide_bar} {pos}/{len} {per_sec} ";

/// Progress style, used when no maximum value is given
///
const PROGRESS_PLAIN_STYLE: &'static str = "        {msg} {pos} {per_sec} ";

/// Creats a tracing span with multiple progress (bars)
pub struct ProgressMonitor {
    thread_handle: Option<JoinHandle<Result<()>>>, // Wrapped in Option to be able to take it when await join
    stop_flag: Arc<AtomicBool>,
}

impl ProgressMonitor {
    /// Creates a progress view for the given progress values
    /// Make sure to call it with '.instrument(<span>)' to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `progresses` - Vector of progress values
    /// * `progresses_max` - Vector of maximum values for the progress bars (if None, the progress is displayed as value)
    /// * `labels` - Vector of labels for the progress bars
    /// * `update_interval_override` - Override for the update interval (default: [UPDATE_INTERVAL])
    ///
    pub fn new(
        title: &str,
        progresses: Vec<Arc<AtomicUsize>>,
        progresses_max: Vec<Option<u64>>,
        labels: Vec<String>,
        update_interval_override: Option<u64>,
    ) -> Result<Self> {
        if progresses.len() != progresses_max.len() || progresses.len() != labels.len() {
            bail!("Number of progresses, progress max and labels must be equal")
        }
        let stop_flag = Arc::new(AtomicBool::new(false));
        let progress_span = info_span!("");
        progress_span.pb_set_message(title);
        let thread_handle = Some(tokio::spawn(
            Self::view(
                progresses,
                progresses_max,
                labels,
                stop_flag.clone(),
                update_interval_override,
            )
            .instrument(progress_span),
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
    /// * `progresses` - Vector of progress values
    /// * `progresses_max` - Vector of maximum values for the progress bars (if None, the progress is displayed as value)
    /// * `labels` - Vector of labels for the progress bars
    /// * `stop_flag` - Flag to stop the progress bar
    /// * `update_interval_override` - Override for the update interval (default: 1000ms)
    ///
    async fn view(
        progresses: Vec<Arc<AtomicUsize>>,
        progresses_max: Vec<Option<u64>>,
        labels: Vec<String>,
        stop_flag: Arc<AtomicBool>,
        update_interval_override: Option<u64>,
    ) -> Result<()> {
        let _ = Span::current().enter();
        let update_interval = update_interval_override.unwrap_or(UPDATE_INTERVAL);
        let progress_spans = progresses
            .iter()
            .enumerate()
            .map(|(progress_idx, _)| {
                let progress_span = info_span!("");
                progress_span.pb_set_message(&labels[progress_idx]);
                progress_span.pb_set_position(0);
                if let Some(max) = progresses_max[progress_idx] {
                    progress_span.pb_set_style(&ProgressStyle::with_template(PROGRESS_BAR_STYLE)?);
                    progress_span.pb_set_length(max);
                } else {
                    progress_span
                        .pb_set_style(&ProgressStyle::with_template(PROGRESS_PLAIN_STYLE)?);
                }
                Ok(progress_span)
            })
            .collect::<Result<Vec<Span>>>()?;

        while stop_flag.load(std::sync::atomic::Ordering::Relaxed) == false {
            for (progress_idx, progress) in progresses.iter().enumerate() {
                let progress_span = &progress_spans[progress_idx];
                let _ = progress_span.enter();
                progress_span
                    .pb_set_position(progress.load(std::sync::atomic::Ordering::Relaxed) as u64);
            }
            tokio::time::sleep(Duration::from_millis(update_interval)).await;
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
