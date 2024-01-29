// std imports
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

// 3rd party imports
use indicatif::ProgressStyle;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{info_span, Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

/// update interval for the progress bar in ms
///
const UPDATE_INTERVAL: u64 = 300;

pub struct ProgressView;

impl ProgressView {
    /// Creates a progress bar for the given progress values
    /// Make sure to call it with '.instrument(<span>)' to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `progresses` - Vector of progress values
    /// * `progresses_max` - Vector of maximum values for the progress bars (if None, the progress is displayed as value)
    /// * `labels` - Vector of labels for the progress bars
    /// * `stop_flag` - Flag to stop the progress bar
    /// * `update_interval_override` - Override for the update interval (default: [UPDATE_INTERVAL])
    ///
    pub fn create(
        title: &str,
        progresses: Vec<Arc<AtomicUsize>>,
        progresses_max: Vec<Option<u64>>,
        labels: Vec<String>,
        stop_flag: Arc<AtomicBool>,
        update_interval_override: Option<u64>,
    ) -> JoinHandle<()> {
        let progress_span = info_span!("");
        progress_span.pb_set_message(title);
        tokio::spawn(
            Self::view(
                progresses,
                progresses_max,
                labels,
                stop_flag,
                update_interval_override,
            )
            .instrument(progress_span),
        )
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
    ) {
        let _ = Span::current().enter();

        let update_interval = update_interval_override.unwrap_or(UPDATE_INTERVAL);
        let progress_spans: Vec<Span> = progresses
            .iter()
            .enumerate()
            .map(|(progress_idx, _)| {
                let progress_bar = info_span!("");
                progress_bar.pb_set_message(&labels[progress_idx]);
                if let Some(max) = progresses_max[progress_idx] {
                    progress_bar.pb_set_style(&ProgressStyle::default_bar());
                    progress_bar.pb_set_length(max);
                    progress_bar.pb_set_position(0);
                }
                progress_bar
            })
            .collect();

        while stop_flag.load(std::sync::atomic::Ordering::Relaxed) == false {
            for (progress_idx, progress) in progresses.iter().enumerate() {
                let progress_span = &progress_spans[progress_idx];
                let _ = progress_span.enter();
                if let Some(_) = progresses_max[progress_idx] {
                    progress_span
                        .pb_set_position(progress.load(std::sync::atomic::Ordering::Relaxed) as u64);
                } else {
                    progress_span.pb_set_message(&format!(
                        "{} {}",
                        progress.load(std::sync::atomic::Ordering::Relaxed),
                        labels[progress_idx]
                    ));
                }
            }
            tokio::time::sleep(Duration::from_millis(update_interval)).await;
        }
    }
}
