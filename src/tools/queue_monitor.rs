// std imports
use std::sync::Arc;
use std::sync::{atomic::AtomicBool, Mutex};

// 3rd party imports
use anyhow::{bail, Result};
use crossbeam_queue::ArrayQueue;
use indicatif::ProgressStyle;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{info_span, Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

/// update interval for the progress bar in ms
///
const UPDATE_INTERVAL: u64 = 300;

/// Progress bar style. Used when a maximum value is given
///
const PROGRESS_BAR_STYLE: &'static str = "        {msg} {wide_bar} {pos}/{len}";

/// Creates a tracing span with multiple progress (bars)
pub struct QueueMonitor {
    thread_handle: Option<JoinHandle<Result<()>>>, // Wrapped in Option to be able to take it when await join
    stop_flag: Arc<AtomicBool>,
}

impl QueueMonitor {
    /// Creates a progress view for the given progress values
    /// Make sure to call it with '.instrument(<span>)' to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `progresses` - Vector of progress values
    /// * `progresses_max` - Vector of maximum values for the progress bars (if None, the progress is displayed as value)
    /// * `labels` - Vector of labels for the progress bars
    /// * `update_interval_override` - Override for the update interval (default: [UPDATE_INTERVAL])
    ///
    pub fn new<T>(
        title: &str,
        queues: Vec<Arc<Mutex<Vec<T>>>>,
        queue_sizes: Vec<u64>,
        labels: Vec<String>,
        update_interval_override: Option<u64>,
    ) -> Result<Self>
    where
        T: Send + Sync + 'static,
    {
        if queues.len() != queue_sizes.len() && queues.len() != labels.len() {
            bail!("Number of queues, queue sizes and labels must be equal")
        }

        let stop_flag = Arc::new(AtomicBool::new(false));
        let progress_span = info_span!("");
        progress_span.pb_set_message(title);
        let thread_handle = Some(tokio::spawn(
            Self::view(
                queues,
                queue_sizes,
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
    async fn view<T>(
        queues: Vec<Arc<Mutex<Vec<T>>>>,
        queue_sizes: Vec<u64>,
        labels: Vec<String>,
        stop_flag: Arc<AtomicBool>,
        update_interval_override: Option<u64>,
    ) -> Result<()> {
        let _ = Span::current().enter();
        let update_interval = update_interval_override.unwrap_or(UPDATE_INTERVAL);
        let status_spans: Vec<Span> = queue_sizes
            .iter()
            .zip(labels.iter())
            .map(|(queue_size, label)| {
                let progress_span = info_span!("");
                progress_span.pb_set_message(label);
                progress_span.pb_set_position(0);
                progress_span.pb_set_style(&ProgressStyle::with_template(PROGRESS_BAR_STYLE)?);
                progress_span.pb_set_length(*queue_size);

                Ok(progress_span)
            })
            .collect::<Result<Vec<Span>>>()?;

        while stop_flag.load(std::sync::atomic::Ordering::Relaxed) == false {
            for (queue, status_span) in queues.iter().zip(status_spans.iter()) {
                let _ = status_span.enter();
                match queue.lock() {
                    Ok(queue) => {
                        status_span.pb_set_position(queue.len() as u64);
                    }
                    Err(_) => {
                        bail!("Could not lock queue for getting status")
                    }
                }
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

/// Creates a tracing span with multiple progress (bars)
pub struct ArrayQueueMonitor {
    thread_handle: Option<JoinHandle<Result<()>>>, // Wrapped in Option to be able to take it when await join
    stop_flag: Arc<AtomicBool>,
}

impl ArrayQueueMonitor {
    /// Creates a progress view for the given progress values
    /// Make sure to call it with '.instrument(<span>)' to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `progresses` - Vector of progress values
    /// * `progresses_max` - Vector of maximum values for the progress bars (if None, the progress is displayed as value)
    /// * `labels` - Vector of labels for the progress bars
    /// * `update_interval_override` - Override for the update interval (default: [UPDATE_INTERVAL])
    ///
    pub fn new<T>(
        title: &str,
        queues: Vec<Arc<ArrayQueue<T>>>,
        queue_sizes: Vec<u64>,
        labels: Vec<String>,
        update_interval_override: Option<u64>,
    ) -> Result<Self>
    where
        T: Send + Sync + 'static,
    {
        if queues.len() != queue_sizes.len() && queues.len() != labels.len() {
            bail!("Number of queues, queue sizes and labels must be equal")
        }

        let stop_flag = Arc::new(AtomicBool::new(false));
        let progress_span = info_span!("");
        progress_span.pb_set_message(title);
        let thread_handle = Some(tokio::spawn(
            Self::view(
                queues,
                queue_sizes,
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
    async fn view<T>(
        queues: Vec<Arc<ArrayQueue<T>>>,
        queue_sizes: Vec<u64>,
        labels: Vec<String>,
        stop_flag: Arc<AtomicBool>,
        update_interval_override: Option<u64>,
    ) -> Result<()> {
        let _ = Span::current().enter();
        let update_interval = update_interval_override.unwrap_or(UPDATE_INTERVAL);
        let status_spans: Vec<Span> = queue_sizes
            .iter()
            .zip(labels.iter())
            .map(|(queue_size, label)| {
                let progress_span = info_span!("");
                progress_span.pb_set_message(label);
                progress_span.pb_set_position(0);
                progress_span.pb_set_style(&ProgressStyle::with_template(PROGRESS_BAR_STYLE)?);
                progress_span.pb_set_length(*queue_size);

                Ok(progress_span)
            })
            .collect::<Result<Vec<Span>>>()?;

        while stop_flag.load(std::sync::atomic::Ordering::Relaxed) == false {
            for (queue, status_span) in queues.iter().zip(status_spans.iter()) {
                let _ = status_span.enter();
                status_span.pb_set_position(queue.len() as u64);
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
