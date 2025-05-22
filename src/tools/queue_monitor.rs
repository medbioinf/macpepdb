// std imports
use std::sync::Arc;
use std::sync::{atomic::AtomicBool, Mutex};

// 3rd party imports
use anyhow::{bail, Result};
use crossbeam_queue::ArrayQueue;
use futures::Future;
use indicatif::ProgressStyle;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{error, info_span, Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

/// update interval for the progress bar in ms
///
pub const UPDATE_INTERVAL: u64 = 300;

/// Progress bar style. Used when a maximum value is given
///
const PROGRESS_BAR_STYLE: &str = "        {msg} {wide_bar} {pos}/{len}";

/// Trait for a monitorable queue
///
pub trait MonitorableQueue: Send + Sync + 'static {
    /// Returns the length of the queue
    ///
    fn len(&self) -> impl Future<Output = usize> + Send;

    /// Returns if the queue is empty
    fn is_empty(&self) -> impl Future<Output = bool> + Send {
        async { self.len().await == 0 }
    }
}

impl<T> MonitorableQueue for Arc<Mutex<Vec<T>>>
where
    T: Send + Sync + 'static,
{
    async fn len(&self) -> usize {
        match self.lock() {
            Ok(queue) => queue.len(),
            Err(_) => {
                error!("Failed to lock queue");
                0
            }
        }
    }
}

impl<T> MonitorableQueue for Arc<ArrayQueue<T>>
where
    T: Send + Sync + 'static,
{
    async fn len(&self) -> usize {
        // Avoid recursive call of len by using self.as_ref()
        self.as_ref().len()
    }
}

/// Show the progress of a queue
///
pub struct QueueMonitor {
    thread_handle: Option<JoinHandle<Result<()>>>, // Wrapped in Option to be able to take it when await join
    stop_flag: Arc<AtomicBool>,
}

impl QueueMonitor {
    /// Shows the size of the given queues in a progress bar
    /// Make sure to call it with `.instrument(<span>)` to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `queues` - Vector of queues to monitor
    /// * `queue_sizes` - Max capacity of the queues
    /// * `labels` - Vector of labels for the queues
    /// * `update_interval_override` - Override for the update interval (default: [UPDATE_INTERVAL])
    ///
    pub fn new<T>(
        title: &str,
        queues: Vec<T>,
        queue_sizes: Vec<u64>,
        labels: Vec<String>,
        update_interval_override: Option<u64>,
    ) -> Result<Self>
    where
        T: MonitorableQueue,
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

    /// Creates a progress bar for the given queues and updates them constantly
    /// Make sure to call it with '.instrument(<span>)' to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `queues` - Vector of queues to monitor
    /// * `queue_sizes` - Max capacity of the queues
    /// * `labels` - Vector of labels for the queues
    /// * `stop_flag` - Flag to stop the progress bar
    /// * `update_interval_override` - Override for the update interval (default: 1000ms)
    ///
    async fn view<T>(
        queues: Vec<T>,
        queue_sizes: Vec<u64>,
        labels: Vec<String>,
        stop_flag: Arc<AtomicBool>,
        update_interval_override: Option<u64>,
    ) -> Result<()>
    where
        T: MonitorableQueue,
    {
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

        while !stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
            for (queue, status_span) in queues.iter().zip(status_spans.iter()) {
                let _ = status_span.enter();
                status_span.pb_set_position(queue.len().await as u64);
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
        if let Some(handle) = self.thread_handle.take() {
            handle.await??
        }
        Ok(())
    }
}
