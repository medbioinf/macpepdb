use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicBool, Arc};

use anyhow::Result;
use indicatif::ProgressStyle;
use prometheus_parse::{Scrape, Value};
use reqwest;
use tracing::{info_span, warn, Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

/// Refresh interval for the metrics monitor in ms
///
const REFRESH_INTERVAL: u64 = 1000;

/// Progress style, used when no maximum value is given
///
const SIMPLE_STYLE: &'static str = "        {msg} {pos} {per_sec} ";

/// Progress bar style
///
const PROGRESS_BAR_STYLE: &'static str = "        {msg} {wide_bar} {pos}/{len} {per_sec} ";

/// Progress bar style
///
const QUEUE_STYLE: &'static str = "        {msg} {wide_bar} {pos}/{len}  ";

/// Struct for defining a monitorable metric
pub struct MonitorableMetric {
    /// Prometheus metric name
    name: String,
    /// Type of metric
    metric_type: MonitorableMetricType,
}

impl MonitorableMetric {
    /// Creates a new instance of MonitorableMetric
    ///
    /// # Arguments
    /// * `name` - Prometheus metric name
    /// * `metric_type` - Type of metric
    ///
    pub fn new(name: String, metric_type: MonitorableMetricType) -> Self {
        Self { name, metric_type }
    }
}

/// Enum for defining the type of a monitorable metric
/// Rate: Rendered as a entities per second
/// Progress: Rendered as progress bar with value as maximum + rate
///
pub enum MonitorableMetricType {
    /// Rendered as a entities per second
    Rate,
    /// Rendered as progress bar with value as maximum + rate
    Progress(u64),
    /// Renders the queue fill level as progress bar
    Queue(u64),
}

/// Struct for showing metrics of the local Prometheus scrape endpoint
/// on the console.
///
pub struct MetricsMonitor {
    thread_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    stop_flag: Arc<AtomicBool>,
}

impl MetricsMonitor {
    /// Creates a new instance of MetricsMonitor and starts an inner, async task
    /// for visualizing the metrics on the console.
    ///
    /// # Arguments
    ///
    pub fn new(title: &str, metrics: Vec<MonitorableMetric>, scrape_url: String) -> Result<Self> {
        let monitor_span = info_span!("");
        monitor_span.pb_set_message(title);
        let stop_flag = Arc::new(AtomicBool::new(false));
        let thread_handle = tokio::spawn(
            Self::view(metrics, scrape_url, stop_flag.clone()).instrument(monitor_span),
        );

        Ok(Self {
            thread_handle: Some(thread_handle),
            stop_flag,
        })
    }

    /// Async task which visualizes each metrics in an indicatife span
    ///
    /// # Arguments
    /// * `monitorable_metrics` - List of monitorable metrics
    /// * `scrape_url` - URL of the Prometheus scrape endpoint
    /// * `stop_flag` - Atomic boolean flag to stop the logger
    ///
    pub async fn view(
        monitorable_metrics: Vec<MonitorableMetric>,
        scrape_url: String,
        stop_flag: Arc<AtomicBool>,
    ) -> Result<()> {
        let _ = Span::current().enter();
        let mut spans: HashMap<String, Span> = monitorable_metrics
            .iter()
            .map(|metric| {
                let span = info_span!("");
                let _ = span.enter();
                span.pb_set_position(0);
                span.pb_set_message(&metric.name.to_owned());
                match metric.metric_type {
                    MonitorableMetricType::Rate => {
                        span.pb_set_style(&ProgressStyle::with_template(SIMPLE_STYLE).unwrap())
                    }
                    MonitorableMetricType::Progress(max) => {
                        span.pb_set_style(
                            &ProgressStyle::with_template(PROGRESS_BAR_STYLE).unwrap(),
                        );
                        span.pb_set_length(max);
                    }
                    MonitorableMetricType::Queue(max) => {
                        span.pb_set_style(&ProgressStyle::with_template(QUEUE_STYLE).unwrap());
                        span.pb_set_length(max);
                    }
                }
                (metric.name.to_owned(), span)
            })
            .collect();
        while !stop_flag.load(Ordering::Relaxed) {
            let next_refresh =
                tokio::time::Instant::now() + tokio::time::Duration::from_millis(REFRESH_INTERVAL);
            let body = reqwest::get(&scrape_url).await?.text().await?;
            let lines: Vec<_> = body.lines().map(|s| Ok(s.to_owned())).collect();
            let metrics = Scrape::parse(lines.into_iter())?;
            for sample in metrics.samples.iter() {
                match spans.entry(sample.metric.clone()) {
                    Entry::Occupied(mut entry) => {
                        let span = entry.get_mut();
                        let _ = span.enter();
                        match sample.value {
                            Value::Counter(value) | Value::Untyped(value) | Value::Gauge(value) => {
                                span.pb_set_position(value as u64);
                            }
                            _ => {
                                warn!("Unsupported sample value {:?}", sample.value);
                            }
                        }
                    }
                    Entry::Vacant(_) => {}
                }
            }
            tokio::time::sleep_until(next_refresh).await;
        }

        Ok(())
    }

    /// Stops the logger
    ///
    pub async fn stop(&mut self) -> Result<()> {
        self.stop_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        match self.thread_handle.take() {
            Some(handle) => handle.await?,
            None => Ok(()),
        }
    }
}
