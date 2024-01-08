// std imports
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
};

// 3rd party imports
use anyhow::{bail, Result};
use tokio::spawn;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio::time::{sleep, Instant};
use tracing::info_span;
use tracing_indicatif::span_ext::IndicatifSpanExt;

// internal imports
use crate::tools::message_logger::MessageLogger;

/// Struct for logging metrics to a file and/or console
pub struct MetricsLogger;

impl MetricsLogger {
    /// Checks if the number of labels and values are equal
    ///
    /// # Arguments
    /// * `metrics_labels` - Vector of metric labels
    /// * `metrics_values` - Vector of metric values
    ///
    fn are_labels_and_values_valid(
        metrics_labels: &Vec<String>,
        metrics_values: &Vec<AtomicUsize>,
    ) -> Result<()> {
        if metrics_labels.len() != metrics_values.len() {
            bail!("Number of metrics and metric values must be equal")
        }

        Ok(())
    }

    /// Takes a snapshot of the current metric values
    ///
    /// # Arguments
    /// * `metrics_values` - Vector of metric values
    ///
    fn snapshot_metric_values(metrics_values: &Vec<AtomicUsize>) -> Vec<usize> {
        metrics_values
            .iter()
            .map(|metric| metric.load(Ordering::Relaxed))
            .collect()
    }

    /// Calculate the delta between two metric snapshots
    ///
    /// # Arguments
    /// * `current_metrics` - Vector of current metric values
    /// * `previous_metrics` - Vector of previous metric values
    ///
    fn calculate_metrics_delta(
        current_metrics: &Vec<usize>,
        previous_metrics: &Vec<usize>,
    ) -> Vec<usize> {
        current_metrics
            .iter()
            .zip(previous_metrics.iter())
            .map(|(current, previous)| current - previous)
            .collect::<Vec<usize>>()
    }

    /// Logs metrics each `log_interval` to file and console each second
    /// Stops when `stop_flag` is set to true
    ///
    /// # Arguments
    /// * `metrics_values` - Vector of metric values
    /// * `metrics_labels` - Vector of metric labels
    /// * `log_file_path` - Path to log file
    /// * `log_interval` - Number of seconds between each log (seconds)
    /// * `stop_flag` - Flag to stop logging
    ///
    pub async fn start_logging_to_both(
        metrics_values: Arc<Vec<AtomicUsize>>,
        metrics_labels: Vec<String>,
        log_file_path: PathBuf,
        log_interval: u64,
        stop_flag: Arc<AtomicBool>,
    ) -> Result<()> {
        Self::are_labels_and_values_valid(&metrics_labels, metrics_values.as_ref())?;

        // Start file logging in separate thread
        let thread_metrics_values = metrics_values.clone();
        let thread_metrics_name = metrics_labels.clone();
        let thread_stop_flag = stop_flag.clone();
        // Start message logger
        let metric_file_logger: JoinHandle<Result<()>> = spawn(async move {
            Self::start_logging_to_file(
                thread_metrics_values,
                thread_metrics_name,
                log_file_path,
                log_interval,
                thread_stop_flag,
            )
            .await?;

            Ok(())
        });

        // Start console logging
        Self::start_logging_to_console(metrics_values, metrics_labels, stop_flag).await?;

        // Stop file logging
        metric_file_logger.await??;

        Ok(())
    }

    /// Logs metrics to console each second
    ///
    /// # Arguments
    /// * `metrics_values` - Vector of metric values
    /// * `metrics_labels` - Vector of metric labels
    /// * `stop_flag` - Flag to stop logging
    pub async fn start_logging_to_console(
        metrics_values: Arc<Vec<AtomicUsize>>,
        metrics_labels: Vec<String>,
        stop_flag: Arc<AtomicBool>,
    ) -> Result<()> {
        Self::are_labels_and_values_valid(&metrics_labels, metrics_values.as_ref())?;

        let mut previous_metrics = Self::snapshot_metric_values(metrics_values.as_ref());

        let spans: Vec<_> = metrics_labels
            .iter()
            .map(|metric_name| info_span!("{}", metric_name))
            .collect();

        while !stop_flag.load(Ordering::Relaxed) {
            sleep(Duration::from_secs(1)).await;
            let current_metrics = Self::snapshot_metric_values(metrics_values.as_ref());

            let delta_metrics = Self::calculate_metrics_delta(&current_metrics, &previous_metrics);

            for (metric_idx, span) in spans.iter().enumerate() {
                // Enter span
                let _e = span.enter();
                // Update span
                span.pb_set_message(&format!(
                    "{} {} total, {} processed/s",
                    metrics_labels[metric_idx],
                    current_metrics[metric_idx],
                    delta_metrics[metric_idx],
                ));
            }

            previous_metrics = current_metrics;
        }

        Ok(())
    }

    async fn start_logging_to_file(
        metric_values: Arc<Vec<AtomicUsize>>,
        metric_labels: Vec<String>,
        log_file_path: PathBuf,
        log_interval: u64,
        stop_flag: Arc<AtomicBool>,
    ) -> Result<()> {
        // Message logger variable
        let (message_sender, message_receiver) = channel::<String>(10);
        // Start message logger
        let message_logger: JoinHandle<Result<()>> = spawn(async move {
            MessageLogger::start_logging(log_file_path, message_receiver, 1).await?;
            Ok(())
        });

        // Create TSV header: `time\tmetric_1\tmetric_1_rate\tmetric_2\tmetric_2_rate\t...`
        let mut tsv_header = vec!["time".to_owned()];
        tsv_header.extend(
            metric_labels
                .into_iter()
                .flat_map(|metric| vec![metric.clone(), format!("{}_rate", metric)]),
        );

        // Send header to file
        message_sender.send(tsv_header.join("\t")).await?;

        let mut log_counter: u64 = 0;
        let mut next_log_time = Instant::now() + Duration::from_secs(log_interval);
        let mut previous_metrics = Self::snapshot_metric_values(metric_values.as_ref());

        // Wait for stop flag
        while !stop_flag.load(Ordering::Relaxed) {
            // Check each second if log time is reached other wise continue.
            // Without regular short checks the stop flag would not be checked for a long time.
            sleep(Duration::from_secs(1)).await;
            if Instant::now() < next_log_time {
                continue;
            }
            next_log_time = Instant::now() + Duration::from_secs(log_interval);
            log_counter += 1;

            let current_metrics = Self::snapshot_metric_values(metric_values.as_ref());
            let delta_metrics = Self::calculate_metrics_delta(&current_metrics, &previous_metrics);
            Self::send_metrics_to_file(
                &current_metrics,
                &delta_metrics,
                (log_counter * log_interval) as usize,
                &message_sender,
            )
            .await?;
            previous_metrics = current_metrics;
        }
        let current_metrics = Self::snapshot_metric_values(metric_values.as_ref());
        let delta_metrics = Self::calculate_metrics_delta(&current_metrics, &previous_metrics);
        Self::send_metrics_to_file(
            &current_metrics,
            &delta_metrics,
            (log_counter * log_interval) as usize,
            &message_sender,
        )
        .await?;

        drop(message_sender);

        message_logger.await??;

        Ok(())
    }

    /// Creates TSV string from metric values and send it to message logger
    ///
    /// # Arguments
    /// * `current_metrics` - Vector of current metric values
    /// * `delta_metrics` - Vector of metric deltas
    /// * `timepoint` - Timepoint of metric values
    /// * `message_sender` - Sender for sending messages to message logger
    ///
    async fn send_metrics_to_file(
        current_metrics: &Vec<usize>,
        delta_metrics: &Vec<usize>,
        timepoint: usize,
        message_sender: &Sender<String>,
    ) -> Result<()> {
        let mut metrics_log: Vec<String> = vec![timepoint.to_string()];
        metrics_log.extend(
            current_metrics
                .iter()
                .zip(delta_metrics.iter())
                .flat_map(|(current, delta)| vec![current.to_string(), delta.to_string()]),
        );

        message_sender.send(metrics_log.join("\t")).await?;

        Ok(())
    }
}
