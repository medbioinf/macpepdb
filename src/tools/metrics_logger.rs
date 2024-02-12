// std imports
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
};

// 3rd party imports
use anyhow::{bail, Result};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::time::Duration;
use tokio::time::{sleep, Instant};

/// Struct for logging metrics to a file and/or console
pub struct MetricsLogger {
    thread_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    stop_flag: Arc<AtomicBool>,
}

impl MetricsLogger {
    /// Creates a new instance of MetricsLogger
    ///
    /// # Arguments
    /// * `metric_values` - Vector of metric values
    /// * `metric_labels` - Vector of metric labels
    /// * `log_file_path` - Path to log file
    /// * `log_interval` - Interval in seconds for logging
    ///
    pub fn new(
        metric_values: Vec<Arc<AtomicUsize>>,
        metrics_labels: Vec<String>,
        log_file_path: PathBuf,
        log_interval: u64,
    ) -> Result<Self> {
        if metrics_labels.len() != metric_values.len() {
            bail!("Number of metrics and metric labels must be equal")
        }

        let stop_flag = Arc::new(AtomicBool::new(false));
        let thread_handle = tokio::spawn(Self::log(
            metric_values,
            metrics_labels,
            log_file_path,
            log_interval,
            stop_flag.clone(),
        ));

        Ok(Self {
            thread_handle: Some(thread_handle),
            stop_flag,
        })
    }

    pub async fn log(
        metric_values: Vec<Arc<AtomicUsize>>,
        metric_labels: Vec<String>,
        log_file_path: PathBuf,
        log_interval: u64,
        stop_flag: Arc<AtomicBool>,
    ) -> Result<()> {
        let mut log_counter: u64 = 0;
        let mut next_log_time = Instant::now() + Duration::from_secs(log_interval);
        let mut previous_metrics = metric_values
            .iter()
            .map(|metric| metric.load(Ordering::Relaxed))
            .collect::<Vec<usize>>();
        let mut file_writer = BufWriter::new(tokio::fs::File::create(log_file_path).await?);
        // Create TSV header: `time\tmetric_1\tmetric_1_rate\tmetric_2\tmetric_2_rate\t...`
        let mut tsv_header = vec!["time".to_owned()];
        tsv_header.extend(
            metric_labels
                .into_iter()
                .flat_map(|metric| vec![metric.clone(), format!("{}_rate", metric)]),
        );

        file_writer.write(tsv_header.join("\t").as_bytes()).await?;

        // Wait for stop flag
        while !stop_flag.load(Ordering::Relaxed) {
            // Check each second if log time is reached other wise continue.
            // Without regular short checks the stop flag would not be checked for a long time.
            sleep(Duration::from_secs(1)).await;
            if Instant::now() < next_log_time {
                continue;
            }
            // Set next log time
            next_log_time = Instant::now() + Duration::from_secs(log_interval);

            // Increment log counter
            log_counter += 1;

            // Collect current metrics and calculate delta
            let current_metrics = metric_values
                .iter()
                .map(|metric| metric.load(Ordering::Relaxed))
                .collect::<Vec<usize>>();
            // Calculate delta
            let delta = current_metrics
                .iter()
                .zip(previous_metrics.iter())
                .map(|(current, previous)| current - previous)
                .collect::<Vec<usize>>();

            // Current timepoint
            let timepoint = (log_counter * log_interval) as usize;

            // Start new line ...
            let mut tsv_line: Vec<u8> = Vec::from(b"\n");
            // ... and write timepoint
            tsv_line.extend(timepoint.to_string().as_bytes());
            // ... write metrics and delta
            current_metrics
                .iter()
                .zip(delta.iter())
                .for_each(|(current, delta)| {
                    tsv_line.extend(b"\t");
                    tsv_line.extend(current.to_string().as_bytes());
                    tsv_line.extend(b"\t");
                    tsv_line.extend(delta.to_string().as_bytes());
                });
            file_writer.write(&tsv_line).await?;
            file_writer.flush().await?;
            // Update previous metrics
            previous_metrics = current_metrics;
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
