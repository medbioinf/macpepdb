mod prometheus_handler;

use std::{collections::HashMap, fmt::Display, net::SocketAddr, path::PathBuf, pin::Pin, process};

use clap::ValueEnum;
use macpepdb_metrics_peek::MetricsPeek;
use macpepdb_tui::{TuiLayer, TuiRecorder};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusRecorder};
use metrics_util::layers::{Fanout, FanoutBuilder};
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing::Level;
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{
    EnvFilter, filter::Directive, layer::SubscriberExt, util::SubscriberInitExt,
};
use url::Url;

use crate::monitoring::prometheus_handler::PrometheusHandler;

static DEFAULT_FILTER: &[(&str, Level)] = &[
    ("axum", Level::ERROR),
    ("h2", Level::ERROR),
    ("hyper", Level::ERROR),
    ("mio", Level::ERROR),
    ("reqwest", Level::ERROR),
    ("tokio_postgres", Level::ERROR),
];

/// Errors returned while setting up tracing/metrics monitoring.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Unable to parse filter directive: {0}")]
    InvalidFilterDirective(#[from] tracing_subscriber::filter::ParseError),
    #[error("Log file has no parent directory")]
    LogFileHasNoParent,
    #[error("Log file has no file name")]
    LogFileHasNoName,
    #[error("Log file is a path")]
    LogFileIsDirectory,
    #[error("Unable to build Loki tracing layer: {0}")]
    Loki(#[from] tracing_loki::Error),
    #[error("Unable to build Prometheus scrape endpoint: {0}")]
    PrometheusBuild(#[from] metrics_exporter_prometheus::BuildError),
    #[error("Promtheus error in monitoring: {0}")]
    Prometheus(#[from] crate::monitoring::prometheus_handler::Error),
    #[error("Unable to set global metrics recorder: {0}")]
    SetMetricsRecorder(#[from] metrics::SetRecorderError<Fanout>),
    #[error("Terminal and TUI are exclusive, choose one")]
    TerminalAndTuiExclusive,
}

/// Target for tracing
///
pub enum TracingTarget {
    Loki(Url, String),
    File(PathBuf, Rotation),
    Terminal,
    Tui(TuiLayer),
    #[cfg(feature = "tokio-console")]
    Console(Option<SocketAddr>),
}

/// Target for metrics
///
pub enum MetricTarget {
    Prometheus(
        SocketAddr,
        Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ),
    Tui(TuiRecorder),
    Tracing(u64),
}

/// Log rotation values for CLI
///
#[derive(Clone, ValueEnum)]
pub enum TracingLogRotation {
    Minutely,
    Hourly,
    Daily,
    Never,
}

impl From<TracingLogRotation> for Rotation {
    fn from(rotation: TracingLogRotation) -> Self {
        match rotation {
            TracingLogRotation::Minutely => Rotation::MINUTELY,
            TracingLogRotation::Hourly => Rotation::HOURLY,
            TracingLogRotation::Daily => Rotation::DAILY,
            TracingLogRotation::Never => Rotation::NEVER,
        }
    }
}

impl Display for TracingLogRotation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TracingLogRotation::Minutely => "minutely",
            TracingLogRotation::Hourly => "hourly",
            TracingLogRotation::Daily => "daily",
            TracingLogRotation::Never => "never",
        };
        write!(f, "{s}")
    }
}

/// Owns the handles/guards of all active tracing and metrics targets (log file
/// writer, Loki task, Prometheus scrape endpoint), keeping them alive for the
/// program's lifetime.
#[derive(Default)]
pub struct Monitoring {
    loki_handler: Option<JoinHandle<()>>,
    log_writer_guard: Option<WorkerGuard>,
    promethus_handler: Option<PrometheusHandler>,
}

impl Monitoring {
    /// Wires up the given tracing and metrics targets as the global `tracing`
    /// subscriber and `metrics` recorder, then returns a `Monitoring` holding
    /// whatever handles/guards need to stay alive.
    ///
    /// # Arguments
    /// * `verbosity` - Repeat-count of the CLI `-v` flag; maps to a base log level via
    ///   [`Monitoring::verbosity_to_log_level`] (or forces trace-everything past 10, see
    ///   [`Monitoring::is_all_trace`]).
    /// * `tracing_targets` - The tracing outputs to enable (terminal, TUI, file, Loki,
    ///   tokio-console); `Terminal` and `Tui` are mutually exclusive.
    /// * `metric_targets` - The metrics outputs to enable (Prometheus, TUI, periodic
    ///   tracing log), fanned out to a single global recorder.
    /// * `tracing_filters` - Per-target level overrides merged over `DEFAULT_FILTER`.
    pub async fn new(
        verbosity: u8,
        tracing_targets: impl Iterator<Item = TracingTarget>,
        metric_targets: impl Iterator<Item = MetricTarget>,
        tracing_filters: HashMap<&str, Level>,
    ) -> Result<Self, Error> {
        let mut monitoring = Self::default();

        let log_level = Self::verbosity_to_log_level(verbosity);

        let mut filters = DEFAULT_FILTER.iter().cloned().collect::<HashMap<_, _>>();
        filters.extend(tracing_filters);

        if Self::is_all_trace(verbosity) {
            filters
                .iter_mut()
                .for_each(|(_, level)| *level = Level::TRACE);
        }

        // Tracing layers
        let mut terminal_layer = None;
        let mut tui_layer = None;
        let mut loki_layer = None;
        let mut file_layer = None;
        #[cfg(feature = "tokio-console")]
        let mut console_layer = None;

        for tracing_target in tracing_targets {
            match tracing_target {
                TracingTarget::File(path, rotation) => {
                    if path.is_dir() {
                        return Err(Error::LogFileIsDirectory);
                    }
                    let file_appender = RollingFileAppender::new(
                        rotation,
                        path.parent().ok_or(Error::LogFileHasNoParent)?,
                        path.file_name().ok_or(Error::LogFileHasNoName)?,
                    );
                    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
                    file_layer = Some(tracing_subscriber::fmt::layer().with_writer(non_blocking));
                    monitoring.log_writer_guard = Some(guard);
                }
                TracingTarget::Loki(url, label) => {
                    let (layer, task) = tracing_loki::builder()
                        .label(env!("CARGO_CRATE_NAME"), label)?
                        .extra_field("pid", format!("{}", process::id()))?
                        .build_url(url)?;
                    monitoring.loki_handler = Some(tokio::spawn(task));
                    loki_layer = Some(layer);
                }
                TracingTarget::Terminal => {
                    if tui_layer.is_some() {
                        return Err(Error::TerminalAndTuiExclusive);
                    }
                    terminal_layer = Some(
                        tracing_subscriber::fmt::Layer::default().with_writer(std::io::stderr),
                    );
                }
                TracingTarget::Tui(layer) => {
                    if terminal_layer.is_some() {
                        return Err(Error::TerminalAndTuiExclusive);
                    }
                    tui_layer = Some(layer);
                }
                #[cfg(feature = "tokio-console")]
                TracingTarget::Console(socket) => {
                    filters.insert("tokio", Level::TRACE);
                    filters.insert("runtime", Level::TRACE);
                    let mut builder = console_subscriber::ConsoleLayer::builder()
                        .with_default_env()
                        .enable_grpc_web(true);
                    if let Some(socket) = socket {
                        builder = builder.server_addr(socket);
                    }
                    console_layer = Some(builder.spawn());
                }
            }
        }

        let mut filter = filters.iter().try_fold(
            EnvFilter::from_default_env(),
            |filter: EnvFilter, (target, level): (&&str, &Level)| {
                let directive: Directive = format!("{target}={}", level.to_string().to_lowercase())
                    .parse()
                    .map_err(Error::InvalidFilterDirective)?;
                Ok::<EnvFilter, Error>(filter.add_directive(directive))
            },
        )?;

        filter = filter.add_directive(
            format!("{log_level}")
                .parse()
                .map_err(Error::InvalidFilterDirective)?,
        );

        let registry = tracing_subscriber::registry()
            .with(terminal_layer)
            .with(tui_layer)
            .with(file_layer)
            .with(loki_layer);

        #[cfg(feature = "tokio-console")]
        let registry = registry.with(console_layer);

        registry.with(filter).init();

        let mut tui_recorder: Option<TuiRecorder> = None;
        let mut prometheus_recorder: Option<PrometheusRecorder> = None;
        let mut peek_recorder: Option<MetricsPeek<_>> = None;

        for metric_target in metric_targets {
            match metric_target {
                MetricTarget::Prometheus(addr, shutdown_signal) => {
                    let recorder = PrometheusBuilder::new().build_recorder();
                    monitoring.promethus_handler = Some(
                        PrometheusHandler::new(recorder.handle(), addr, shutdown_signal).await?,
                    );
                    prometheus_recorder = Some(recorder);
                }
                MetricTarget::Tui(recorder) => {
                    tui_recorder = Some(recorder);
                }
                MetricTarget::Tracing(milliseconds) => {
                    let log_mode = if milliseconds == 0 {
                        macpepdb_metrics_peek::LogMode::Immediate
                    } else {
                        macpepdb_metrics_peek::LogMode::Periodic(milliseconds)
                    };

                    peek_recorder = Some(MetricsPeek::new(
                        log_mode,
                        |msg| tracing::info!("{msg}"),
                        |err| tracing::error!("Error in metrics recorder: {err}"),
                    ));
                }
            }
        }

        let mut fanout_builder = FanoutBuilder::default();
        if let Some(recorder) = tui_recorder {
            fanout_builder = fanout_builder.add_recorder(recorder);
        }
        if let Some(recorder) = prometheus_recorder {
            fanout_builder = fanout_builder.add_recorder(recorder);
        }
        if let Some(recorder) = peek_recorder {
            fanout_builder = fanout_builder.add_recorder(recorder);
        }

        metrics::set_global_recorder(fanout_builder.build())?;

        Ok(monitoring)
    }

    /// Maps a `-v` repeat-count to a base `tracing` log level (0 = error, 4+ = trace).
    pub fn verbosity_to_log_level(verbosity: u8) -> Level {
        match verbosity {
            0 => Level::ERROR,
            1 => Level::WARN,
            2 => Level::INFO,
            3 => Level::DEBUG,
            _ => Level::TRACE,
        }
    }

    /// Whether the verbosity level is high enough to force every filter to `TRACE`.
    pub fn is_all_trace(verbosity: u8) -> bool {
        verbosity >= 10
    }
}
