//! # macpepdb_tui
//!
//! A [`ratatui`]-based terminal UI that provides:
//!
//! - A scrollable **log panel** fed by a [`tracing_subscriber::Layer`]
//! - A **metrics panel** driven by the [`metrics`] crate
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use std::time::Duration;
//! use macpepdb_tui::{MetricConfig, Tui};
//!
//! #[tokio::main]
//! async fn main() {
//!     let handle = Tui::builder()
//!         .title("My App")
//!         .with_metric(MetricConfig::counter("records_processed", "Records processed"))
//!         .with_metric(MetricConfig::progress("overall_progress", "Overall progress", 10_000.0))
//!         .with_metric(MetricConfig::rate("records_processed", "Throughput"))
//!         .build()
//!         .run()
//!         .expect("failed to start TUI");
//!
//!     // Use the `tracing` and `metrics` macros as usual — the TUI picks them up.
//!     tracing::info!("Starting work…");
//!     metrics::counter!("records_processed").increment(1);
//!
//!     handle.stop().await;
//! }
//! ```
//!
//! ## Manual subscriber composition
//!
//! If you already have a tracing subscriber, add [`TuiLayer`] yourself:
//!
//! ```rust,ignore
//! use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
//! use macpepdb_tui::{Tui, TuiLayer};
//!
//! let tui = Tui::builder().build();
//!
//! tracing_subscriber::registry()
//!     .with(tracing_subscriber::EnvFilter::from_default_env())
//!     .with(tui.layer())   // <── plug in the TUI layer
//!     .init();
//!
//! // Install the metrics recorder separately.
//! metrics::set_global_recorder(tui.recorder()).unwrap();
//!
//! // Then start the render loop.
//! let handle = tui.run_raw();
//! ```

mod app;
mod config;
mod layer;
mod recorder;
mod state;

pub use config::{MetricConfig, MetricKind, TuiConfig};
pub use layer::TuiLayer;
pub use recorder::TuiRecorder;

use std::sync::Arc;

use parking_lot::RwLock;
use state::{SharedState, TuiState};
use tokio::{sync::oneshot, task::JoinHandle};

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum TuiError {
    #[error("a metrics recorder is already installed")]
    RecorderAlreadyInstalled,
}

// ── TuiHandle ─────────────────────────────────────────────────────────────────

/// A handle to a running TUI task.
///
/// Dropping the handle sends a shutdown signal automatically, but does **not**
/// await the task — call [`TuiHandle::stop`] if you need a clean shutdown.
///
/// The handle also gives you live access to the shared state so you can add,
/// remove, or replace metric rows at any time:
///
/// ```rust,ignore
/// handle.add_metric(MetricConfig::counter("new_metric", "New metric"));
/// handle.remove_metric("old_metric");
/// handle.set_metrics(vec![
///     MetricConfig::counter("a", "A"),
///     MetricConfig::rate("a", "A/s"),
/// ]);
/// ```
pub struct TuiHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: JoinHandle<()>,
    state: SharedState,
}

impl TuiHandle {
    /// Gracefully shut down the TUI and wait for the render task to finish.
    pub async fn stop(mut self) {
        self.send_shutdown();
        // Ignore join errors (task may have already finished).
        let _ = (&mut self.join_handle).await;
    }

    /// Send the shutdown signal without awaiting (fire-and-forget).
    pub fn stop_nowait(&mut self) {
        self.send_shutdown();
    }

    /// Wait for the TUI render task to finish without sending a shutdown signal.
    ///
    /// Use this when you want the TUI to remain alive after the main work
    /// completes, allowing the user to review logs before exiting with q or
    /// Ctrl+C.
    pub async fn wait(&mut self) {
        let _ = (&mut self.join_handle).await;
    }

    fn send_shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    // ── Live metrics panel management ────────────────────────────────────────

    /// Append a new metric display row to the running TUI.
    ///
    /// The same key can appear multiple times with different
    /// [`MetricKind`]s (e.g. a counter *and* a rate row).
    /// If no live value exists yet for the key a zero-valued entry is
    /// created automatically.
    pub fn add_metric(&self, metric: MetricConfig) {
        self.state.write().add_metric(metric);
    }

    /// Remove **all** display rows whose key equals `key`.
    ///
    /// The underlying value and history are kept so that re-adding the
    /// metric later resumes from where it left off.  Pass the key to
    /// [`TuiHandle::clear_metric_data`] as well if you need a clean slate.
    pub fn remove_metric(&self, key: &str) {
        self.state.write().remove_metric(key);
    }

    /// Replace the **entire** display list atomically.
    ///
    /// Prefer this over multiple individual [`TuiHandle::add_metric`] /
    /// [`TuiHandle::remove_metric`] calls when you want to swap out many
    /// rows at once, since it only acquires the lock once.
    pub fn set_metrics(&self, metrics: impl IntoIterator<Item = MetricConfig>) {
        self.state.write().set_metrics(metrics);
    }

    /// Erase the stored value and history for `key` without touching the
    /// display list.
    ///
    /// Combine with [`TuiHandle::remove_metric`] when you want both the row
    /// and the data gone.
    pub fn clear_metric_data(&self, key: &str) {
        self.state.write().clear_metric_data(key);
    }
}

impl Drop for TuiHandle {
    /// Ensures the render task receives a shutdown signal when the handle is
    /// dropped, even if [`TuiHandle::stop`] was not called.
    fn drop(&mut self) {
        self.send_shutdown();
    }
}

// ── Tui ───────────────────────────────────────────────────────────────────────

/// The main TUI instance.  Build it with [`Tui::builder`], then start it with
/// [`Tui::run`] (or [`Tui::run_raw`] for manual subscriber wiring).
pub struct Tui {
    config: TuiConfig,
    state: SharedState,
}

impl Tui {
    /// Create a [`TuiBuilder`] with default settings.
    pub fn builder() -> TuiBuilder {
        TuiBuilder {
            config: TuiConfig::default(),
        }
    }

    // ── Component accessors ───────────────────────────────────────────────────

    /// Return a [`TuiLayer`] that can be composed into an existing
    /// [`tracing_subscriber`] registry.
    ///
    /// This gives you full control over the subscriber stack.  If you just
    /// want everything set up automatically use [`Tui::run`] instead.
    pub fn layer(&self) -> TuiLayer {
        TuiLayer::new(self.state.clone())
    }

    /// Return a [`TuiRecorder`] that can be installed as the global
    /// [`metrics`] recorder.
    ///
    /// Use this when you need to install the recorder manually (e.g. when
    /// composing with other recorders).
    pub fn recorder(&self) -> TuiRecorder {
        TuiRecorder::new(self.state.clone())
    }

    // ── Start helpers ─────────────────────────────────────────────────────────

    /// Install a tracing subscriber and the metrics recorder globally, then
    /// spawn the TUI render task.
    ///
    /// - The tracing subscriber respects the `RUST_LOG` environment variable.
    /// - If a tracing subscriber is already installed the error is silently
    ///   ignored (the layer is simply not added).
    /// - If a metrics recorder is already installed [`TuiError::RecorderAlreadyInstalled`]
    ///   is returned.
    ///
    /// **Requires** an active Tokio runtime — call this inside
    /// `#[tokio::main]` or equivalent.
    pub fn run(self) -> Result<TuiHandle, TuiError> {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

        // Install tracing layer (ignore error if a subscriber already exists).
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(TuiLayer::new(self.state.clone()))
            .try_init()
            .ok();

        // Install metrics recorder.
        metrics::set_global_recorder(TuiRecorder::new(self.state.clone()))
            .map_err(|_| TuiError::RecorderAlreadyInstalled)?;

        Ok(self.spawn_task())
    }

    /// Spawn the TUI render task **without** touching the global tracing
    /// subscriber or the metrics recorder.
    ///
    /// Wire those up yourself via [`Tui::layer`] and [`Tui::recorder`] before
    /// calling this method.
    ///
    /// **Requires** an active Tokio runtime.
    pub fn run_raw(self) -> TuiHandle {
        self.spawn_task()
    }

    fn spawn_task(self) -> TuiHandle {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let config = self.config;
        let state = self.state;

        let handle_state = state.clone();

        let join_handle = tokio::spawn(async move {
            app::run_tui(config, state, shutdown_rx).await;
        });

        TuiHandle {
            shutdown_tx: Some(shutdown_tx),
            join_handle,
            state: handle_state,
        }
    }
}

// ── TuiBuilder ────────────────────────────────────────────────────────────────

/// Fluent builder for [`Tui`].
pub struct TuiBuilder {
    config: TuiConfig,
}

impl TuiBuilder {
    /// Set the title shown in the outer border (default: `"TUI"`).
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.config.title = title.into();
        self
    }

    /// Set the maximum number of log lines kept in memory (default: 1000).
    pub fn max_log_messages(mut self, max: usize) -> Self {
        self.config.max_log_messages = max;
        self
    }

    /// Set how often the screen is redrawn (default: 100 ms).
    pub fn tick_rate(mut self, rate: std::time::Duration) -> Self {
        self.config.tick_rate = rate;
        self
    }

    /// Append a metric row to the Metrics panel.
    ///
    /// Rows are displayed in the order they are added.  The same key can be
    /// added multiple times with different [`MetricKind`] to show both a
    /// counter and a rate for the same underlying metric.
    pub fn with_metric(mut self, metric: MetricConfig) -> Self {
        self.config.metrics.push(metric);
        self
    }

    /// Replace the entire metrics list.
    pub fn with_metrics(mut self, metrics: impl IntoIterator<Item = MetricConfig>) -> Self {
        self.config.metrics = metrics.into_iter().collect();
        self
    }

    /// Consume the builder and produce a [`Tui`] instance.
    pub fn build(self) -> Tui {
        let state = Arc::new(RwLock::new(TuiState::new(
            self.config.max_log_messages,
            &self.config.metrics,
        )));
        Tui {
            config: self.config,
            state,
        }
    }
}
