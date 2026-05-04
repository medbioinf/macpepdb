//! # metrics-recorder
//!
//! A [`metrics::Recorder`] implementation that forwards every metric update to
//! a user-supplied callback, either **immediately** on each change or **periodically**
//! on a configurable millisecond interval.
//!
//! ## Why callbacks instead of a hard dependency on `log` / `tracing`?
//!
//! Tying the crate to a specific logging facade version would cause dependency
//! conflicts.  Instead you pass any `Fn(&str)` — a closure, a free function,
//! `|s| tracing::info!("{s}")`, `|s| println!("{s}")`, etc.
//!
//! ## Quick-start
//!
//! ```rust,no_run
//! use metrics_recorder::{MetricsPeek, LogMode};
//!
//! let recorder = MetricsPeek::new(
//!     LogMode::Periodic(500),                  // flush every 500 ms
//!     |s| println!("{s}"),                     // log callback
//!     |e| eprintln!("metrics error: {e}"),     // error callback
//! );
//!
//! metrics::set_global_recorder(recorder).unwrap();
//!
//! // Now use the metrics crate as normal.
//! metrics::counter!("requests_total").increment(1);
//! metrics::gauge!("queue_depth").set(42.0);
//! metrics::histogram!("response_time_ms").record(3.7);
//! ```
//!
//! With `tracing`:
//!
//! ```rust,ignore
//! use metrics_recorder::{MetricsPeek, LogMode};
//!
//! let recorder = MetricsPeek::new(
//!     LogMode::Immediate,
//!     |s| tracing::info!("{}", s),
//!     |e| tracing::error!("{}", e),
//! );
//! metrics::set_global_recorder(recorder).unwrap();
//! ```

pub use metrics;

/// Commands sent from metric handles to the background processing thread.
///
/// Each variant carries the fully-formatted metric key (name plus any labels)
/// and the update value, so the state machine never has to touch the
/// `metrics::Key` type after registration.
mod cmd;
mod handles;
mod state;

use cmd::MetricsCmd;
use handles::{CounterHandle, GaugeHandle, HistogramHandle};
use state::MetricsState;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Controls when the log callback is invoked.
pub enum LogMode {
    /// Invoke the callback immediately after every metric update.
    Immediate,
    /// Aggregate updates and invoke the callback once every `n` milliseconds.
    Periodic(u64),
}

/// A [`metrics::Recorder`] that reports metrics by calling a user-supplied
/// closure.
///
/// Construct with [`MetricsPeek::new`], then register globally with
/// [`metrics::set_global_recorder`].
///
/// The recorder spawns exactly one background thread that owns all mutable
/// metric state; handles communicate with it via an MPSC channel, keeping
/// the `Recorder` implementation lock-free on the hot path.
pub struct MetricsPeek<ErrFn> {
    tx: Sender<MetricsCmd>,
    err_cb: ErrFn,
}

impl<ErrFn> MetricsPeek<ErrFn>
where
    ErrFn: Fn(&str) + Clone + Send + Sync + 'static,
{
    /// Create a new recorder.
    ///
    /// - `mode`   — [`LogMode::Immediate`] or [`LogMode::Periodic`] (milliseconds).
    /// - `log_cb` — called with each formatted metric snapshot; receives a `&str`
    ///   containing one or more `"Counter/Gauge/Histogram …"` lines.
    /// - `err_cb` — called when an internal channel send fails (very unlikely in
    ///   practice; only happens if the background thread panicked).
    pub fn new<LogFn>(mode: LogMode, log_cb: LogFn, err_cb: ErrFn) -> Self
    where
        LogFn: Fn(&str) + Clone + Send + Sync + 'static,
    {
        let (tx, rx) = mpsc::channel();
        match mode {
            LogMode::Immediate => Self::run_immediate(rx, log_cb),
            LogMode::Periodic(ms) => Self::run_periodic(rx, log_cb, ms),
        }
        Self { tx, err_cb }
    }

    // -----------------------------------------------------------------------
    // Background threads
    // -----------------------------------------------------------------------

    fn run_immediate<LogFn>(rx: Receiver<MetricsCmd>, log_cb: LogFn)
    where
        LogFn: Fn(&str) + Send + 'static,
    {
        std::thread::Builder::new()
            .name("metrics-recorder-immediate".into())
            .spawn(move || {
                let mut state = MetricsState::new();
                for cmd in rx.iter() {
                    state.update(cmd);
                    if let Some(logs) = state.take_logs() {
                        log_cb(&logs);
                    }
                }
            })
            .expect("metrics-recorder: failed to spawn background thread");
    }

    fn run_periodic<LogFn>(rx: Receiver<MetricsCmd>, log_cb: LogFn, interval_ms: u64)
    where
        LogFn: Fn(&str) + Send + 'static,
    {
        std::thread::Builder::new()
            .name("metrics-recorder-periodic".into())
            .spawn(move || {
                let mut state = MetricsState::new();
                let interval = Duration::from_millis(interval_ms);
                let mut next_flush = Instant::now() + interval;

                loop {
                    // Wait for the next command or until the interval expires.
                    match rx.recv_timeout(interval) {
                        Ok(cmd) => state.update(cmd),
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }

                    let now = Instant::now();
                    if now >= next_flush {
                        if let Some(logs) = state.take_logs() {
                            log_cb(&logs);
                        }
                        next_flush = now + interval;
                    }
                }
            })
            .expect("metrics-recorder: failed to spawn background thread");
    }

    // -----------------------------------------------------------------------
    // Key formatting helper
    // -----------------------------------------------------------------------

    /// Format a `metrics::Key` as `"name"` or `"name{label=value,…}"`.
    fn format_key(key: &Key) -> String {
        let mut labels = key.labels();
        // peek at the first label to decide whether to add braces
        match labels.next() {
            None => key.name().to_string(),
            Some(first) => {
                let mut s = format!("{}{{{}", key.name(), label_pair(first));
                for label in labels {
                    s.push(',');
                    s.push_str(&label_pair(label));
                }
                s.push('}');
                s
            }
        }
    }
}

#[inline]
fn label_pair(label: &metrics::Label) -> String {
    format!("{}={}", label.key(), label.value())
}

// ---------------------------------------------------------------------------
// Recorder implementation
// ---------------------------------------------------------------------------

impl<ErrFn> Recorder for MetricsPeek<ErrFn>
where
    ErrFn: Fn(&str) + Clone + Send + Sync + 'static,
{
    // Descriptions are metadata-only; we don't need to store them.
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(CounterHandle {
            key: Self::format_key(key),
            tx: self.tx.clone(),
            err_cb: self.err_cb.clone(),
        }))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(GaugeHandle {
            key: Self::format_key(key),
            tx: self.tx.clone(),
            err_cb: self.err_cb.clone(),
        }))
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(HistogramHandle {
            key: Self::format_key(key),
            tx: self.tx.clone(),
            err_cb: self.err_cb.clone(),
        }))
    }
}
