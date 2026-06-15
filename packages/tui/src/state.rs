use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::RwLock;

use crate::config::MetricConfig;

/// Opaque identifier for a single display row in the Metrics panel.
///
/// Returned by [`TuiState::add_metric`] and [`TuiHandle::add_metric`].
/// Pass it to [`TuiState::remove_metric_row`] to remove exactly that row
/// without affecting other rows that share the same underlying metric key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MetricRowId(u64);

/// A single captured tracing event shown in the log panel.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub level: tracing::Level,
    /// The tracing target (module path).
    pub target: String,
    /// Formatted event message, including any extra fields.
    pub message: String,
    /// Wall-clock time the event was captured.
    pub timestamp: chrono::DateTime<chrono::Local>,
}

/// Internal storage for one tracked metric key.
pub struct MetricEntry {
    /// Latest recorded value.
    pub current_value: f64,
    /// Ring buffer of `(timestamp, value)` snapshots used to compute rates and
    /// to provide a history for the sliding-window rate estimator.
    pub history: VecDeque<(Instant, f64)>,
}

/// All state shared between the recorder, the tracing layer, and the renderer.
pub struct TuiState {
    /// Captured log messages (newest at the back).
    pub log_messages: VecDeque<LogEntry>,
    /// Hard cap on the log ring buffer.
    pub max_log_messages: usize,
    /// All metric values keyed by the metrics-crate key name.
    pub metrics: HashMap<String, MetricEntry>,
    /// Ordered list of metric display rows shown in the Metrics panel.
    ///
    /// Each entry pairs a stable [`MetricRowId`] with its [`MetricConfig`].
    /// The same underlying metric key may appear more than once (e.g. once as
    /// a counter, once as a rate) because rows are identified by their
    /// `MetricRowId`, not by the metric key.  Mutate it at runtime with
    /// [`TuiState::add_metric`], [`TuiState::remove_metric`],
    /// [`TuiState::remove_metric_row`], or [`TuiState::set_metrics`].
    pub metrics_config: Vec<(MetricRowId, MetricConfig)>,
    /// Monotonically increasing counter used to mint fresh [`MetricRowId`]s.
    next_row_id: u64,
    /// Index of the first *visible* log line when the user has scrolled up.
    pub log_scroll: usize,
    /// When `true` the log panel always shows the most-recent entries.
    pub auto_scroll: bool,
}

/// `Arc<RwLock<TuiState>>` — cheap to clone and share across threads.
pub type SharedState = Arc<RwLock<TuiState>>;

// ── History window ───────────────────────────────────────────────────────────

/// How many seconds of history each metric keeps.
const HISTORY_WINDOW_SECS: f64 = 30.0;

// ── TuiState impl ────────────────────────────────────────────────────────────

impl TuiState {
    /// Create a fresh state pre-populated with entries for every configured
    /// metric so they show up immediately (with value 0) in the panel.
    pub fn new(max_log_messages: usize, metric_configs: &[MetricConfig]) -> Self {
        let mut metrics = HashMap::new();
        for cfg in metric_configs {
            metrics
                .entry(cfg.key.clone())
                .or_insert_with(|| MetricEntry {
                    current_value: 0.0,
                    history: VecDeque::new(),
                });
        }
        let metrics_config = metric_configs
            .iter()
            .enumerate()
            .map(|(i, cfg)| (MetricRowId(i as u64), cfg.clone()))
            .collect();
        Self {
            log_messages: VecDeque::new(),
            max_log_messages,
            metrics,
            metrics_config,
            next_row_id: metric_configs.len() as u64,
            log_scroll: 0,
            auto_scroll: true,
        }
    }

    // ── Metrics-config helpers ────────────────────────────────────────────────

    /// Append a new metric display row to the Metrics panel and return its
    /// unique [`MetricRowId`].
    ///
    /// The same key can be added multiple times with different [`MetricKind`]s
    /// (e.g. a counter row **and** a rate row for the same underlying key).
    /// Each call gets a distinct `MetricRowId` so the rows can be removed
    /// independently via [`TuiState::remove_metric_row`].
    /// If no live value exists yet for the key a zero-valued entry is created
    /// so the row shows up immediately.
    pub fn add_metric(&mut self, config: MetricConfig) -> MetricRowId {
        self.metrics
            .entry(config.key.clone())
            .or_insert_with(|| MetricEntry {
                current_value: 0.0,
                history: VecDeque::new(),
            });
        let id = MetricRowId(self.next_row_id);
        self.next_row_id += 1;
        self.metrics_config.push((id, config));
        id
    }

    /// Remove **all** display rows whose key equals `key`.
    ///
    /// The underlying live value and history for that key are intentionally
    /// kept so that re-adding the metric later continues from where it left
    /// off.  Call [`TuiState::clear_metric_data`] if you want to wipe the
    /// data too.
    ///
    /// To remove a single row without affecting others that share the same
    /// key, use [`TuiState::remove_metric_row`] with the [`MetricRowId`]
    /// returned by [`TuiState::add_metric`].
    pub fn remove_metric(&mut self, key: &str) {
        self.metrics_config.retain(|(_, c)| c.key != key);
    }

    /// Remove the single display row identified by `id`.
    ///
    /// Other rows that happen to share the same underlying metric key are left
    /// untouched.  This is the companion to [`TuiState::add_metric`]'s return
    /// value and allows showing the same metric in multiple display styles
    /// simultaneously while still being able to remove each row independently.
    pub fn remove_metric_row(&mut self, id: MetricRowId) {
        self.metrics_config.retain(|(row_id, _)| *row_id != id);
    }

    /// Replace the entire display list in one atomic write.
    ///
    /// Useful when you want to swap out many rows at once without holding
    /// the lock across multiple individual calls.  Any key that does not yet
    /// have a live entry gets a zero-valued one.
    ///
    /// Returns the [`MetricRowId`] assigned to each row in the same order as
    /// the provided configs.
    pub fn set_metrics(&mut self, configs: impl IntoIterator<Item = MetricConfig>) -> Vec<MetricRowId> {
        let configs: Vec<MetricConfig> = configs.into_iter().collect();
        for cfg in &configs {
            self.metrics
                .entry(cfg.key.clone())
                .or_insert_with(|| MetricEntry {
                    current_value: 0.0,
                    history: VecDeque::new(),
                });
        }
        let ids: Vec<MetricRowId> = (self.next_row_id..self.next_row_id + configs.len() as u64)
            .map(MetricRowId)
            .collect();
        self.next_row_id += configs.len() as u64;
        self.metrics_config = ids.iter().copied().zip(configs).collect();
        ids
    }

    /// Erase the stored value and history for `key`.
    ///
    /// This does **not** remove the display row(s) from [`metrics_config`]; if
    /// you want to hide the row too call [`TuiState::remove_metric`] first (or
    /// afterwards).
    pub fn clear_metric_data(&mut self, key: &str) {
        self.metrics.remove(key);
    }

    // ── Log helpers ──────────────────────────────────────────────────────────

    /// Append a log entry, evicting the oldest if the ring buffer is full.
    pub fn push_log(&mut self, entry: LogEntry) {
        if self.log_messages.len() >= self.max_log_messages {
            self.log_messages.pop_front();
            // Keep the scroll position pointing at the same logical line.
            self.log_scroll = self.log_scroll.saturating_sub(1);
        }
        self.log_messages.push_back(entry);
    }

    // ── Metric helpers ───────────────────────────────────────────────────────

    /// Overwrite the metric with an absolute value and record a history point.
    pub fn set_metric_value(&mut self, key: &str, value: f64) {
        let now = Instant::now();
        let entry = self
            .metrics
            .entry(key.to_string())
            .or_insert_with(|| MetricEntry {
                current_value: 0.0,
                history: VecDeque::new(),
            });
        entry.current_value = value;
        entry.history.push_back((now, value));
        prune_history(&mut entry.history, now, HISTORY_WINDOW_SECS);
    }

    /// Add `delta` to the metric's current value and record a history point.
    pub fn increment_metric(&mut self, key: &str, delta: u64) {
        let now = Instant::now();
        let entry = self
            .metrics
            .entry(key.to_string())
            .or_insert_with(|| MetricEntry {
                current_value: 0.0,
                history: VecDeque::new(),
            });
        entry.current_value += delta as f64;
        let v = entry.current_value;
        entry.history.push_back((now, v));
        prune_history(&mut entry.history, now, HISTORY_WINDOW_SECS);
    }

    /// Apply an arbitrary function `f(current) -> new` to the metric's value
    /// (used for gauge increment / decrement) and record a history point.
    pub fn modify_metric<F>(&mut self, key: &str, f: F)
    where
        F: FnOnce(f64) -> f64,
    {
        let now = Instant::now();
        let entry = self
            .metrics
            .entry(key.to_string())
            .or_insert_with(|| MetricEntry {
                current_value: 0.0,
                history: VecDeque::new(),
            });
        entry.current_value = f(entry.current_value);
        let v = entry.current_value;
        entry.history.push_back((now, v));
        prune_history(&mut entry.history, now, HISTORY_WINDOW_SECS);
    }

    // ── Rate computation ─────────────────────────────────────────────────────

    /// Estimate the rate of change for `key` over `window_secs`.
    ///
    /// Returns 0.0 if there is not enough history or the elapsed time is
    /// negligibly small.
    pub fn compute_rate(&self, key: &str, window_secs: f64) -> f64 {
        let Some(entry) = self.metrics.get(key) else {
            return 0.0;
        };
        if entry.history.len() < 2 {
            return 0.0;
        }

        let now = Instant::now();
        let cutoff = now
            .checked_sub(Duration::from_secs_f64(window_secs.max(0.001)))
            .unwrap_or(now);

        // Find the oldest sample that still falls within the window.
        let Some((oldest_time, oldest_value)) = entry.history.iter().find(|(t, _)| *t >= cutoff)
        else {
            // All history is older than the window — use the very oldest point
            // available so we at least show *something*.
            if let (Some((t0, v0)), Some((_, v1))) = (entry.history.front(), entry.history.back()) {
                let elapsed = now.duration_since(*t0).as_secs_f64();
                if elapsed > 0.001 {
                    return (v1 - v0).max(0.0) / elapsed;
                }
            }
            return 0.0;
        };

        let elapsed = now.duration_since(*oldest_time).as_secs_f64();
        if elapsed < 0.001 {
            return 0.0;
        }

        (entry.current_value - oldest_value).max(0.0) / elapsed
    }
}

// ── private helpers ──────────────────────────────────────────────────────────

fn prune_history(history: &mut VecDeque<(Instant, f64)>, now: Instant, max_secs: f64) {
    while history
        .front()
        .map(|(t, _)| now.duration_since(*t).as_secs_f64() > max_secs)
        .unwrap_or(false)
    {
        history.pop_front();
    }
}
