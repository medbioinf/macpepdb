use std::time::Duration;

/// Top-level configuration for the TUI.
#[derive(Debug, Clone)]
pub struct TuiConfig {
    /// Title shown in the outer border.
    pub title: String,
    /// Maximum number of log messages to keep in the ring buffer.
    pub max_log_messages: usize,
    /// How often the screen is redrawn (also the input polling interval).
    pub tick_rate: Duration,
    /// Ordered list of metrics to display in the Metrics panel.
    pub metrics: Vec<MetricConfig>,
}

impl Default for TuiConfig {
    fn default() -> Self {
        Self {
            title: String::from("TUI"),
            max_log_messages: 1_000,
            tick_rate: Duration::from_millis(100),
            metrics: Vec::new(),
        }
    }
}

/// Configuration for a single metric row in the Metrics panel.
#[derive(Debug, Clone)]
pub struct MetricConfig {
    /// The exact key used with the `metrics` crate macros,
    /// e.g. `counter!("proteins_processed")`.
    pub key: String,
    /// Human-readable label shown in the panel.
    pub label: String,
    /// How this metric should be visualised.
    pub kind: MetricKind,
}

/// How a metric value is rendered in the Metrics panel.
#[derive(Debug, Clone)]
pub enum MetricKind {
    /// Plain integer counter — e.g. `42,000`.
    Counter,
    /// Raw floating-point value — e.g. `3.1415`.
    Gauge,
    /// A two-row progress bar with a fixed maximum.
    Progress {
        /// Upper bound of the progress bar (maps to 100 %).
        max: f64,
    },
    /// Rate of change of a counter over a sliding time window, shown as
    /// `N/s`, `N.xxK/s`, or `N.xxM/s`.
    Rate {
        /// Length of the sliding window used to estimate the rate.
        window: Duration,
    },
}

// ── Convenience constructors ────────────────────────────────────────────────

impl MetricConfig {
    /// Create a plain integer counter metric.
    pub fn counter(key: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
            kind: MetricKind::Counter,
        }
    }

    /// Create a raw floating-point gauge metric.
    pub fn gauge(key: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
            kind: MetricKind::Gauge,
        }
    }

    /// Create a progress-bar metric with the given maximum value.
    pub fn progress(key: impl Into<String>, label: impl Into<String>, max: f64) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
            kind: MetricKind::Progress { max },
        }
    }

    /// Create a rate metric using a default 1-second sliding window.
    pub fn rate(key: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
            kind: MetricKind::Rate {
                window: Duration::from_secs(1),
            },
        }
    }

    /// Create a rate metric with a custom sliding window duration.
    pub fn rate_window(key: impl Into<String>, label: impl Into<String>, window: Duration) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
            kind: MetricKind::Rate { window },
        }
    }
}
