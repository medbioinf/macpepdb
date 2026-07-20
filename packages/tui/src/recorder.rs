use std::sync::Arc;

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};

use crate::state::SharedState;

// ── CounterFn ────────────────────────────────────────────────────────────────

struct TuiCounterFn {
    key: String,
    state: SharedState,
}

impl CounterFn for TuiCounterFn {
    fn increment(&self, value: u64) {
        self.state.write().increment_metric(&self.key, value);
    }

    fn absolute(&self, value: u64) {
        self.state.write().set_metric_value(&self.key, value as f64);
    }
}

// ── GaugeFn ──────────────────────────────────────────────────────────────────

struct TuiGaugeFn {
    key: String,
    state: SharedState,
}

impl GaugeFn for TuiGaugeFn {
    fn increment(&self, value: f64) {
        let mut state = self.state.write();
        let current = state
            .metrics
            .get(&self.key)
            .map(|e| e.current_value)
            .unwrap_or(0.0);
        state.set_metric_value(&self.key, current + value);
    }

    fn decrement(&self, value: f64) {
        let mut state = self.state.write();
        let current = state
            .metrics
            .get(&self.key)
            .map(|e| e.current_value)
            .unwrap_or(0.0);
        state.set_metric_value(&self.key, current - value);
    }

    fn set(&self, value: f64) {
        self.state.write().set_metric_value(&self.key, value);
    }
}

// ── HistogramFn ───────────────────────────────────────────────────────────────

struct TuiHistogramFn {
    key: String,
    state: SharedState,
}

impl HistogramFn for TuiHistogramFn {
    fn record(&self, value: f64) {
        self.state.write().set_metric_value(&self.key, value);
    }
}

// ── Recorder ─────────────────────────────────────────────────────────────────

/// A [`metrics::Recorder`] that stores every metric update in the shared TUI
/// state so it can be rendered in the terminal.
pub struct TuiRecorder {
    state: SharedState,
}

impl TuiRecorder {
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }
}

impl Recorder for TuiRecorder {
    // We deliberately ignore description calls – the TUI uses the human-readable
    // label from `MetricConfig` instead.
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(TuiCounterFn {
            key: key.name().to_string(),
            state: self.state.clone(),
        }))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(TuiGaugeFn {
            key: key.name().to_string(),
            state: self.state.clone(),
        }))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(TuiHistogramFn {
            key: key.name().to_string(),
            state: self.state.clone(),
        }))
    }
}
