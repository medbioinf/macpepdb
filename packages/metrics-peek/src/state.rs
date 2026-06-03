use crate::cmd::{CounterCmd, GaugeCmd, HistogramCmd, MetricsCmd};
use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Public state tracker
// ---------------------------------------------------------------------------

/// Holds the in-memory snapshot of every metric that has been updated since
/// the last call to [`MetricsState::take_logs`].
#[derive(Default)]
pub(crate) struct MetricsState {
    // Current values
    counter_state: HashMap<String, u64>,
    gauge_state: HashMap<String, f64>,
    histogram_state: HashMap<String, HistogramState>,

    // Keys touched since the last flush — only these appear in the output.
    counter_updates: HashSet<String>,
    gauge_updates: HashSet<String>,
    histogram_updates: HashSet<String>,
}

impl MetricsState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Apply a single command to the in-memory state.
    pub(crate) fn update(&mut self, cmd: MetricsCmd) {
        match cmd {
            MetricsCmd::Counter(c) => match c {
                CounterCmd::Increment { key, value } => {
                    *self.counter_state.entry(key.clone()).or_insert(0) += value;
                    self.counter_updates.insert(key);
                }
                CounterCmd::Absolute { key, value } => {
                    self.counter_state.insert(key.clone(), value);
                    self.counter_updates.insert(key);
                }
            },
            MetricsCmd::Gauge(g) => match g {
                GaugeCmd::Increment { key, value } => {
                    *self.gauge_state.entry(key.clone()).or_insert(0.0) += value;
                    self.gauge_updates.insert(key);
                }
                GaugeCmd::Decrement { key, value } => {
                    *self.gauge_state.entry(key.clone()).or_insert(0.0) -= value;
                    self.gauge_updates.insert(key);
                }
                GaugeCmd::Set { key, value } => {
                    self.gauge_state.insert(key.clone(), value);
                    self.gauge_updates.insert(key);
                }
            },
            MetricsCmd::Histogram(h) => match h {
                HistogramCmd::Record { key, value } => {
                    self.histogram_state
                        .entry(key.clone())
                        .or_default()
                        .update(value);
                    self.histogram_updates.insert(key);
                }
            },
        }
    }

    /// Drain all pending updates into a formatted string, clearing the
    /// "dirty" sets in the process.  Returns `None` when nothing changed.
    pub(crate) fn take_logs(&mut self) -> Option<String> {
        let mut out = String::new();

        for key in self.counter_updates.drain() {
            if let Some(v) = self.counter_state.get(&key) {
                out.push_str(&format!("Counter   {key} = {v}\n"));
            }
        }

        for key in self.gauge_updates.drain() {
            if let Some(v) = self.gauge_state.get(&key) {
                out.push_str(&format!("Gauge     {key} = {v:.6}\n"));
            }
        }

        for key in self.histogram_updates.drain() {
            if let Some(h) = self.histogram_state.get(&key) {
                let avg = h.avg().unwrap_or(0.0);
                let std_dev = h.std_dev().unwrap_or(0.0);
                out.push_str(&format!(
                    "Histogram {key} — avg: {avg:.4}, std_dev: {std_dev:.4}, \
                     min: {min:.4}, max: {max:.4}, samples: {n}\n",
                    min = h.min,
                    max = h.max,
                    n = h.num_samples,
                ));
            }
        }

        if out.is_empty() { None } else { Some(out) }
    }
}

// ---------------------------------------------------------------------------
// Per-histogram rolling statistics (online algorithm, no allocations)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct HistogramState {
    sum: f64,
    sum_sq: f64,
    pub(crate) num_samples: u64,
    pub(crate) min: f64,
    pub(crate) max: f64,
}

impl HistogramState {
    pub(crate) fn update(&mut self, value: f64) {
        self.sum += value;
        self.sum_sq += value * value;
        if self.num_samples == 0 {
            self.min = value;
            self.max = value;
        } else {
            if value < self.min {
                self.min = value;
            }
            if value > self.max {
                self.max = value;
            }
        }
        self.num_samples += 1;
    }

    pub(crate) fn avg(&self) -> Option<f64> {
        if self.num_samples == 0 {
            None
        } else {
            Some(self.sum / self.num_samples as f64)
        }
    }

    /// Population standard deviation computed via the computational formula:
    /// σ = sqrt( E[X²] − (E[X])² )
    pub(crate) fn std_dev(&self) -> Option<f64> {
        if self.num_samples == 0 {
            return None;
        }
        let mean = self.avg().unwrap();
        let mean_sq = self.sum_sq / self.num_samples as f64;
        // Clamp to 0 to guard against tiny negative values from f64 rounding.
        Some((mean_sq - mean * mean).max(0.0).sqrt())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn histogram_of(values: &[f64]) -> HistogramState {
        let mut h = HistogramState::default();
        for &v in values {
            h.update(v);
        }
        h
    }

    #[test]
    fn avg_basic() {
        let h = histogram_of(&[10.0, 20.0, 30.0]);
        assert_eq!(h.avg(), Some(20.0));
    }

    #[test]
    fn avg_empty() {
        assert_eq!(HistogramState::default().avg(), None);
    }

    #[test]
    fn std_dev_basic() {
        // σ = sqrt((100+400+900)/3 − 20²) = sqrt(466.67 − 400) = sqrt(66.67) ≈ 8.165
        let h = histogram_of(&[10.0, 20.0, 30.0]);
        let sd = h.std_dev().unwrap();
        assert!((sd - 8.165).abs() < 0.001, "std_dev was {sd}");
    }

    #[test]
    fn std_dev_empty() {
        assert_eq!(HistogramState::default().std_dev(), None);
    }

    #[test]
    fn min_max() {
        let h = histogram_of(&[15.0, 5.0, 25.0]);
        assert_eq!(h.min, 5.0);
        assert_eq!(h.max, 25.0);
    }

    #[test]
    fn state_counter_increment() {
        let mut s = MetricsState::new();
        s.update(MetricsCmd::Counter(CounterCmd::Increment {
            key: "hits".into(),
            value: 3,
        }));
        s.update(MetricsCmd::Counter(CounterCmd::Increment {
            key: "hits".into(),
            value: 7,
        }));
        let log = s.take_logs().unwrap();
        assert!(log.contains("hits = 10"), "log was: {log}");
        // Second call should produce nothing (no new updates).
        assert!(s.take_logs().is_none());
    }

    #[test]
    fn state_gauge_set() {
        let mut s = MetricsState::new();
        s.update(MetricsCmd::Gauge(GaugeCmd::Set {
            key: "temp".into(),
            value: 36.6,
        }));
        let log = s.take_logs().unwrap();
        assert!(log.contains("temp"), "log was: {log}");
    }

    #[test]
    fn state_histogram_record() {
        let mut s = MetricsState::new();
        for v in [1.0_f64, 2.0, 3.0] {
            s.update(MetricsCmd::Histogram(HistogramCmd::Record {
                key: "latency".into(),
                value: v,
            }));
        }
        let log = s.take_logs().unwrap();
        assert!(log.contains("latency"), "log was: {log}");
        assert!(log.contains("samples: 3"), "log was: {log}");
    }
}
