use crate::cmd::{CounterCmd, GaugeCmd, HistogramCmd, MetricsCmd};
use metrics::{CounterFn, GaugeFn, HistogramFn};
use std::sync::mpsc::Sender;

// ---------------------------------------------------------------------------
// Counter
// ---------------------------------------------------------------------------

pub(crate) struct CounterHandle<F> {
    pub(crate) key: String,
    pub(crate) tx: Sender<MetricsCmd>,
    /// Called when sending a command fails (the receiver has been dropped).
    pub(crate) err_cb: F,
}

impl<F> CounterFn for CounterHandle<F>
where
    F: Fn(&str) + Send + Sync,
{
    fn increment(&self, value: u64) {
        if let Err(e) = self.tx.send(MetricsCmd::Counter(CounterCmd::Increment {
            key: self.key.clone(),
            value,
        })) {
            (self.err_cb)(&format!(
                "metrics-recorder: counter increment send failed: {e}"
            ));
        }
    }

    fn absolute(&self, value: u64) {
        if let Err(e) = self.tx.send(MetricsCmd::Counter(CounterCmd::Absolute {
            key: self.key.clone(),
            value,
        })) {
            (self.err_cb)(&format!(
                "metrics-recorder: counter absolute send failed: {e}"
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Gauge
// ---------------------------------------------------------------------------

pub(crate) struct GaugeHandle<F> {
    pub(crate) key: String,
    pub(crate) tx: Sender<MetricsCmd>,
    pub(crate) err_cb: F,
}

impl<F> GaugeFn for GaugeHandle<F>
where
    F: Fn(&str) + Send + Sync,
{
    fn increment(&self, value: f64) {
        if let Err(e) = self.tx.send(MetricsCmd::Gauge(GaugeCmd::Increment {
            key: self.key.clone(),
            value,
        })) {
            (self.err_cb)(&format!(
                "metrics-recorder: gauge increment send failed: {e}"
            ));
        }
    }

    fn decrement(&self, value: f64) {
        if let Err(e) = self.tx.send(MetricsCmd::Gauge(GaugeCmd::Decrement {
            key: self.key.clone(),
            value,
        })) {
            (self.err_cb)(&format!(
                "metrics-recorder: gauge decrement send failed: {e}"
            ));
        }
    }

    fn set(&self, value: f64) {
        if let Err(e) = self.tx.send(MetricsCmd::Gauge(GaugeCmd::Set {
            key: self.key.clone(),
            value,
        })) {
            (self.err_cb)(&format!("metrics-recorder: gauge set send failed: {e}"));
        }
    }
}

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

pub(crate) struct HistogramHandle<F> {
    pub(crate) key: String,
    pub(crate) tx: Sender<MetricsCmd>,
    pub(crate) err_cb: F,
}

impl<F> HistogramFn for HistogramHandle<F>
where
    F: Fn(&str) + Send + Sync,
{
    fn record(&self, value: f64) {
        if let Err(e) = self.tx.send(MetricsCmd::Histogram(HistogramCmd::Record {
            key: self.key.clone(),
            value,
        })) {
            (self.err_cb)(&format!(
                "metrics-recorder: histogram record send failed: {e}"
            ));
        }
    }
}
