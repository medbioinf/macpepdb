pub(crate) enum CounterCmd {
    Increment { key: String, value: u64 },
    Absolute { key: String, value: u64 },
}

pub(crate) enum GaugeCmd {
    Increment { key: String, value: f64 },
    Decrement { key: String, value: f64 },
    Set { key: String, value: f64 },
}

pub(crate) enum HistogramCmd {
    Record { key: String, value: f64 },
}

pub(crate) enum MetricsCmd {
    Counter(CounterCmd),
    Gauge(GaugeCmd),
    Histogram(HistogramCmd),
}
