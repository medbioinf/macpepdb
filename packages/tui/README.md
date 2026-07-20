# macpepdb_tui

A [`ratatui`]-powered terminal UI crate that provides a live dashboard with:

- **Log panel** — captures every `tracing` event and displays it in a
  scrollable, colour-coded list.
- **Metrics panel** — reads values from the [`metrics`] crate and renders
  them as counters, gauges, progress bars, or throughput rates.

---

## Layout

```
╭─────────────────────── My App ────────────────────────╮
│ ╭─ Metrics ────────────────────────────────────────╮  │
│ │ Records processed           42,000               │  │
│ │ Overall progress            42000 / 100000 (42%) │  │
│ │ ━━━━━━━━━━━━━━━━━━━━━╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌  │  │
│ │ Throughput                  1.23K/s               │  │
│ ╰──────────────────────────────────────────────────╯  │
│ ╭─ Log ↓ following ───────────────────────────────╮  │
│ │ 12:00:00.001 INFO  [my_app] Starting work…      │  │
│ │ 12:00:00.123 INFO  [my_app] Processing record 1 │  │
│ │ 12:00:00.456 WARN  [my_app] retrying batch      │  │
│ ╰──────────────────────────────────────────────────╯  │
│  q / Ctrl+C  quit    ↑↓ PgUp PgDn  scroll    End  follow │
╰───────────────────────────────────────────────────────╯
```

---

## Quick start

Add the crate to your workspace member and call `Tui::run()` early in `main`.
`run()` installs both a `tracing` subscriber (respecting `RUST_LOG`) and the
`metrics` global recorder, then spawns the render loop as a background Tokio task.

```rust
use std::time::Duration;
use macpepdb_tui::{MetricConfig, Tui};

#[tokio::main]
async fn main() {
    let handle = Tui::builder()
        .title("My App")
        // Show a plain counter
        .with_metric(MetricConfig::counter(
            "records_processed",
            "Records processed",
        ))
        // Show a progress bar (max = 100 000)
        .with_metric(MetricConfig::progress(
            "records_processed",
            "Overall progress",
            100_000.0,
        ))
        // Show throughput (rate over a 2-second sliding window)
        .with_metric(MetricConfig::rate_window(
            "records_processed",
            "Throughput",
            Duration::from_secs(2),
        ))
        .build()
        .run()
        .expect("failed to start TUI");

    // Use `tracing` and `metrics` macros as usual anywhere in your code.
    tracing::info!("Starting work…");

    for i in 0..100_000u64 {
        metrics::counter!("records_processed").increment(1);
        tracing::debug!(record = i, "processed");
        tokio::time::sleep(Duration::from_micros(100)).await;
    }

    // Graceful shutdown — restores the terminal before exiting.
    handle.stop().await;
}
```

---

## Metric kinds

| Constructor | Renders as |
|---|---|
| `MetricConfig::counter(key, label)` | Right-aligned integer with thousands separators |
| `MetricConfig::gauge(key, label)` | Floating-point value (4 decimal places) |
| `MetricConfig::progress(key, label, max)` | Two-row entry: numeric summary + `LineGauge` bar |
| `MetricConfig::rate(key, label)` | Rate over a 1 s sliding window, auto-scaled (`/s`, `K/s`, `M/s`) |
| `MetricConfig::rate_window(key, label, window)` | Same but with a custom window duration |

The same metrics key can appear multiple times with different kinds — for
example, display a running total **and** a rate for the same counter.

---

## Manual subscriber / recorder composition

If you already have a `tracing` subscriber stack, add `TuiLayer` yourself and
skip the automatic installation:

```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use macpepdb_tui::{MetricConfig, Tui};

let tui = Tui::builder()
    .with_metric(MetricConfig::counter("events", "Events"))
    .build();

// Compose the TUI layer with your existing layers.
tracing_subscriber::registry()
    .with(tracing_subscriber::EnvFilter::from_default_env())
    .with(tracing_subscriber::fmt::layer())  // keep normal stderr output too
    .with(tui.layer())                       // feed events into the TUI
    .init();

// Install the metrics recorder separately.
metrics::set_global_recorder(tui.recorder()).unwrap();

// Start the render loop (no global installs attempted).
let handle = tui.run_raw();
```

---

## Keyboard shortcuts

| Key | Action |
|---|---|
| `q` / `Ctrl+C` | Quit the TUI (sends shutdown, restores terminal) |
| `↑` / `↓` | Scroll log one line |
| `PgUp` / `PgDn` | Scroll log ten lines |
| `Home` | Jump to oldest log entry |
| `End` | Return to live tail (auto-scroll) |

When the log panel is in **following** mode new entries automatically keep the
view pinned to the bottom. Pressing any scroll key pauses following; pressing
`End` resumes it.

---

## Configuration

| Builder method | Default | Description |
|---|---|---|
| `.title(s)` | `"TUI"` | Title shown in the outer border |
| `.max_log_messages(n)` | `1000` | Log ring-buffer size |
| `.tick_rate(duration)` | `100 ms` | Screen refresh / input poll interval |
| `.with_metric(cfg)` | — | Append a metric row |
| `.with_metrics(iter)` | — | Replace the whole metrics list |

---

## Environment variables

| Variable | Effect |
|---|---|
| `RUST_LOG` | Standard `tracing-subscriber` filter (e.g. `debug`, `my_crate=trace`) |


```
Architecture

```
packages/tui/src/
├── lib.rs        – public API: Tui, TuiBuilder, TuiHandle, TuiError
├── config.rs     – TuiConfig, MetricConfig, MetricKind
├── state.rs      – SharedState (Arc<RwLock<TuiState>>), LogEntry, MetricEntry
├── recorder.rs   – metrics::Recorder impl (CounterFn / GaugeFn / HistogramFn)
├── layer.rs      – tracing_subscriber::Layer impl
└── app.rs        – async event loop + all ratatui rendering
```

### How it works

| Component | Role |
|---|---|
| **`TuiLayer`** | `tracing_subscriber::Layer` that captures every `info!`/`warn!`/… event and pushes it into the shared `RwLock<TuiState>` log ring-buffer. |
| **`TuiRecorder`** | `metrics::Recorder` that wraps every counter/gauge/histogram in a thin `Arc<dyn Fn…>` which writes into the same shared state. Rate history (30 s ring) is recorded on every update. |
| **`run_tui` task** | Async Tokio task that uses a `crossterm::EventStream` + `tokio::select!` to either redraw on a configurable tick (default 100 ms) or immediately on a keypress. A `TerminalGuard` in `Drop` ensures the terminal is always restored even on panic. |

### Metric kinds

| Kind | Rendered as |
|---|---|
| `MetricConfig::counter(key, label)` | Right-aligned integer, e.g. `1,234,567` |
| `MetricConfig::gauge(key, label)` | Float to 4 dp |
| `MetricConfig::progress(key, label, max)` | Two rows: numeric summary + `LineGauge` bar |
| `MetricConfig::rate(key, label)` | Sliding-window rate, auto-scaled (`/s`, `K/s`, `M/s`) |
| `MetricConfig::rate_window(key, label, dur)` | Same with custom window |

The **same key** can appear with multiple kinds — e.g. a counter total *and* a rate in the same panel.

### Typical usage

```macpepdb-new-new/packages/tui/src/lib.rs#L12-29
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
```

`Tui::run()` installs both the tracing subscriber (RUST_LOG-aware) and the metrics recorder globally. If you already manage those yourself, use `tui.layer()` + `tui.recorder()` + `tui.run_raw()` instead.
```
