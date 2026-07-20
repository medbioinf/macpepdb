use tracing::{Event, Subscriber};
use tracing_subscriber::{Layer, layer::Context};

use crate::state::{LogEntry, SharedState};

// ── Field visitor ─────────────────────────────────────────────────────────────

/// Collects the `message` field and any extra key/value fields from a tracing
/// event so we can format a single display string.
struct FieldVisitor {
    message: String,
    extras: Vec<(String, String)>,
}

impl FieldVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
            extras: Vec::new(),
        }
    }

    /// Build the final display string: `<message>  key=value key=value …`
    fn into_display(self) -> String {
        if self.extras.is_empty() {
            return self.message;
        }
        let mut s = self.message;
        s.push_str("  ");
        for (i, (k, v)) in self.extras.iter().enumerate() {
            if i > 0 {
                s.push(' ');
            }
            s.push_str(k);
            s.push('=');
            s.push_str(v);
        }
        s
    }
}

impl tracing::field::Visit for FieldVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.extras
                .push((field.name().to_string(), value.to_string()));
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        // `tracing`'s info!/warn!/… macros store the message as fmt::Arguments
        // whose Debug impl just forwards to Display — no surrounding quotes.
        let formatted = format!("{value:?}");
        if field.name() == "message" {
            self.message = formatted;
        } else {
            self.extras.push((field.name().to_string(), formatted));
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.extras
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.extras
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        self.extras
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.extras
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.extras
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.extras
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.extras
                .push((field.name().to_string(), value.to_string()));
        }
    }
}

// ── TuiLayer ──────────────────────────────────────────────────────────────────

/// A [`tracing_subscriber::Layer`] that forwards every captured event into the
/// shared TUI state ring buffer so it can be rendered in the log panel.
///
/// Compose it with any existing subscriber:
///
/// ```rust,ignore
/// use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
///
/// let (tui, handle) = Tui::builder().build_split();
///
/// tracing_subscriber::registry()
///     .with(tracing_subscriber::EnvFilter::from_default_env())
///     .with(tui.layer())
///     .init();
///
/// let handle = tui.run_raw();
/// ```
pub struct TuiLayer {
    state: SharedState,
}

impl TuiLayer {
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for TuiLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();

        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);

        let entry = LogEntry {
            level: *meta.level(),
            target: meta.target().to_string(),
            message: visitor.into_display(),
            timestamp: chrono::Local::now(),
        };

        self.state.write().push_log(entry);
    }
}
