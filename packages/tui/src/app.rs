use std::io;

use crossterm::{
    event::{Event, EventStream, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt as _;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Block, Borders, LineGauge, List, ListItem, Paragraph},
};

use crate::{
    config::{MetricConfig, MetricKind, TuiConfig},
    state::{LogEntry, MetricRowId, SharedState, TuiState},
};

// ── Terminal cleanup guard ────────────────────────────────────────────────────

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Async TUI event loop.  Spawned by [`crate::Tui::run`].
pub async fn run_tui(
    config: TuiConfig,
    state: SharedState,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    // Install a panic hook that always restores the terminal before printing
    // the panic message, so the user can actually read it.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    // ── Terminal setup ────────────────────────────────────────────────────────

    if let Err(e) = enable_raw_mode() {
        eprintln!("[macpepdb_tui] Failed to enable raw mode: {e}");
        return;
    }

    let mut stdout = io::stdout();
    if let Err(e) = execute!(stdout, EnterAlternateScreen) {
        let _ = disable_raw_mode();
        eprintln!("[macpepdb_tui] Failed to enter alternate screen: {e}");
        return;
    }

    let terminal = match Terminal::new(CrosstermBackend::new(stdout)) {
        Ok(t) => t,
        Err(e) => {
            let _ = disable_raw_mode();
            eprintln!("[macpepdb_tui] Failed to create terminal: {e}");
            return;
        }
    };

    let mut guard = TerminalGuard { terminal };
    let tick = config.tick_rate;
    let mut events = EventStream::new();

    // ── Event loop ────────────────────────────────────────────────────────────

    loop {
        // Render ──────────────────────────────────────────────────────────────
        guard
            .terminal
            .draw(|f| render_frame(f, &config, &state))
            .ok();

        // Wait for the earliest of: shutdown signal / tick / key event ────────
        let should_quit = tokio::select! {
            _ = &mut shutdown_rx => true,

            _ = tokio::time::sleep(tick) => false,

            event = events.next() => match event {
                Some(Ok(Event::Key(key))) => {
                    let quit = key.code == KeyCode::Char('q')
                        || (key.code == KeyCode::Char('c')
                            && key.modifiers.contains(KeyModifiers::CONTROL));
                    if !quit {
                        handle_key(key, &state);
                    }
                    quit
                }
                // Stream ended (e.g. stdin closed)
                None => true,
                // Resize / focus / other non-key events — just redraw
                _ => false,
            },
        };

        if should_quit {
            break;
        }
    }

    // TerminalGuard::drop() restores the terminal.
}

// ── Keyboard input ────────────────────────────────────────────────────────────

fn handle_key(key: crossterm::event::KeyEvent, state: &SharedState) {
    let mut s = state.write();
    let total = s.log_messages.len();

    match key.code {
        KeyCode::Up => {
            s.auto_scroll = false;
            s.log_scroll = s.log_scroll.saturating_sub(1);
        }
        KeyCode::Down => {
            let max = total.saturating_sub(1);
            s.log_scroll = (s.log_scroll + 1).min(max);
            if s.log_scroll >= max {
                s.auto_scroll = true;
            }
        }
        KeyCode::PageUp => {
            s.auto_scroll = false;
            s.log_scroll = s.log_scroll.saturating_sub(10);
        }
        KeyCode::PageDown => {
            let max = total.saturating_sub(1);
            s.log_scroll = (s.log_scroll + 10).min(max);
            if s.log_scroll >= max {
                s.auto_scroll = true;
            }
        }
        KeyCode::Home => {
            s.auto_scroll = false;
            s.log_scroll = 0;
        }
        KeyCode::End => {
            s.auto_scroll = true;
            s.log_scroll = total.saturating_sub(1);
        }
        _ => {}
    }
}

// ── Top-level frame ───────────────────────────────────────────────────────────

fn render_frame(f: &mut Frame, config: &TuiConfig, state: &SharedState) {
    let area = f.area();
    let state = state.read();

    // Outer chrome ────────────────────────────────────────────────────────────
    let outer = Block::default()
        .title(format!(" {} ", config.title))
        .title_alignment(Alignment::Center)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let inner = outer.inner(area);
    f.render_widget(outer, area);

    // Vertical layout: [metrics?] / log / status ──────────────────────────────
    let metrics_h = metrics_panel_height(&state.metrics_config);
    let status_h = 1u16;

    let chunks = if metrics_h == 0 {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(status_h)])
            .split(inner)
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(metrics_h),
                Constraint::Min(3),
                Constraint::Length(status_h),
            ])
            .split(inner)
    };

    if metrics_h > 0 {
        render_metrics(f, &state, chunks[0]);
        render_logs(f, &state, chunks[1]);
        render_status(f, chunks[2]);
    } else {
        render_logs(f, &state, chunks[0]);
        render_status(f, chunks[1]);
    }
}

// ── Metrics panel ─────────────────────────────────────────────────────────────

/// How many terminal rows the metrics panel needs (including its border).
fn metrics_panel_height(metrics: &[(MetricRowId, MetricConfig)]) -> u16 {
    if metrics.is_empty() {
        return 0;
    }
    let content: u16 = metrics
        .iter()
        .map(|(_, m)| match &m.kind {
            MetricKind::Progress { .. } => 2,
            _ => 1,
        })
        .sum();
    content + 2 // +2 for the Block border lines
}

fn render_metrics(f: &mut Frame, state: &TuiState, area: Rect) {
    let block = Block::default()
        .title(" Metrics ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let metrics = &state.metrics_config;

    if metrics.is_empty() || inner.height == 0 {
        return;
    }

    // One sub-row per metric
    let constraints: Vec<Constraint> = metrics
        .iter()
        .map(|(_, m)| match &m.kind {
            MetricKind::Progress { .. } => Constraint::Length(2),
            _ => Constraint::Length(1),
        })
        .collect();

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(inner);

    for (i, (_, metric)) in metrics.iter().enumerate() {
        if i >= rows.len() {
            break;
        }
        render_metric_row(f, metric, state, rows[i]);
    }
}

fn render_metric_row(f: &mut Frame, metric: &MetricConfig, state: &TuiState, area: Rect) {
    let value = state
        .metrics
        .get(&metric.key)
        .map(|e| e.current_value)
        .unwrap_or(0.0);

    match &metric.kind {
        MetricKind::Counter => {
            let p = Paragraph::new(Line::from(vec![
                Span::styled(
                    format!("{:<35}", metric.label),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format_integer(value as u64),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            ]));
            f.render_widget(p, area);
        }

        MetricKind::Gauge => {
            let p = Paragraph::new(Line::from(vec![
                Span::styled(
                    format!("{:<35}", metric.label),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("{:.4}", value),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            ]));
            f.render_widget(p, area);
        }

        MetricKind::Rate { window } => {
            let rate = state.compute_rate(&metric.key, window.as_secs_f64());
            let p = Paragraph::new(Line::from(vec![
                Span::styled(
                    format!("{:<35}", metric.label),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format_rate(rate),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
            ]));
            f.render_widget(p, area);
        }

        MetricKind::Progress { max } => {
            if area.height < 2 {
                return;
            }
            let sub = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(1), Constraint::Length(1)])
                .split(area);

            let ratio = if *max > 0.0 {
                (value / max).clamp(0.0, 1.0)
            } else {
                0.0
            };

            // First row: label + numeric summary
            let label_line = Paragraph::new(Line::from(vec![
                Span::styled(metric.label.clone(), Style::default().fg(Color::White)),
                Span::styled(
                    format!("  {:.0} / {:.0}  ({:.1}%)", value, max, ratio * 100.0),
                    Style::default().fg(Color::DarkGray),
                ),
            ]));
            f.render_widget(label_line, sub[0]);

            // Second row: the actual progress bar
            let gauge = LineGauge::default()
                .ratio(ratio)
                .label(Line::from(""))
                .filled_style(
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                )
                .unfilled_style(Style::default().fg(Color::DarkGray))
                .line_set(symbols::line::THICK);
            f.render_widget(gauge, sub[1]);
        }
    }
}

// ── Log panel ─────────────────────────────────────────────────────────────────

fn render_logs(f: &mut Frame, state: &TuiState, area: Rect) {
    let follow_hint = if state.auto_scroll {
        " ↓ following "
    } else {
        " ⏸ paused — End to follow "
    };

    let block = Block::default()
        .title(format!(" Log{follow_hint}"))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let visible = inner.height as usize;
    if visible == 0 {
        return;
    }

    let total = state.log_messages.len();
    let scroll_start = if state.auto_scroll {
        total.saturating_sub(visible)
    } else {
        state.log_scroll.min(total.saturating_sub(1))
    };

    let items: Vec<ListItem> = state
        .log_messages
        .iter()
        .skip(scroll_start)
        .take(visible)
        .map(make_log_item)
        .collect();

    f.render_widget(List::new(items), inner);
}

fn make_log_item(entry: &LogEntry) -> ListItem<'static> {
    let (level_str, level_style) = match entry.level {
        tracing::Level::ERROR => (
            "ERROR",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        tracing::Level::WARN => ("WARN ", Style::default().fg(Color::Yellow)),
        tracing::Level::INFO => ("INFO ", Style::default().fg(Color::Green)),
        tracing::Level::DEBUG => ("DEBUG", Style::default().fg(Color::Blue)),
        tracing::Level::TRACE => ("TRACE", Style::default().fg(Color::DarkGray)),
    };

    let time_str = entry.timestamp.format("%H:%M:%S%.3f").to_string();

    // Truncate long targets from the left so the most-specific part is visible.
    let target = &entry.target;
    let target_display = if target.len() > 24 {
        format!("…{}", &target[target.len() - 23..])
    } else {
        target.clone()
    };

    ListItem::new(Line::from(vec![
        Span::styled(time_str, Style::default().fg(Color::DarkGray)),
        Span::raw(" "),
        Span::styled(level_str.to_string(), level_style),
        Span::raw(" "),
        Span::styled(
            format!("[{target_display}]"),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(" "),
        Span::styled(entry.message.clone(), Style::default().fg(Color::White)),
    ]))
}

// ── Status bar ────────────────────────────────────────────────────────────────

fn render_status(f: &mut Frame, area: Rect) {
    let line = Line::from(vec![
        Span::styled(" q", Style::default().fg(Color::Yellow)),
        Span::styled(" / Ctrl+C", Style::default().fg(Color::DarkGray)),
        Span::styled("  quit", Style::default().fg(Color::DarkGray)),
        Span::raw("    "),
        Span::styled("↑ ↓ PgUp PgDn", Style::default().fg(Color::Yellow)),
        Span::styled("  scroll", Style::default().fg(Color::DarkGray)),
        Span::raw("    "),
        Span::styled("Home", Style::default().fg(Color::Yellow)),
        Span::styled("  top", Style::default().fg(Color::DarkGray)),
        Span::raw("    "),
        Span::styled("End", Style::default().fg(Color::Yellow)),
        Span::styled("  follow", Style::default().fg(Color::DarkGray)),
    ]);
    f.render_widget(Paragraph::new(line), area);
}

// ── Formatting helpers ────────────────────────────────────────────────────────

/// Format an integer with thousands-separator commas: `1234567` → `"1,234,567"`.
fn format_integer(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out.chars().rev().collect()
}

/// Format a rate with automatic SI prefix: `1500.0` → `"1.50K/s"`.
fn format_rate(rate: f64) -> String {
    if rate >= 1_000_000.0 {
        format!("{:.2}M/s", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.2}K/s", rate / 1_000.0)
    } else {
        format!("{:.2}/s", rate)
    }
}
