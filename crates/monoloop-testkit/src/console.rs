//! Append-only console renderer for canonical events (test adapter).

use monoloop_contracts::{
    CanonicalUnit, CanonicalUnitEvent, InterpretationEnd, InterpreterOutputEvent, TextChannel,
    ToolRequestState, UnitState,
};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Configuration for the console renderer.
#[derive(Clone, Debug)]
pub struct ConsoleRendererConfig {
    /// Include paragraph/structure verbose lines.
    pub verbose: bool,
    /// Show tool request payloads (bounded; default false).
    pub show_tool_payloads: bool,
    /// Maximum content characters per line (escape + truncate).
    pub max_content_chars: usize,
}

impl Default for ConsoleRendererConfig {
    fn default() -> Self {
        Self {
            verbose: false,
            show_tool_payloads: false,
            max_content_chars: 500,
        }
    }
}

/// Sink for rendered lines.
pub trait ConsoleSink: Send + Sync {
    /// Write one complete record (already includes newline).
    fn write_line(&self, line: &str);
}

/// Sync-friendly memory sink.
#[derive(Clone, Default)]
pub struct SyncMemorySink {
    lines: Arc<std::sync::Mutex<Vec<String>>>,
}

impl SyncMemorySink {
    /// Create empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot.
    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().expect("sink").clone()
    }

    /// Joined output.
    pub fn join(&self) -> String {
        self.lines.lock().expect("sink").join("")
    }
}

impl ConsoleSink for SyncMemorySink {
    fn write_line(&self, line: &str) {
        self.lines.lock().expect("sink").push(line.to_string());
    }
}

/// Stdout sink (serialized).
pub struct StdoutSink;

impl ConsoleSink for StdoutSink {
    fn write_line(&self, line: &str) {
        print!("{line}");
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

/// One logical rendered record (before TTY wrapping).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsoleRenderRecord {
    /// Full line including trailing newline.
    pub line: String,
}

/// Passive-only console renderer.
pub struct ConsoleRenderer {
    config: ConsoleRendererConfig,
    sink: Arc<dyn ConsoleSink>,
}

impl ConsoleRenderer {
    /// Create a renderer.
    pub fn new(config: ConsoleRendererConfig, sink: Arc<dyn ConsoleSink>) -> Self {
        Self { config, sink }
    }

    /// Render one interpreter output event immediately.
    pub fn render(&self, event: &InterpreterOutputEvent) -> ConsoleRenderRecord {
        let line = match event {
            InterpreterOutputEvent::Unit(u) => self.format_unit(u),
            InterpreterOutputEvent::Ended(end) => self.format_end(end),
        };
        self.sink.write_line(&line);
        ConsoleRenderRecord { line }
    }

    /// Spawn a task that consumes a channel of events until Ended (inclusive).
    pub fn spawn_consumer(
        self: &Arc<Self>,
        mut rx: mpsc::Receiver<InterpreterOutputEvent>,
    ) -> tokio::task::JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                let is_end = matches!(ev, InterpreterOutputEvent::Ended(_));
                this.render(&ev);
                if is_end {
                    break;
                }
            }
        })
    }

    fn format_unit(&self, event: &CanonicalUnitEvent) -> String {
        let s = event.snapshot();
        let corr = format!(
            "[c:{} i:{} f:{} l:{} u:{} g:{}]",
            short_id(s.connection_id.as_str()),
            short_id(s.interpretation_id.as_str()),
            s.flow_id.as_str(),
            s.lane_id.as_str(),
            short_id(s.unit_id.as_str()),
            s.unit_generation,
        );
        let (kind_state, label, content) = match &s.unit {
            CanonicalUnit::Text(t) => {
                let state = unit_state_label(s.unit_state);
                (
                    format!("text/{state}"),
                    t.channel.label().to_string(),
                    escape_content(&t.content, self.config.max_content_chars),
                )
            }
            CanonicalUnit::Tool(t) => {
                let state = tool_state_label(t);
                let name = t.tool_name.as_deref().unwrap_or("?");
                let mut content = t
                    .waiting_for
                    .clone()
                    .unwrap_or_else(|| state.clone());
                if self.config.show_tool_payloads {
                    if let Some(ref p) = t.request_payload {
                        content.push_str(" args=");
                        content.push_str(&escape_content(p, self.config.max_content_chars));
                    }
                }
                (
                    format!("tool/{state}"),
                    name.to_string(),
                    escape_content(&content, self.config.max_content_chars),
                )
            }
            CanonicalUnit::Boundary(b) => (
                "boundary/complete".into(),
                format!("{:?}", b.kind),
                String::new(),
            ),
            CanonicalUnit::Diagnostic(d) => (
                "diagnostic".into(),
                format!("{:?}", d.kind),
                escape_content(&d.message, self.config.max_content_chars),
            ),
            CanonicalUnit::Structure(st) => (
                "structure/complete".into(),
                format!("{:?}", st.kind),
                escape_content(&st.content, self.config.max_content_chars),
            ),
            CanonicalUnit::Paragraph(p) => (
                format!("paragraph/{:?}", p.kind),
                TextChannel::PublicResponse.label().into(),
                String::new(),
            ),
            CanonicalUnit::Usage(u) => (
                "usage".into(),
                "tokens".into(),
                format!("{u:?}"),
            ),
        };
        format!("{corr} {kind_state} {label} {content}\n")
    }

    fn format_end(&self, end: &InterpretationEnd) -> String {
        format!(
            "[c:{} i:{}] interpretation/{:?} events={} sentences={} unresolved_bytes={}\n",
            short_id(end.connection_id.as_str()),
            short_id(end.interpretation_id.as_str()),
            end.kind,
            end.canonical_event_count,
            end.completed_sentence_count,
            end.unresolved_text_bytes,
        )
    }
}

fn tool_state_label(t: &monoloop_contracts::ToolActionEvent) -> String {
    if t.terminal_outcome.is_some() {
        return "complete".into();
    }
    match t.request_state {
        ToolRequestState::Ready => "ready".into(),
        ToolRequestState::Assembling => "waiting".into(),
        ToolRequestState::Incomplete => "incomplete".into(),
        ToolRequestState::Malformed => "malformed".into(),
    }
}

fn unit_state_label(s: UnitState) -> &'static str {
    match s {
        UnitState::Complete => "complete",
        UnitState::Waiting => "waiting",
        UnitState::Incomplete => "incomplete",
        UnitState::Malformed => "malformed",
    }
}

fn short_id(id: &str) -> &str {
    if id.len() <= 8 {
        id
    } else {
        &id[..8]
    }
}

fn escape_content(s: &str, max: usize) -> String {
    let mut out = String::with_capacity(s.len().min(max) + 8);
    for (i, ch) in s.chars().enumerate() {
        if i >= max {
            out.push('…');
            break;
        }
        match ch {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{1b}' => out.push_str("\\e"),
            c if c.is_control() => out.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => out.push(c),
        }
    }
    out
}
