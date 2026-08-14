//! HTML projection of canonical events for visual interpretation checks.
//!
//! **Test kit only.** Builds a reviewable HTML page from *already assembled*
//! canonical units — it does not re-parse raw Grok bytes or invent completeness.
//!
//! Layout:
//! 1. **Assembled document** — public response sentences joined as Markdown, then
//!    rendered to HTML (markdown → HTML) so you can see if serialisation reads right.
//! 2. **Event timeline** — every canonical unit generation with correlation, for
//!    spotting missing tools, partial escapes, or order issues.

use monoloop_contracts::{
    BoundaryKind, CanonicalUnit, InterpretationEnd, InterpreterOutputEvent, StructureKind,
    TextChannel, ToolRequestState, UnitState,
};
use pulldown_cmark::{html, Options, Parser};
use std::path::Path;

/// Options for HTML dump generation.
#[derive(Clone, Debug)]
pub struct HtmlReportParams {
    /// Include the event timeline section.
    pub include_timeline: bool,
    /// Include reasoning-summary channel in a separate document section.
    pub include_reasoning: bool,
    /// Show tool request payloads in the timeline (bounded).
    pub show_tool_payloads: bool,
    /// Max chars of tool payload shown.
    pub max_payload_chars: usize,
    /// Page title.
    pub title: String,
}

impl Default for HtmlReportParams {
    fn default() -> Self {
        Self {
            include_timeline: true,
            include_reasoning: true,
            show_tool_payloads: true,
            max_payload_chars: 800,
            title: "Monoloop interpretation review".into(),
        }
    }
}

/// Built HTML report from a run's canonical events.
#[derive(Clone, Debug)]
pub struct HtmlReport {
    /// Markdown assembled from complete public_response sentences only.
    pub assembled_markdown: String,
    /// Markdown → HTML for the assembled document body.
    pub document_html: String,
    /// Full self-contained HTML page (document + timeline + CSS).
    pub full_page_html: String,
    /// Number of complete public_response sentences used.
    pub sentence_count: usize,
    /// Number of timeline rows.
    pub timeline_rows: usize,
}

/// Build an HTML report from interpreter output events (canonical only).
pub fn build_html_report(
    events: &[InterpreterOutputEvent],
    params: &HtmlReportParams,
) -> HtmlReport {
    let mut public_sentences: Vec<String> = Vec::new();
    let mut reasoning_sentences: Vec<String> = Vec::new();
    let mut timeline: Vec<TimelineRow> = Vec::new();
    let mut end: Option<&InterpretationEnd> = None;

    for ev in events {
        match ev {
            InterpreterOutputEvent::Unit(unit_ev) => {
                let snap = unit_ev.snapshot();
                match &snap.unit {
                    CanonicalUnit::Text(t) => {
                        match t.channel {
                            TextChannel::PublicResponse => {
                                public_sentences.push(t.content.clone());
                            }
                            TextChannel::PublicReasoningSummary => {
                                reasoning_sentences.push(t.content.clone());
                            }
                            TextChannel::StatusNarration | TextChannel::QuotedExternalContent => {}
                        }
                        timeline.push(TimelineRow {
                            lifecycle: unit_ev.lifecycle_label().to_string(),
                            kind: "text".into(),
                            state: format!("{:?}", snap.unit_state),
                            label: t.channel.label().to_string(),
                            correlation: format!(
                                "c:{} i:{} u:{} g:{}",
                                short(snap.connection_id.as_str()),
                                short(snap.interpretation_id.as_str()),
                                short(snap.unit_id.as_str()),
                                snap.unit_generation
                            ),
                            body: t.content.clone(),
                            css_class: "ev-text".into(),
                        });
                    }
                    CanonicalUnit::Tool(t) => {
                        let name = t.tool_name.as_deref().unwrap_or("?");
                        let mut body = format!(
                            "request={:?} exec={:?} result={:?}",
                            t.request_state, t.execution_state, t.result_state
                        );
                        if let Some(ref w) = t.waiting_for {
                            body.push_str(&format!(" waiting_for={w}"));
                        }
                        if params.show_tool_payloads {
                            if let Some(ref p) = t.request_payload {
                                if t.request_state == ToolRequestState::Ready {
                                    body.push_str(" args=");
                                    body.push_str(&truncate(p, params.max_payload_chars));
                                }
                            }
                        }
                        timeline.push(TimelineRow {
                            lifecycle: unit_ev.lifecycle_label().to_string(),
                            kind: "tool".into(),
                            state: tool_state_label(t.request_state, snap.unit_state),
                            label: name.to_string(),
                            correlation: format!(
                                "c:{} i:{} action:{} g:{}",
                                short(snap.connection_id.as_str()),
                                short(snap.interpretation_id.as_str()),
                                t.tool_action_id.as_str(),
                                snap.unit_generation
                            ),
                            body,
                            css_class: "ev-tool".into(),
                        });
                    }
                    CanonicalUnit::Structure(st) => {
                        timeline.push(TimelineRow {
                            lifecycle: unit_ev.lifecycle_label().to_string(),
                            kind: format!("structure/{:?}", st.kind),
                            state: format!("{:?}", snap.unit_state),
                            label: structure_label(st.kind).into(),
                            correlation: format!(
                                "u:{} g:{}",
                                short(snap.unit_id.as_str()),
                                snap.unit_generation
                            ),
                            body: st.content.clone(),
                            css_class: "ev-structure".into(),
                        });
                    }
                    CanonicalUnit::Boundary(b) => {
                        timeline.push(TimelineRow {
                            lifecycle: unit_ev.lifecycle_label().to_string(),
                            kind: "boundary".into(),
                            state: "complete".into(),
                            label: boundary_label(b.kind).into(),
                            correlation: format!("u:{}", short(snap.unit_id.as_str())),
                            body: String::new(),
                            css_class: "ev-boundary".into(),
                        });
                    }
                    CanonicalUnit::Diagnostic(d) => {
                        timeline.push(TimelineRow {
                            lifecycle: unit_ev.lifecycle_label().to_string(),
                            kind: "diagnostic".into(),
                            state: format!("{:?}", d.kind),
                            label: "diag".into(),
                            correlation: format!("u:{}", short(snap.unit_id.as_str())),
                            body: d.message.clone(),
                            css_class: "ev-diag".into(),
                        });
                    }
                    CanonicalUnit::Paragraph(p) => {
                        timeline.push(TimelineRow {
                            lifecycle: unit_ev.lifecycle_label().to_string(),
                            kind: format!("paragraph/{:?}", p.kind),
                            state: "complete".into(),
                            label: "¶".into(),
                            correlation: format!("u:{}", short(snap.unit_id.as_str())),
                            body: String::new(),
                            css_class: "ev-para".into(),
                        });
                    }
                    CanonicalUnit::Usage(u) => {
                        timeline.push(TimelineRow {
                            lifecycle: unit_ev.lifecycle_label().to_string(),
                            kind: "usage".into(),
                            state: "complete".into(),
                            label: "tokens".into(),
                            correlation: String::new(),
                            body: format!("{u:?}"),
                            css_class: "ev-usage".into(),
                        });
                    }
                }
            }
            InterpreterOutputEvent::Ended(e) => {
                end = Some(e);
                timeline.push(TimelineRow {
                    lifecycle: "ended".into(),
                    kind: "interpretation".into(),
                    state: format!("{:?}", e.kind),
                    label: "end".into(),
                    correlation: format!(
                        "c:{} i:{}",
                        short(e.connection_id.as_str()),
                        short(e.interpretation_id.as_str())
                    ),
                    body: format!(
                        "events={} sentences={} unresolved_bytes={}",
                        e.canonical_event_count,
                        e.completed_sentence_count,
                        e.unresolved_text_bytes
                    ),
                    css_class: "ev-end".into(),
                });
            }
        }
    }

    let assembled_markdown = join_sentences_as_markdown(&public_sentences);
    let document_html = markdown_to_html(&assembled_markdown);

    let reasoning_md = if params.include_reasoning && !reasoning_sentences.is_empty() {
        join_sentences_as_markdown(&reasoning_sentences)
    } else {
        String::new()
    };
    let reasoning_html = if reasoning_md.is_empty() {
        String::new()
    } else {
        markdown_to_html(&reasoning_md)
    };

    let sentence_count = public_sentences.len();
    let timeline_rows = timeline.len();
    let full_page_html = render_full_page(
        params,
        &assembled_markdown,
        &document_html,
        &reasoning_html,
        &timeline,
        end,
    );

    HtmlReport {
        assembled_markdown,
        document_html,
        full_page_html,
        sentence_count,
        timeline_rows,
    }
}

/// Write the full HTML page to `path` (parent dirs created as needed).
pub fn write_html_report(path: impl AsRef<Path>, report: &HtmlReport) -> std::io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    std::fs::write(path, report.full_page_html.as_bytes())
}

/// Join complete sentences into a single Markdown document body.
///
/// Sentences are already canonical; we only serialise them for visual review.
/// Consecutive sentences are separated by a blank line so Markdown treats them
/// as paragraphs (readable HTML after conversion).
pub fn join_sentences_as_markdown(sentences: &[String]) -> String {
    sentences
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Convert Markdown to HTML (pulldown-cmark). Safe for untrusted text content.
pub fn markdown_to_html(md: &str) -> String {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TASKLISTS);
    let parser = Parser::new_ext(md, options);
    let mut out = String::new();
    html::push_html(&mut out, parser);
    out
}

#[derive(Clone, Debug)]
struct TimelineRow {
    lifecycle: String,
    kind: String,
    state: String,
    label: String,
    correlation: String,
    body: String,
    css_class: String,
}

fn render_full_page(
    params: &HtmlReportParams,
    assembled_md: &str,
    document_html: &str,
    reasoning_html: &str,
    timeline: &[TimelineRow],
    end: Option<&InterpretationEnd>,
) -> String {
    let mut body = String::new();
    body.push_str(&format!("<h1>{}</h1>\n", escape_html(&params.title)));
    body.push_str(
        "<p class=\"meta\">Built from <strong>canonical Interpreter events only</strong> \
         — not a re-parse of raw Grok wire bytes. Use this to verify sentence assembly \
         and tool lifecycle serialisation.</p>\n",
    );

    if let Some(e) = end {
        body.push_str(&format!(
            "<p class=\"meta end\">Interpretation end: <code>{:?}</code> · \
             canonical events: {} · sentences: {} · unresolved text bytes: {}</p>\n",
            e.kind, e.canonical_event_count, e.completed_sentence_count, e.unresolved_text_bytes
        ));
    }

    body.push_str("<section id=\"document\">\n");
    body.push_str("<h2>Assembled document</h2>\n");
    body.push_str(
        "<p class=\"meta\">Public response sentences → Markdown (blank-line separated) → HTML.</p>\n",
    );
    if document_html.trim().is_empty() {
        body.push_str("<p class=\"empty\"><em>(no complete public_response sentences)</em></p>\n");
    } else {
        body.push_str("<div class=\"prose\">\n");
        body.push_str(document_html);
        body.push_str("</div>\n");
    }
    body.push_str("<details><summary>Source Markdown (assembled)</summary>\n");
    body.push_str("<pre class=\"md-source\">");
    body.push_str(&escape_html(assembled_md));
    body.push_str("</pre></details>\n");
    body.push_str("</section>\n");

    if !reasoning_html.is_empty() {
        body.push_str("<section id=\"reasoning\">\n");
        body.push_str("<h2>Reasoning summary</h2>\n");
        body.push_str("<div class=\"prose reasoning\">\n");
        body.push_str(reasoning_html);
        body.push_str("</div></section>\n");
    }

    if params.include_timeline {
        body.push_str("<section id=\"timeline\">\n");
        body.push_str("<h2>Canonical event timeline</h2>\n");
        body.push_str(
            "<p class=\"meta\">Every unit generation as emitted by the Interpreter \
             (append-only order).</p>\n",
        );
        body.push_str("<ol class=\"timeline\">\n");
        for row in timeline {
            body.push_str(&format!(
                "<li class=\"{}\"><div class=\"hdr\"><span class=\"kind\">{}</span> \
                 <span class=\"state\">{}/{}</span> <span class=\"label\">{}</span> \
                 <span class=\"corr\">{}</span></div>",
                escape_html(&row.css_class),
                escape_html(&row.kind),
                escape_html(&row.lifecycle),
                escape_html(&row.state),
                escape_html(&row.label),
                escape_html(&row.correlation),
            ));
            if !row.body.is_empty() {
                body.push_str(&format!(
                    "<pre class=\"body\">{}</pre>",
                    escape_html(&row.body)
                ));
            }
            body.push_str("</li>\n");
        }
        body.push_str("</ol></section>\n");
    }

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{title}</title>
<style>
{css}
</style>
</head>
<body>
{body}
</body>
</html>
"#,
        title = escape_html(&params.title),
        css = PAGE_CSS,
        body = body,
    )
}

const PAGE_CSS: &str = r#"
:root {
  --bg: #0f1419;
  --panel: #1a2332;
  --text: #e7ecf3;
  --muted: #8b9bb4;
  --accent: #5b9fd4;
  --tool: #c9a227;
  --diag: #d46b6b;
  --border: #2a3548;
  --code: #0d1117;
}
* { box-sizing: border-box; }
body {
  margin: 0 auto;
  max-width: 920px;
  padding: 1.5rem 1.25rem 3rem;
  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.55;
}
h1 { font-size: 1.45rem; margin: 0 0 0.5rem; }
h2 { font-size: 1.15rem; margin: 1.75rem 0 0.75rem; border-bottom: 1px solid var(--border); padding-bottom: 0.35rem; }
.meta { color: var(--muted); font-size: 0.92rem; }
.meta.end { background: var(--panel); padding: 0.6rem 0.8rem; border-radius: 6px; border: 1px solid var(--border); }
.prose {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1rem 1.15rem;
}
.prose p { margin: 0 0 0.85rem; }
.prose p:last-child { margin-bottom: 0; }
.prose code, .prose pre {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.9em;
}
.prose pre {
  background: var(--code);
  padding: 0.75rem;
  border-radius: 6px;
  overflow-x: auto;
}
.prose.reasoning { border-left: 3px solid var(--accent); }
.empty { color: var(--muted); }
details { margin-top: 0.75rem; color: var(--muted); }
.md-source {
  background: var(--code);
  padding: 0.75rem;
  border-radius: 6px;
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-word;
  font-size: 0.85rem;
}
.timeline { list-style: none; padding: 0; margin: 0; }
.timeline li {
  background: var(--panel);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 0.65rem 0.8rem;
  margin: 0 0 0.55rem;
}
.timeline .hdr { font-size: 0.85rem; display: flex; flex-wrap: wrap; gap: 0.4rem 0.75rem; align-items: baseline; }
.timeline .kind { color: var(--accent); font-weight: 600; }
.timeline .state { color: var(--muted); }
.timeline .label { font-weight: 600; }
.timeline .corr { color: var(--muted); font-family: ui-monospace, monospace; font-size: 0.8rem; }
.timeline .body {
  margin: 0.45rem 0 0;
  padding: 0.5rem 0.6rem;
  background: var(--code);
  border-radius: 4px;
  font-size: 0.85rem;
  white-space: pre-wrap;
  word-break: break-word;
  overflow-x: auto;
}
.ev-tool .kind, .ev-tool .label { color: var(--tool); }
.ev-diag .kind { color: var(--diag); }
.ev-end { border-color: var(--accent); }
"#;

fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

fn short(id: &str) -> &str {
    if id.len() <= 10 {
        id
    } else {
        &id[..10]
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let t: String = s.chars().take(max).collect();
        format!("{t}…")
    }
}

fn tool_state_label(req: ToolRequestState, unit: UnitState) -> String {
    let base = match req {
        ToolRequestState::Ready => "ready",
        ToolRequestState::Assembling => "waiting",
        ToolRequestState::Incomplete => "incomplete",
        ToolRequestState::Malformed => "malformed",
    };
    if unit == UnitState::Complete {
        format!("{base}/complete")
    } else {
        base.to_string()
    }
}

fn structure_label(k: StructureKind) -> &'static str {
    match k {
        StructureKind::Heading => "heading",
        StructureKind::ListItem => "list_item",
        StructureKind::CodeBlock => "code",
        StructureKind::TableRow => "table_row",
        StructureKind::BlockQuote => "quote",
        StructureKind::ThematicBreak => "hr",
        StructureKind::RawBlock => "raw",
    }
}

fn boundary_label(k: BoundaryKind) -> &'static str {
    match k {
        BoundaryKind::ResponseStarted => "response_started",
        BoundaryKind::ChannelStarted => "channel_started",
        BoundaryKind::ChannelFinished => "channel_finished",
        BoundaryKind::ResponseFinished => "response_finished",
        BoundaryKind::UsageFinalized => "usage_finalized",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use monoloop_contracts::{
        CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, FlowId,
        InterpretationEndKind, InterpretationId, LaneId, TextSentence, UnitId,
    };

    fn text_ev(content: &str, n: u64) -> InterpreterOutputEvent {
        InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
            unit_id: UnitId::new(format!("s{n}")),
            unit_generation: 1,
            unit_state: UnitState::Complete,
            interpretation_id: InterpretationId::new("i1"),
            connection_id: ConnectionId::new("c1"),
            external_session_id: None,
            flow_id: FlowId::main(),
            lane_id: LaneId::response(),
            lane_ordinal: n,
            causal_parent_id: None,
            unit: CanonicalUnit::Text(TextSentence {
                sentence_id: UnitId::new(format!("s{n}")),
                channel: TextChannel::PublicResponse,
                paragraph_id: None,
                sentence_ordinal: n,
                content: content.into(),
            }),
        }))
    }

    #[test]
    fn markdown_to_html_basic() {
        let html = markdown_to_html("Hello **world**.");
        assert!(html.contains("<strong>world</strong>") || html.contains("<p>"));
    }

    #[test]
    fn report_assembles_sentences_and_timeline() {
        let events = vec![
            text_ev("Hello **world**.", 1),
            text_ev("Second sentence!", 2),
            InterpreterOutputEvent::Ended(InterpretationEnd {
                interpretation_id: InterpretationId::new("i1"),
                connection_id: ConnectionId::new("c1"),
                external_session_id: None,
                kind: InterpretationEndKind::Complete,
                canonical_event_count: 2,
                completed_sentence_count: 2,
                completed_structure_count: 0,
                unresolved_text_bytes: 0,
                source_bytes_consumed: 40,
                safe_diagnostics: vec![],
            }),
        ];
        let report = build_html_report(&events, &HtmlReportParams::default());
        assert_eq!(report.sentence_count, 2);
        assert!(report.assembled_markdown.contains("Hello **world**."));
        assert!(report.assembled_markdown.contains("Second sentence!"));
        assert!(report.document_html.contains("Hello") || report.document_html.contains("world"));
        assert!(report.full_page_html.contains("Canonical event timeline"));
        assert!(report.full_page_html.contains("Assembled document"));
        assert!(report.timeline_rows >= 3);
    }
}
