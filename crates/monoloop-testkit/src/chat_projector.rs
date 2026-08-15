//! Human-digestible **chat projection** of canonical Interpreter events.
//!
//! **Test kit only.** This is a *report*, not ground truth.
//!
//! ## Design (scalable / universal)
//!
//! **Always correct core — chronological chat**
//!
//! Walk complete units in Interpreter emit order, map channels to chat roles,
//! collapse consecutive same-role text. No invented speech, no tool↔sentence
//! claims. Works for every mix of agent / thinking / tool / status.
//!
//! **Optional structural zip (gated)**
//!
//! Only when *all* of these hold:
//! 1. Every tool first-sight precedes every public-response sentence (tools-first dump)
//! 2. Public text contains numbered list items (`1. …`, `2. …`)
//! 3. Count of those list items **equals** the tool count
//!
//! Then: preamble → thinking/status → ordinal `(tool[i], step[i])` → epilogue.
//! Pairing is **index-only** — no CREATE/READ/keyword matching.
//!
//! If any gate fails → chronological. Prefer honesty over a pretty but wrong weave.
//!
//! ```text
//! AGENT:    I'll take care of that.
//! THINKING: … should read the file first
//! TOOL:     Read path/to/file.txt
//! AGENT:    Aha — let me update it.
//! TOOL:     Write path/to/file.txt
//! AGENT:    Done; file is ready.
//! ```

use monoloop_contracts::{
    CanonicalUnit, InterpreterOutputEvent, TextChannel, ToolRequestState, UnitState,
};
use std::collections::HashMap;

/// Strategy used to lay out the projected chat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionStrategy {
    /// Emit-order layout with chat chrome. Universal default.
    ChronologicalChat,
    /// Tools-first dump + equal numbered steps → ordinal tool/step zip only.
    StructuralOrdinalZip,
}

/// Confidence of the projection relative to Interpreter emit order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionConfidence {
    /// Layout follows emit order (no reordering of tools vs text).
    EmitOrder,
    /// Tools/steps reordered under structural gates; still no invented content.
    StructuralReorder,
}

/// Options for [`project_chat_with`].
#[derive(Clone, Debug)]
pub struct ProjectChatOptions {
    /// When true (default), allow [`ProjectionStrategy::StructuralOrdinalZip`]
    /// if structural gates pass. When false, always chronological.
    pub allow_structural_zip: bool,
}

impl Default for ProjectChatOptions {
    fn default() -> Self {
        Self {
            allow_structural_zip: true,
        }
    }
}

/// Role of a projected chat line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChatRole {
    /// Public assistant speech.
    Agent,
    /// Public reasoning summary (never private CoT).
    Thinking,
    /// Tool action card.
    Tool,
    /// Status / narration channel.
    Status,
}

/// Compact tool snapshot for projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectedTool {
    /// Dialect tool action id.
    pub action_id: String,
    /// Display title (often path-aware from the dialect).
    pub title: String,
    /// Short display verb (`Write`, `Read`, first token, …) — cosmetic only.
    pub verb: String,
    /// Request args JSON when ready (bounded later by the HTML layer).
    pub args: Option<String>,
    /// Terminal outcome label when known (`Success`, …).
    pub terminal: Option<String>,
    /// Request/unit state label.
    pub state: String,
}

/// One line in the projected chat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatLine {
    /// Speaker / surface.
    pub role: ChatRole,
    /// Markdown (or plain) body for agent/thinking/status lines.
    pub text: String,
    /// Tool payload when `role == Tool`.
    pub tool: Option<ProjectedTool>,
    /// True when this line was placed by structural zip (not pure emit order).
    pub reordered: bool,
}

/// Full chat projection for a run.
#[derive(Clone, Debug)]
pub struct ChatProjection {
    /// Layout strategy chosen for this run.
    pub strategy: ProjectionStrategy,
    /// Confidence relative to emit order.
    pub confidence: ProjectionConfidence,
    /// Why this strategy was chosen (short, for UI / tests).
    pub strategy_reason: String,
    /// Projected lines in display order.
    pub lines: Vec<ChatLine>,
    /// Plain-text chat transcript for terminals / logs.
    pub plain_text: String,
    /// Self-contained HTML fragment (section body only).
    pub html: String,
    /// Human-facing disclaimer (always present).
    pub disclaimer: &'static str,
}

const DISCLAIMER: &str = "Chat projection is a human-readable report, not ground truth. \
Default layout follows Interpreter emit order. Structural ordinal zip (when shown) \
only reorders tools against later numbered list steps when counts match — it does \
not invent speech. Use the event-order interleaved stream and canonical timeline \
for exact emit order.";

/// Project canonical Interpreter events into a chat-like flow (default options).
pub fn project_chat(events: &[InterpreterOutputEvent]) -> ChatProjection {
    project_chat_with(events, &ProjectChatOptions::default())
}

/// Project with explicit options.
pub fn project_chat_with(
    events: &[InterpreterOutputEvent],
    opts: &ProjectChatOptions,
) -> ChatProjection {
    let extracted = extract(events);
    let (strategy, reason) = choose_strategy(&extracted, opts);
    let confidence = match strategy {
        ProjectionStrategy::ChronologicalChat => ProjectionConfidence::EmitOrder,
        ProjectionStrategy::StructuralOrdinalZip => ProjectionConfidence::StructuralReorder,
    };
    let lines = match strategy {
        ProjectionStrategy::StructuralOrdinalZip => assemble_structural_zip(&extracted),
        ProjectionStrategy::ChronologicalChat => assemble_chronological(&extracted),
    };
    let lines = merge_consecutive_same_role(&lines);
    let plain_text = render_plain(&lines, strategy, confidence, &reason);
    let html = render_html(&lines, strategy, confidence, &reason);
    ChatProjection {
        strategy,
        confidence,
        strategy_reason: reason,
        lines,
        plain_text,
        html,
        disclaimer: DISCLAIMER,
    }
}

// ── extraction ──────────────────────────────────────────────────────────────

struct Extracted {
    /// Event-order blocks (text or first-sight tool).
    chrono: Vec<ChronoBlock>,
    /// Tools in first-sight order, upgraded to latest generation.
    tools: Vec<ProjectedTool>,
    /// Public response sentences in emit order.
    public: Vec<String>,
    /// Reasoning-summary sentences (also present in chrono).
    reasoning: Vec<String>,
    /// Status narration sentences (also present in chrono).
    status: Vec<String>,
    /// True if every tool first-sight precedes every public sentence.
    tools_before_public: bool,
}

enum ChronoBlock {
    Text {
        channel: TextChannel,
        content: String,
    },
    Tool(ProjectedTool),
}

fn extract(events: &[InterpreterOutputEvent]) -> Extracted {
    let mut chrono: Vec<ChronoBlock> = Vec::new();
    let mut tools: Vec<ProjectedTool> = Vec::new();
    let mut tool_index: HashMap<String, usize> = HashMap::new();
    let mut public: Vec<String> = Vec::new();
    let mut reasoning: Vec<String> = Vec::new();
    let mut status: Vec<String> = Vec::new();
    let mut saw_public = false;
    let mut tool_after_public = false;

    for ev in events {
        let InterpreterOutputEvent::Unit(unit_ev) = ev else {
            continue;
        };
        let snap = unit_ev.snapshot();
        match &snap.unit {
            CanonicalUnit::Text(t) => match t.channel {
                TextChannel::PublicResponse => {
                    saw_public = true;
                    public.push(t.content.clone());
                    chrono.push(ChronoBlock::Text {
                        channel: t.channel,
                        content: t.content.clone(),
                    });
                }
                TextChannel::PublicReasoningSummary => {
                    reasoning.push(t.content.clone());
                    chrono.push(ChronoBlock::Text {
                        channel: t.channel,
                        content: t.content.clone(),
                    });
                }
                TextChannel::StatusNarration => {
                    status.push(t.content.clone());
                    chrono.push(ChronoBlock::Text {
                        channel: t.channel,
                        content: t.content.clone(),
                    });
                }
                TextChannel::QuotedExternalContent => {}
            },
            CanonicalUnit::Tool(t) => {
                if saw_public {
                    tool_after_public = true;
                }
                let projected = projected_tool(t, snap.unit_state);
                let id = projected.action_id.clone();
                if let Some(&idx) = tool_index.get(&id) {
                    tools[idx] = projected.clone();
                    if let Some(ChronoBlock::Tool(slot)) = chrono.iter_mut().find(|b| match b {
                        ChronoBlock::Tool(p) => p.action_id == id,
                        _ => false,
                    }) {
                        *slot = projected;
                    }
                } else {
                    tool_index.insert(id, tools.len());
                    tools.push(projected.clone());
                    chrono.push(ChronoBlock::Tool(projected));
                }
            }
            _ => {}
        }
    }

    let tools_before_public = !tools.is_empty()
        && !public.is_empty()
        && !tool_after_public;

    Extracted {
        chrono,
        tools,
        public,
        reasoning,
        status,
        tools_before_public,
    }
}

fn projected_tool(
    t: &monoloop_contracts::ToolActionEvent,
    unit_state: UnitState,
) -> ProjectedTool {
    let title = t.tool_name.clone().unwrap_or_else(|| "tool".into());
    let verb = display_verb(&title, t.request_payload.as_deref());
    let state = {
        let base = match t.request_state {
            ToolRequestState::Ready => "ready",
            ToolRequestState::Assembling => "waiting",
            ToolRequestState::Incomplete => "incomplete",
            ToolRequestState::Malformed => "malformed",
        };
        if unit_state == UnitState::Complete {
            format!("{base}/complete")
        } else {
            base.to_string()
        }
    };
    ProjectedTool {
        action_id: t.tool_action_id.as_str().to_string(),
        title,
        verb,
        args: t.request_payload.clone(),
        terminal: t.terminal_outcome.map(|o| format!("{o:?}")),
        state,
    }
}

/// Cosmetic label for tool cards only — never used for pairing.
fn display_verb(title: &str, args: Option<&str>) -> String {
    let lower = title.to_ascii_lowercase();
    if lower.starts_with("write") || lower.contains("write `") {
        return "Write".into();
    }
    if lower.starts_with("read") || lower.contains("read `") || lower.contains("read_file") {
        return "Read".into();
    }
    if lower.starts_with("execute") || lower.starts_with("run") || lower.contains("terminal") {
        return "Execute".into();
    }
    if lower.starts_with("delete") || lower.starts_with("remove") {
        return "Delete".into();
    }
    if let Some(a) = args {
        if a.contains("\"command\"") {
            return "Execute".into();
        }
        if a.contains("file_path") && a.contains("content") {
            return "Write".into();
        }
        if a.contains("target_file") {
            return "Read".into();
        }
    }
    title
        .split_whitespace()
        .next()
        .unwrap_or("Tool")
        .to_string()
}

// ── strategy ────────────────────────────────────────────────────────────────

fn choose_strategy(
    ex: &Extracted,
    opts: &ProjectChatOptions,
) -> (ProjectionStrategy, String) {
    if !opts.allow_structural_zip {
        return (
            ProjectionStrategy::ChronologicalChat,
            "structural zip disabled by options".into(),
        );
    }

    if !ex.tools_before_public {
        return (
            ProjectionStrategy::ChronologicalChat,
            "emit-order chat (tools not strictly before public text, or no tools/text)"
                .into(),
        );
    }

    let list_steps = count_list_steps(&ex.public);
    let n_tools = ex.tools.len();

    if list_steps == 0 {
        return (
            ProjectionStrategy::ChronologicalChat,
            "tools-first dump without numbered list steps — chronological (no safe zip)"
                .into(),
        );
    }

    if list_steps != n_tools {
        return (
            ProjectionStrategy::ChronologicalChat,
            format!(
                "tools-first but list steps ({list_steps}) ≠ tools ({n_tools}) — \
                 chronological (refuse mis-pairing)"
            ),
        );
    }

    (
        ProjectionStrategy::StructuralOrdinalZip,
        format!(
            "tools-first + {n_tools} numbered steps matching tool count — \
             ordinal zip only (no keyword pairing)"
        ),
    )
}

fn count_list_steps(public: &[String]) -> usize {
    public.iter().filter(|s| looks_like_list_item(s.trim())).count()
}

// ── assembly ────────────────────────────────────────────────────────────────

fn looks_like_list_item(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    // Require "N." or "N. " form (digit run + period). Content may follow.
    i > 0 && i < b.len() && b[i] == b'.'
}

fn split_public_by_list_items(public: &[String]) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut preamble = Vec::new();
    let mut steps = Vec::new();
    let mut epilogue = Vec::new();
    let mut seen_step = false;

    for s in public {
        if looks_like_list_item(s.trim()) {
            seen_step = true;
            steps.push(s.clone());
        } else if !seen_step {
            preamble.push(s.clone());
        } else {
            epilogue.push(s.clone());
        }
    }
    (preamble, steps, epilogue)
}

fn assemble_structural_zip(ex: &Extracted) -> Vec<ChatLine> {
    let mut lines = Vec::new();
    let (preamble, steps, epilogue) = split_public_by_list_items(&ex.public);

    // Reasoning/status that appeared in the stream: for tools-first dumps they
    // usually sit with tools or after; surface them before the weave as context.
    for s in &ex.reasoning {
        lines.push(line(ChatRole::Thinking, s.clone(), None, true));
    }
    for s in &ex.status {
        lines.push(line(ChatRole::Status, s.clone(), None, true));
    }
    for s in &preamble {
        lines.push(line(ChatRole::Agent, s.clone(), None, true));
    }

    // Gates guarantee steps.len() == tools.len().
    for (tool, step) in ex.tools.iter().zip(steps.iter()) {
        lines.push(line(
            ChatRole::Tool,
            String::new(),
            Some(tool.clone()),
            true,
        ));
        lines.push(line(ChatRole::Agent, step.clone(), None, true));
    }

    for s in &epilogue {
        lines.push(line(ChatRole::Agent, s.clone(), None, true));
    }
    lines
}

fn assemble_chronological(ex: &Extracted) -> Vec<ChatLine> {
    let mut lines = Vec::new();
    let mut pending_role: Option<ChatRole> = None;
    let mut pending_text: Vec<String> = Vec::new();

    let flush = |role: &mut Option<ChatRole>,
                 buf: &mut Vec<String>,
                 lines: &mut Vec<ChatLine>| {
        if let Some(r) = role.take() {
            if !buf.is_empty() {
                lines.push(line(r, join_soft(buf), None, false));
                buf.clear();
            }
        }
    };

    for b in &ex.chrono {
        match b {
            ChronoBlock::Text { channel, content } => {
                let role = match channel {
                    TextChannel::PublicResponse => ChatRole::Agent,
                    TextChannel::PublicReasoningSummary => ChatRole::Thinking,
                    TextChannel::StatusNarration => ChatRole::Status,
                    TextChannel::QuotedExternalContent => continue,
                };
                if pending_role != Some(role) {
                    flush(&mut pending_role, &mut pending_text, &mut lines);
                    pending_role = Some(role);
                }
                pending_text.push(content.clone());
            }
            ChronoBlock::Tool(t) => {
                flush(&mut pending_role, &mut pending_text, &mut lines);
                lines.push(line(
                    ChatRole::Tool,
                    String::new(),
                    Some(t.clone()),
                    false,
                ));
            }
        }
    }
    flush(&mut pending_role, &mut pending_text, &mut lines);
    lines
}

fn line(
    role: ChatRole,
    text: String,
    tool: Option<ProjectedTool>,
    reordered: bool,
) -> ChatLine {
    ChatLine {
        role,
        text,
        tool,
        reordered,
    }
}

fn join_soft(parts: &[String]) -> String {
    let mut out = String::new();
    for s in parts {
        let t = s.trim();
        if t.is_empty() {
            continue;
        }
        if out.is_empty() {
            out.push_str(t);
        } else if looks_like_list_item(t) {
            out.push('\n');
            out.push_str(t);
        } else {
            out.push_str("\n\n");
            out.push_str(t);
        }
    }
    out
}

/// Collapse adjacent same-role non-tool bubbles for readability.
fn merge_consecutive_same_role(lines: &[ChatLine]) -> Vec<ChatLine> {
    let mut out: Vec<ChatLine> = Vec::new();
    for line in lines {
        if line.role != ChatRole::Tool {
            if let Some(prev) = out.last_mut() {
                if prev.role == line.role && prev.tool.is_none() && line.tool.is_none() {
                    if !prev.text.is_empty() && !line.text.is_empty() {
                        if looks_like_list_item(line.text.trim()) {
                            prev.text.push('\n');
                        } else {
                            prev.text.push_str("\n\n");
                        }
                    }
                    prev.text.push_str(&line.text);
                    prev.reordered = prev.reordered || line.reordered;
                    continue;
                }
            }
        }
        out.push(line.clone());
    }
    out
}

// ── render ──────────────────────────────────────────────────────────────────

fn render_plain(
    lines: &[ChatLine],
    strategy: ProjectionStrategy,
    confidence: ProjectionConfidence,
    reason: &str,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "=== CHAT PROJECTION ({strategy:?} / {confidence:?}) — not ground truth ===\n"
    ));
    out.push_str(DISCLAIMER);
    out.push_str("\n");
    out.push_str(&format!("reason: {reason}\n\n"));
    for line in lines {
        match (&line.role, &line.tool) {
            (ChatRole::Agent, _) => {
                out.push_str("AGENT:\n");
                out.push_str(&line.text);
                out.push_str("\n\n");
            }
            (ChatRole::Thinking, _) => {
                out.push_str("THINKING:\n… ");
                out.push_str(&line.text);
                out.push_str("\n\n");
            }
            (ChatRole::Status, _) => {
                out.push_str("STATUS:\n");
                out.push_str(&line.text);
                out.push_str("\n\n");
            }
            (ChatRole::Tool, Some(t)) => {
                out.push_str(&format!("TOOL: {} — {}\n", t.verb, t.title));
                if let Some(term) = &t.terminal {
                    out.push_str(&format!("      → {term}\n"));
                }
                out.push('\n');
            }
            (ChatRole::Tool, None) => {}
        }
    }
    out
}

fn render_html(
    lines: &[ChatLine],
    strategy: ProjectionStrategy,
    confidence: ProjectionConfidence,
    reason: &str,
) -> String {
    let conf_class = match confidence {
        ProjectionConfidence::EmitOrder => "conf-emit",
        ProjectionConfidence::StructuralReorder => "conf-structural",
    };
    let mut out = String::new();
    out.push_str(&format!(
        "<div class=\"chat-projection {conf_class}\" data-strategy=\"{strategy:?}\" \
         data-confidence=\"{confidence:?}\">\n"
    ));
    out.push_str("<div class=\"chat-disclaimer\">");
    out.push_str(&escape(DISCLAIMER));
    out.push_str("</div>\n");
    out.push_str(&format!(
        "<p class=\"chat-strategy\">Strategy: <code>{strategy:?}</code> · \
         Confidence: <code>{confidence:?}</code><br/>\
         <span class=\"chat-reason\">{}</span></p>\n",
        escape(reason)
    ));
    out.push_str("<div class=\"chat-flow\">\n");

    for line in lines {
        match (&line.role, &line.tool) {
            (ChatRole::Agent, _) => {
                out.push_str(&agent_bubble(&line.text, line.reordered));
            }
            (ChatRole::Thinking, _) => {
                out.push_str("<div class=\"chat-line thinking");
                if line.reordered {
                    out.push_str(" reordered");
                }
                out.push_str("\"><div class=\"chat-role\">Thinking</div>");
                out.push_str("<div class=\"chat-body thinking-body\">… ");
                out.push_str(&md_html(&line.text));
                out.push_str("</div></div>\n");
            }
            (ChatRole::Status, _) => {
                out.push_str("<div class=\"chat-line status");
                if line.reordered {
                    out.push_str(" reordered");
                }
                out.push_str("\"><div class=\"chat-role\">Status</div>");
                out.push_str("<div class=\"chat-body\">");
                out.push_str(&md_html(&line.text));
                out.push_str("</div></div>\n");
            }
            (ChatRole::Tool, Some(t)) => {
                out.push_str("<div class=\"chat-line tool");
                if line.reordered {
                    out.push_str(" reordered");
                }
                out.push_str("\">");
                out.push_str("<div class=\"chat-role\">Tool</div>");
                out.push_str("<div class=\"chat-tool-card\">");
                out.push_str(&format!(
                    "<span class=\"chat-tool-verb\">{}</span> \
                     <span class=\"chat-tool-title\">{}</span>",
                    escape(&t.verb),
                    escape(&t.title)
                ));
                if let Some(term) = &t.terminal {
                    out.push_str(&format!(
                        " <span class=\"chat-tool-term\">→ {}</span>",
                        escape(term)
                    ));
                }
                out.push_str(&format!(
                    " <span class=\"chat-tool-state\">{}</span>",
                    escape(&t.state)
                ));
                if let Some(a) = &t.args {
                    let clipped = if a.chars().count() > 400 {
                        format!("{}…", a.chars().take(400).collect::<String>())
                    } else {
                        a.clone()
                    };
                    out.push_str(&format!(
                        "<pre class=\"chat-tool-args\">{}</pre>",
                        escape(&clipped)
                    ));
                }
                out.push_str("</div></div>\n");
            }
            (ChatRole::Tool, None) => {}
        }
    }

    out.push_str("</div></div>\n");
    out
}

fn agent_bubble(text: &str, reordered: bool) -> String {
    let mut out = String::new();
    out.push_str("<div class=\"chat-line agent");
    if reordered {
        out.push_str(" reordered");
    }
    out.push_str("\"><div class=\"chat-role\">Agent</div>");
    out.push_str("<div class=\"chat-body\">");
    out.push_str(&md_html(text));
    out.push_str("</div></div>\n");
    out
}

fn md_html(md: &str) -> String {
    use pulldown_cmark::{html, Options, Parser};
    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    let parser = Parser::new_ext(md, options);
    let mut out = String::new();
    html::push_html(&mut out, parser);
    out
}

fn escape(s: &str) -> String {
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

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use monoloop_contracts::{
        CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, FlowId,
        InterpretationId, LaneId, TextSentence, ToolActionEvent, ToolActionId, ToolExecutionState,
        ToolRequestState, ToolResultState, ToolTerminalOutcome, UnitId, UnitState,
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

    fn tool_ev(id: &str, name: &str, n: u64) -> InterpreterOutputEvent {
        InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
            unit_id: UnitId::new(format!("t{n}")),
            unit_generation: 1,
            unit_state: UnitState::Complete,
            interpretation_id: InterpretationId::new("i1"),
            connection_id: ConnectionId::new("c1"),
            external_session_id: None,
            flow_id: FlowId::main(),
            lane_id: LaneId::response(),
            lane_ordinal: n,
            causal_parent_id: None,
            unit: CanonicalUnit::Tool(ToolActionEvent {
                tool_action_id: ToolActionId::new(id),
                tool_name: Some(name.into()),
                request_state: ToolRequestState::Ready,
                execution_state: ToolExecutionState::Terminal,
                result_state: ToolResultState::Complete,
                request_payload: Some("{}".into()),
                result_payload: None,
                terminal_outcome: Some(ToolTerminalOutcome::Success),
                waiting_for: None,
            }),
        }))
    }

    #[test]
    fn tools_first_equal_list_steps_structural_zip() {
        let events = vec![
            tool_ev("a1", "Write `file.txt`", 1),
            tool_ev("a2", "Read `file.txt`", 2),
            tool_ev("a3", "Execute `rm file.txt`", 3),
            text_ev("I'll run the steps.", 4),
            text_ev("1. **CREATE** — Wrote the file.", 5),
            text_ev("2. **READ** — File contained x.", 6),
            text_ev("3. **DELETE** — Removed the file.", 7),
            text_ev("No other files were touched.", 8),
        ];
        let p = project_chat(&events);
        assert_eq!(p.strategy, ProjectionStrategy::StructuralOrdinalZip);
        assert_eq!(p.confidence, ProjectionConfidence::StructuralReorder);
        assert!(p.strategy_reason.contains("ordinal zip"));
        // Preamble, then tool/step pairs.
        assert_eq!(p.lines[0].role, ChatRole::Agent);
        assert!(p.lines[0].text.contains("I'll run"));
        let roles: Vec<ChatRole> = p.lines.iter().map(|l| l.role).collect();
        assert!(
            roles.windows(2).any(|w| w == [ChatRole::Tool, ChatRole::Agent]),
            "expected tool then step: {roles:?}"
        );
        // Ordinal: first tool before first step text.
        let plain = &p.plain_text;
        assert!(plain.find("Write").unwrap() < plain.find("CREATE").unwrap());
        assert!(plain.contains("No other files were touched"));
    }

    #[test]
    fn tools_first_mismatched_counts_stays_chronological() {
        let events = vec![
            tool_ev("a1", "Write `file.txt`", 1),
            tool_ev("a2", "Read `file.txt`", 2),
            text_ev("Only one step listed:", 3),
            text_ev("1. Did something.", 4),
        ];
        let p = project_chat(&events);
        assert_eq!(p.strategy, ProjectionStrategy::ChronologicalChat);
        assert_eq!(p.confidence, ProjectionConfidence::EmitOrder);
        assert!(p.strategy_reason.contains("≠") || p.strategy_reason.contains("refuse"));
        // All tools before text in emit order.
        let roles: Vec<_> = p.lines.iter().map(|l| l.role).collect();
        assert_eq!(
            roles,
            vec![ChatRole::Tool, ChatRole::Tool, ChatRole::Agent]
        );
        assert!(p.lines.iter().all(|l| !l.reordered));
    }

    #[test]
    fn tools_first_free_prose_stays_chronological() {
        let events = vec![
            tool_ev("a1", "search", 1),
            tool_ev("a2", "Write `x`", 2),
            text_ev("I searched and then wrote the file. Looks good.", 3),
        ];
        let p = project_chat(&events);
        assert_eq!(p.strategy, ProjectionStrategy::ChronologicalChat);
        assert!(p.strategy_reason.contains("without numbered list"));
    }

    #[test]
    fn chronological_when_text_between_tools() {
        let events = vec![
            text_ev("Let me start.", 1),
            tool_ev("a1", "Read `a.txt`", 2),
            text_ev("Looks good, writing next.", 3),
            tool_ev("a2", "Write `a.txt`", 4),
            text_ev("Done.", 5),
        ];
        let p = project_chat(&events);
        assert_eq!(p.strategy, ProjectionStrategy::ChronologicalChat);
        assert_eq!(p.confidence, ProjectionConfidence::EmitOrder);
        let roles: Vec<_> = p.lines.iter().map(|l| l.role).collect();
        assert_eq!(
            roles,
            vec![
                ChatRole::Agent,
                ChatRole::Tool,
                ChatRole::Agent,
                ChatRole::Tool,
                ChatRole::Agent,
            ]
        );
        assert!(p.lines.iter().all(|l| !l.reordered));
    }

    #[test]
    fn force_chronological_option() {
        let events = vec![
            tool_ev("a1", "Write `f`", 1),
            text_ev("1. Wrote it.", 2),
        ];
        let p = project_chat_with(
            &events,
            &ProjectChatOptions {
                allow_structural_zip: false,
            },
        );
        assert_eq!(p.strategy, ProjectionStrategy::ChronologicalChat);
        assert!(p.strategy_reason.contains("disabled"));
    }

    #[test]
    fn text_only_is_one_agent_bubble() {
        let p = project_chat(&[
            text_ev("Hello.", 1),
            text_ev("World.", 2),
        ]);
        assert_eq!(p.strategy, ProjectionStrategy::ChronologicalChat);
        assert_eq!(p.lines.len(), 1);
        assert_eq!(p.lines[0].role, ChatRole::Agent);
        assert!(p.lines[0].text.contains("Hello"));
        assert!(p.lines[0].text.contains("World"));
    }

    #[test]
    fn disclaimer_always_present() {
        let p = project_chat(&[text_ev("Hello.", 1)]);
        assert!(p.disclaimer.contains("not ground truth"));
        assert!(p.plain_text.contains("not ground truth"));
        assert!(p.html.contains("chat-disclaimer"));
        assert!(p.html.contains("EmitOrder") || p.html.contains("conf-emit"));
    }
}
