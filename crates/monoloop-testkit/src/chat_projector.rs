//! Human-digestible **chat projection** of canonical Interpreter events.
//!
//! **Test kit only.** This is a *report*, not ground truth.
//!
//! The Interpreter event order remains authoritative (tools-first dumps, bare
//! generations, incomplete assemblies). This projector rearranges complete
//! units into a chat-like flow humans prefer:
//!
//! ```text
//! AGENT:    I'll take care of that.
//! THINKING: … should read the file first
//! TOOL:     Read path/to/file.txt
//! AGENT:    Aha — let me update it.
//! TOOL:     Write path/to/file.txt
//! AGENT:    Done; file is ready.
//! ```
//!
//! When the dialect emitted all tools before any public text (common with
//! Grok Build), the projector pairs tools with later step sentences by ordinal
//! so the narrative reads naturally. That pairing is **heuristic** and may not
//! match wall-clock order.

use monoloop_contracts::{
    CanonicalUnit, InterpreterOutputEvent, TextChannel, ToolRequestState, UnitState,
};
use std::collections::HashMap;

/// Strategy used to lay out the projected chat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionStrategy {
    /// Walk emit order; only add chat chrome (no reordering).
    ChronologicalChat,
    /// Tools completed before public text: pair tools with later step sentences.
    NarrativeReassembly,
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
    /// Short verb hint (`Write`, `Read`, `Execute`, …).
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
    /// True when this line was placed by narrative reassembly (not pure emit order).
    pub reordered: bool,
}

/// Full chat projection for a run.
#[derive(Clone, Debug)]
pub struct ChatProjection {
    /// Layout strategy chosen for this run.
    pub strategy: ProjectionStrategy,
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
It may reorder tools relative to later summary text. Use the event-order interleaved \
stream and canonical timeline for exact Interpreter emit order.";

/// Project canonical Interpreter events into a chat-like flow.
pub fn project_chat(events: &[InterpreterOutputEvent]) -> ChatProjection {
    let extracted = extract(events);
    let strategy = choose_strategy(&extracted);
    let lines = match strategy {
        ProjectionStrategy::NarrativeReassembly => assemble_narrative(&extracted),
        ProjectionStrategy::ChronologicalChat => assemble_chronological(&extracted),
    };
    let lines = merge_consecutive_agent(&lines);
    let plain_text = render_plain(&lines, strategy);
    let html = render_html(&lines, strategy);
    ChatProjection {
        strategy,
        lines,
        plain_text,
        html,
        disclaimer: DISCLAIMER,
    }
}

// ── extraction ──────────────────────────────────────────────────────────────

struct Extracted {
    /// Event-order blocks (text or first-sight tool), used for chronological mode.
    chrono: Vec<ChronoBlock>,
    /// Tools in first-sight order, upgraded to latest terminal generation.
    tools: Vec<ProjectedTool>,
    /// Public response sentences in emit order.
    public: Vec<String>,
    /// Reasoning-summary sentences.
    reasoning: Vec<String>,
    /// Status narration sentences.
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
                    // Prefer terminal / later generation for the card contents.
                    tools[idx] = projected.clone();
                    // Update chrono slot if present.
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
        && !tool_after_public
        && chrono.iter().any(|b| matches!(b, ChronoBlock::Tool(_)));

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
    let title = t
        .tool_name
        .clone()
        .unwrap_or_else(|| "tool".into());
    let verb = infer_verb(&title, t.request_payload.as_deref());
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

fn infer_verb(title: &str, args: Option<&str>) -> String {
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
    // First token of title as fallback.
    title
        .split_whitespace()
        .next()
        .unwrap_or("Tool")
        .to_string()
}

fn choose_strategy(ex: &Extracted) -> ProjectionStrategy {
    // Narrative reassembly when tools finished first and we have step-like
    // summary text to weave with them — the classic "tools then dump" shape.
    let has_steps = ex.public.iter().any(|s| is_step_sentence(s));
    if ex.tools_before_public && has_steps && !ex.tools.is_empty() {
        ProjectionStrategy::NarrativeReassembly
    } else {
        ProjectionStrategy::ChronologicalChat
    }
}

// ── assembly ────────────────────────────────────────────────────────────────

fn is_step_sentence(s: &str) -> bool {
    let t = s.trim();
    if looks_like_list_item(t) {
        return true;
    }
    // Bold step labels without a leading number (legacy bad splits).
    const LABELS: &[&str] = &[
        "**CREATE**",
        "**READ**",
        "**UPDATE**",
        "**DELETE**",
        "**WRITE**",
    ];
    LABELS.iter().any(|l| t.starts_with(l) || t.contains(&format!(" {l}")))
}

fn looks_like_list_item(s: &str) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    i > 0 && i < b.len() && b[i] == b'.'
}

fn split_public(public: &[String]) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut preamble = Vec::new();
    let mut steps = Vec::new();
    let mut epilogue = Vec::new();
    let mut seen_step = false;

    for s in public {
        if is_step_sentence(s) {
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

fn assemble_narrative(ex: &Extracted) -> Vec<ChatLine> {
    let mut lines = Vec::new();
    let (preamble, steps, epilogue) = split_public(&ex.public);

    for s in &preamble {
        lines.push(ChatLine {
            role: ChatRole::Agent,
            text: s.clone(),
            tool: None,
            reordered: true,
        });
    }
    for s in &ex.reasoning {
        lines.push(ChatLine {
            role: ChatRole::Thinking,
            text: s.clone(),
            tool: None,
            reordered: true,
        });
    }
    for s in &ex.status {
        lines.push(ChatLine {
            role: ChatRole::Status,
            text: s.clone(),
            tool: None,
            reordered: true,
        });
    }

    let (pairs, leftover_steps) = pair_tools_with_steps(&ex.tools, &steps);
    for (tool, step) in pairs {
        lines.push(ChatLine {
            role: ChatRole::Tool,
            text: String::new(),
            tool: Some(tool),
            reordered: true,
        });
        if let Some(step) = step {
            lines.push(ChatLine {
                role: ChatRole::Agent,
                text: step,
                tool: None,
                reordered: true,
            });
        }
    }
    for s in leftover_steps {
        lines.push(ChatLine {
            role: ChatRole::Agent,
            text: s,
            tool: None,
            reordered: true,
        });
    }

    for s in &epilogue {
        lines.push(ChatLine {
            role: ChatRole::Agent,
            text: s.clone(),
            tool: None,
            reordered: true,
        });
    }
    lines
}

/// Pair tools with steps (keyword match, then ordinal). Returns leftover steps.
fn pair_tools_with_steps(
    tools: &[ProjectedTool],
    steps: &[String],
) -> (Vec<(ProjectedTool, Option<String>)>, Vec<String>) {
    let mut used_steps = vec![false; steps.len()];
    let mut out: Vec<(ProjectedTool, Option<String>)> = Vec::new();

    for (i, tool) in tools.iter().enumerate() {
        let mut chosen: Option<usize> = None;
        let keys = step_keys_for_tool(tool);
        for (j, step) in steps.iter().enumerate() {
            if used_steps[j] {
                continue;
            }
            let upper = step.to_ascii_uppercase();
            if keys.iter().any(|k| upper.contains(k)) {
                chosen = Some(j);
                break;
            }
        }
        if chosen.is_none() && i < steps.len() && !used_steps[i] {
            chosen = Some(i);
        }
        if chosen.is_none() {
            chosen = used_steps.iter().position(|&u| !u);
        }
        let step_text = chosen.map(|j| {
            used_steps[j] = true;
            steps[j].clone()
        });
        out.push((tool.clone(), step_text));
    }

    let leftover: Vec<String> = steps
        .iter()
        .enumerate()
        .filter(|(j, _)| !used_steps[*j])
        .map(|(_, s)| s.clone())
        .collect();
    (out, leftover)
}

fn step_keys_for_tool(tool: &ProjectedTool) -> Vec<&'static str> {
    let v = tool.verb.to_ascii_uppercase();
    let title = tool.title.to_ascii_uppercase();
    let mut keys = Vec::new();
    if v == "WRITE" || title.contains("WRITE") {
        keys.push("CREATE");
        keys.push("UPDATE");
        keys.push("WRITE");
    }
    if v == "READ" || title.contains("READ") {
        keys.push("READ");
    }
    if v == "EXECUTE" || title.contains("EXECUTE") || title.contains("RM ") || title.contains("`RM")
    {
        keys.push("DELETE");
        keys.push("REMOVE");
    }
    if v == "DELETE" {
        keys.push("DELETE");
    }
    // command: rm → DELETE
    if let Some(a) = &tool.args {
        if a.contains("\"rm ") || a.contains("rm /") || a.contains("\"command\":\"rm") {
            keys.push("DELETE");
        }
    }
    keys
}

fn assemble_chronological(ex: &Extracted) -> Vec<ChatLine> {
    let mut lines = Vec::new();
    // Merge consecutive same-channel text into one bubble for readability.
    let mut pending_role: Option<ChatRole> = None;
    let mut pending_text: Vec<String> = Vec::new();

    let flush = |role: &mut Option<ChatRole>,
                 buf: &mut Vec<String>,
                 lines: &mut Vec<ChatLine>| {
        if let Some(r) = role.take() {
            if !buf.is_empty() {
                lines.push(ChatLine {
                    role: r,
                    text: join_soft(buf),
                    tool: None,
                    reordered: false,
                });
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
                lines.push(ChatLine {
                    role: ChatRole::Tool,
                    text: String::new(),
                    tool: Some(t.clone()),
                    reordered: false,
                });
            }
        }
    }
    flush(&mut pending_role, &mut pending_text, &mut lines);
    lines
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

/// Collapse adjacent agent bubbles into one for chat readability.
fn merge_consecutive_agent(lines: &[ChatLine]) -> Vec<ChatLine> {
    let mut out: Vec<ChatLine> = Vec::new();
    for line in lines {
        if line.role == ChatRole::Agent {
            if let Some(prev) = out.last_mut() {
                if prev.role == ChatRole::Agent {
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

fn render_plain(lines: &[ChatLine], strategy: ProjectionStrategy) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "=== CHAT PROJECTION ({strategy:?}) — not ground truth ===\n"
    ));
    out.push_str(DISCLAIMER);
    out.push_str("\n\n");
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

fn render_html(lines: &[ChatLine], strategy: ProjectionStrategy) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "<div class=\"chat-projection\" data-strategy=\"{strategy:?}\">\n"
    ));
    out.push_str("<div class=\"chat-disclaimer\">");
    out.push_str(&escape(DISCLAIMER));
    out.push_str("</div>\n");
    out.push_str(&format!(
        "<p class=\"chat-strategy\">Strategy: <code>{strategy:?}</code></p>\n"
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
    // Local minimal conversion: reuse pulldown via crate-level helper would create
    // a cycle if html_report imports us — call pulldown directly.
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
    fn tools_first_narrative_pairs_with_steps() {
        let events = vec![
            tool_ev("a1", "Write `file.txt`", 1),
            tool_ev("a2", "Read `file.txt`", 2),
            tool_ev("a3", "Execute `rm file.txt`", 3),
            text_ev("I'll run the CRUD steps, starting with create.", 4),
            text_ev("CRUD exercise on `file.txt` only:", 5),
            text_ev("1. **CREATE** — Wrote the file.", 6),
            text_ev("2. **READ** — File contained x.", 7),
            text_ev("3. **DELETE** — Removed the file.", 8),
            text_ev("No other files were touched.", 9),
        ];
        let p = project_chat(&events);
        assert_eq!(p.strategy, ProjectionStrategy::NarrativeReassembly);
        assert!(p.plain_text.contains("not ground truth"));
        // Preamble first.
        assert_eq!(p.lines[0].role, ChatRole::Agent);
        assert!(p.lines[0].text.contains("I'll run"));
        // Then tool, agent step, tool, agent step…
        let roles: Vec<ChatRole> = p.lines.iter().map(|l| l.role).collect();
        assert!(
            roles.windows(2).any(|w| w == [ChatRole::Tool, ChatRole::Agent]),
            "expected tool then step narration: {roles:?}"
        );
        // Write before CREATE step text in plain.
        let plain = &p.plain_text;
        let write_pos = plain.find("Write").expect("Write");
        let create_pos = plain.find("CREATE").expect("CREATE");
        let read_pos = plain.find("TOOL: Read").or_else(|| plain.find("Read `")).expect("Read tool");
        let delete_pos = plain.find("DELETE").expect("DELETE");
        assert!(write_pos < create_pos);
        assert!(create_pos < read_pos || write_pos < read_pos);
        assert!(delete_pos > write_pos);
        // Epilogue last-ish.
        assert!(plain.contains("No other files were touched"));
        assert!(p.html.contains("chat-projection"));
        assert!(p.html.contains("Agent"));
        assert!(p.html.contains("Tool"));
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
        // Order preserved: agent, tool, agent, tool, agent.
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
    fn disclaimer_always_present() {
        let p = project_chat(&[text_ev("Hello.", 1)]);
        assert!(p.disclaimer.contains("not ground truth"));
        assert!(p.plain_text.contains("not ground truth"));
        assert!(p.html.contains("chat-disclaimer"));
    }
}
