//! Human-digestible **chat projection** of canonical Interpreter events.
//!
//! **Test kit only.** This is a *report*, not ground truth.
//!
//! ## Design (scalable / universal)
//!
//! **Human chat default — dialect source order when present**
//!
//! Complete units often *emit* in a jumbled order relative to how a reader
//! expects the turn (sentence assembly waits while tools complete). When the
//! dialect supplies `source_time` (Grok `agentTimestampMs`) and/or
//! `source_step` (Antigravity `_meta.stepIdx` / numeric `messageId`), the
//! chat report sorts by those observational keys so humans see production
//! order. Missing keys fall back to emit order. Canonical timeline /
//! interleaved stream stay emit-order ground truth.
//!
//! **Fallback core — emit-order chronological chat**
//!
//! Map channels to chat roles, collapse consecutive same-role text. No invented
//! speech, no tool↔sentence claims.
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
//! If any gate fails → chronological (source-time or emit). Prefer honesty over
//! a pretty but wrong weave.
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
    CanonicalUnit, InterpreterOutputEvent, SourceTimeObservation, TextChannel, ToolRequestState,
    UnitState,
};
use std::collections::HashMap;

/// Strategy used to lay out the projected chat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionStrategy {
    /// Chronological layout with chat chrome (source-time or emit order).
    ChronologicalChat,
    /// Tools-first dump + equal numbered steps → ordinal tool/step zip only.
    StructuralOrdinalZip,
}

/// Confidence of the projection relative to Interpreter emit order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionConfidence {
    /// Layout follows emit order (no reordering of tools vs text).
    EmitOrder,
    /// Chronological blocks reordered by dialect `source_time.first_ms` for
    /// human readability. Not causality; content still complete units only.
    DialectSourceTime,
    /// Chronological blocks reordered by dialect `source_step` (stream sequence
    /// id) when wall-clock source times are absent. Observational only.
    DialectSourceStep,
    /// Tools/steps reordered under structural gates; still no invented content.
    StructuralReorder,
}

/// Options for [`project_chat_with`].
#[derive(Clone, Debug)]
pub struct ProjectChatOptions {
    /// When true (default), allow [`ProjectionStrategy::StructuralOrdinalZip`]
    /// if structural gates pass. When false, always chronological.
    pub allow_structural_zip: bool,
    /// When true (default), sort chronological blocks by dialect observational
    /// order when present: `source_time.first_ms`, then `source_step`, then emit
    /// index. When neither time nor step is present, emit order is preserved.
    /// Set false to force pure emit-order chat.
    pub order_by_source_time: bool,
    /// When true (default), annotate lines with dialect source-time / source-step
    /// when known.
    pub annotate_source_time: bool,
}

impl Default for ProjectChatOptions {
    fn default() -> Self {
        Self {
            allow_structural_zip: true,
            // Human product surface: avoid jumbled tools-before-speech when the
            // dialect clock is available. Kernel emit order remains elsewhere.
            order_by_source_time: true,
            annotate_source_time: true,
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
    /// Dialect source time when present on the unit snapshot.
    pub source_time: Option<SourceTimeObservation>,
    /// Dialect stream step when present on the unit snapshot.
    pub source_step: Option<u64>,
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
    /// Dialect source time when present (observational).
    pub source_time: Option<SourceTimeObservation>,
    /// Dialect stream step when present (observational).
    pub source_step: Option<u64>,
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
When dialect source times or stream steps are present, chat lines are ordered by \
those observational keys so readers see production order rather than emit-order \
jumble. Structural ordinal zip (when shown) only reorders tools against later \
numbered list steps when counts match — it does not invent speech. Use the \
event-order interleaved stream and canonical timeline for exact Interpreter \
emit order.";

/// Project canonical Interpreter events into a chat-like flow (default options).
pub fn project_chat(events: &[InterpreterOutputEvent]) -> ChatProjection {
    project_chat_with(events, &ProjectChatOptions::default())
}

/// Project with explicit options.
pub fn project_chat_with(
    events: &[InterpreterOutputEvent],
    opts: &ProjectChatOptions,
) -> ChatProjection {
    let mut extracted = extract(events);
    let source_order = if opts.order_by_source_time {
        apply_source_order(&mut extracted.chrono)
    } else {
        SourceOrderApplied::None
    };
    let (strategy, reason) = choose_strategy(&extracted, opts);
    let mut reason = reason;
    match source_order {
        SourceOrderApplied::ByTime => {
            reason.push_str(
                "; human chat ordered by dialect source_time.first_ms (then source_step, then emit)",
            );
        }
        SourceOrderApplied::ByStep => {
            reason.push_str(
                "; human chat ordered by dialect source_step (emit order preserved when steps absent)",
            );
        }
        SourceOrderApplied::None => {}
    }
    let confidence = match strategy {
        ProjectionStrategy::ChronologicalChat => match source_order {
            SourceOrderApplied::ByTime => ProjectionConfidence::DialectSourceTime,
            SourceOrderApplied::ByStep => ProjectionConfidence::DialectSourceStep,
            SourceOrderApplied::None => ProjectionConfidence::EmitOrder,
        },
        ProjectionStrategy::StructuralOrdinalZip => ProjectionConfidence::StructuralReorder,
    };
    let lines = match strategy {
        ProjectionStrategy::StructuralOrdinalZip => assemble_structural_zip(&extracted),
        ProjectionStrategy::ChronologicalChat => assemble_chronological(&extracted),
    };
    let lines = merge_consecutive_same_role(&lines);
    let plain_text = render_plain(
        &lines,
        strategy,
        confidence,
        &reason,
        opts.annotate_source_time,
    );
    let html = render_html(
        &lines,
        strategy,
        confidence,
        &reason,
        opts.annotate_source_time,
    );
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
        source_time: Option<SourceTimeObservation>,
        source_step: Option<u64>,
        emit_index: usize,
    },
    Tool {
        tool: ProjectedTool,
        emit_index: usize,
    },
}

/// Which dialect observational key (if any) changed display order vs emit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceOrderApplied {
    None,
    ByTime,
    ByStep,
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
    let mut emit_index = 0usize;

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
                        source_time: snap.source_time,
                        source_step: snap.source_step,
                        emit_index,
                    });
                    emit_index += 1;
                }
                TextChannel::PublicReasoningSummary => {
                    reasoning.push(t.content.clone());
                    chrono.push(ChronoBlock::Text {
                        channel: t.channel,
                        content: t.content.clone(),
                        source_time: snap.source_time,
                        source_step: snap.source_step,
                        emit_index,
                    });
                    emit_index += 1;
                }
                TextChannel::StatusNarration => {
                    status.push(t.content.clone());
                    chrono.push(ChronoBlock::Text {
                        channel: t.channel,
                        content: t.content.clone(),
                        source_time: snap.source_time,
                        source_step: snap.source_step,
                        emit_index,
                    });
                    emit_index += 1;
                }
                TextChannel::QuotedExternalContent => {}
            },
            CanonicalUnit::Tool(t) => {
                if saw_public {
                    tool_after_public = true;
                }
                let projected =
                    projected_tool(t, snap.unit_state, snap.source_time, snap.source_step);
                let id = projected.action_id.clone();
                if let Some(&idx) = tool_index.get(&id) {
                    tools[idx] = projected.clone();
                    if let Some(ChronoBlock::Tool { tool: slot, .. }) =
                        chrono.iter_mut().find(|b| match b {
                            ChronoBlock::Tool { tool: p, .. } => p.action_id == id,
                            _ => false,
                        })
                    {
                        *slot = projected;
                    }
                } else {
                    tool_index.insert(id, tools.len());
                    tools.push(projected.clone());
                    chrono.push(ChronoBlock::Tool {
                        tool: projected,
                        emit_index,
                    });
                    emit_index += 1;
                }
            }
            _ => {}
        }
    }

    let tools_before_public = !tools.is_empty() && !public.is_empty() && !tool_after_public;

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
    source_time: Option<SourceTimeObservation>,
    source_step: Option<u64>,
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
        source_time,
        source_step,
    }
}

/// Sort chrono blocks by dialect observational keys when present.
///
/// Preference: wall-clock `source_time.first_ms` (Grok), then stream
/// `source_step` (Antigravity stepIdx/messageId), then emit index.
fn apply_source_order(chrono: &mut [ChronoBlock]) -> SourceOrderApplied {
    if chrono.is_empty() {
        return SourceOrderApplied::None;
    }
    let any_time = chrono.iter().any(|b| block_source_time(b).is_some());
    let any_step = chrono.iter().any(|b| block_source_step(b).is_some());
    if !any_time && !any_step {
        return SourceOrderApplied::None;
    }
    let before: Vec<usize> = chrono.iter().map(block_emit_index).collect();
    chrono.sort_by_key(|b| {
        let t = block_source_time(b).map(|s| s.first_ms).unwrap_or(u64::MAX);
        let step = block_source_step(b).unwrap_or(u64::MAX);
        (t, step, block_emit_index(b))
    });
    let after: Vec<usize> = chrono.iter().map(block_emit_index).collect();
    if before == after {
        return SourceOrderApplied::None;
    }
    if any_time {
        SourceOrderApplied::ByTime
    } else {
        SourceOrderApplied::ByStep
    }
}

fn block_source_time(b: &ChronoBlock) -> Option<SourceTimeObservation> {
    match b {
        ChronoBlock::Text { source_time, .. } => *source_time,
        ChronoBlock::Tool { tool, .. } => tool.source_time,
    }
}

fn block_source_step(b: &ChronoBlock) -> Option<u64> {
    match b {
        ChronoBlock::Text { source_step, .. } => *source_step,
        ChronoBlock::Tool { tool, .. } => tool.source_step,
    }
}

fn block_emit_index(b: &ChronoBlock) -> usize {
    match b {
        ChronoBlock::Text { emit_index, .. } | ChronoBlock::Tool { emit_index, .. } => *emit_index,
    }
}

fn format_source_meta(st: Option<SourceTimeObservation>, step: Option<u64>) -> Option<String> {
    match (st, step) {
        (Some(t), Some(s)) => {
            if t.first_ms == t.last_ms {
                Some(format!("t={} s={s}", t.first_ms))
            } else {
                Some(format!("t={}..{} s={s}", t.first_ms, t.last_ms))
            }
        }
        (Some(t), None) => {
            if t.first_ms == t.last_ms {
                Some(format!("t={}", t.first_ms))
            } else {
                Some(format!("t={}..{}", t.first_ms, t.last_ms))
            }
        }
        (None, Some(s)) => Some(format!("s={s}")),
        (None, None) => None,
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

fn choose_strategy(ex: &Extracted, opts: &ProjectChatOptions) -> (ProjectionStrategy, String) {
    if !opts.allow_structural_zip {
        return (
            ProjectionStrategy::ChronologicalChat,
            "structural zip disabled by options".into(),
        );
    }

    if !ex.tools_before_public {
        return (
            ProjectionStrategy::ChronologicalChat,
            "emit-order chat (tools not strictly before public text, or no tools/text)".into(),
        );
    }

    let list_steps = count_list_steps(&ex.public);
    let n_tools = ex.tools.len();

    if list_steps == 0 {
        return (
            ProjectionStrategy::ChronologicalChat,
            "tools-first dump without numbered list steps — chronological (no safe zip)".into(),
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
    public
        .iter()
        .filter(|s| looks_like_list_item(s.trim()))
        .count()
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
        lines.push(line(ChatRole::Thinking, s.clone(), None, true, None, None));
    }
    for s in &ex.status {
        lines.push(line(ChatRole::Status, s.clone(), None, true, None, None));
    }
    for s in &preamble {
        lines.push(line(ChatRole::Agent, s.clone(), None, true, None, None));
    }

    // Gates guarantee steps.len() == tools.len().
    for (tool, step) in ex.tools.iter().zip(steps.iter()) {
        lines.push(line(
            ChatRole::Tool,
            String::new(),
            Some(tool.clone()),
            true,
            tool.source_time,
            tool.source_step,
        ));
        lines.push(line(ChatRole::Agent, step.clone(), None, true, None, None));
    }

    for s in &epilogue {
        lines.push(line(ChatRole::Agent, s.clone(), None, true, None, None));
    }
    lines
}

fn assemble_chronological(ex: &Extracted) -> Vec<ChatLine> {
    let mut lines = Vec::new();
    let mut pending_role: Option<ChatRole> = None;
    let mut pending_text: Vec<String> = Vec::new();
    let mut pending_time: Option<SourceTimeObservation> = None;
    let mut pending_step: Option<u64> = None;

    let flush = |role: &mut Option<ChatRole>,
                 buf: &mut Vec<String>,
                 time: &mut Option<SourceTimeObservation>,
                 step: &mut Option<u64>,
                 lines: &mut Vec<ChatLine>| {
        if let Some(r) = role.take() {
            if !buf.is_empty() {
                lines.push(line(
                    r,
                    join_soft(buf),
                    None,
                    false,
                    time.take(),
                    step.take(),
                ));
                buf.clear();
            }
        }
    };

    for b in &ex.chrono {
        match b {
            ChronoBlock::Text {
                channel,
                content,
                source_time,
                source_step,
                ..
            } => {
                let role = match channel {
                    TextChannel::PublicResponse => ChatRole::Agent,
                    TextChannel::PublicReasoningSummary => ChatRole::Thinking,
                    TextChannel::StatusNarration => ChatRole::Status,
                    TextChannel::QuotedExternalContent => continue,
                };
                if pending_role != Some(role) {
                    flush(
                        &mut pending_role,
                        &mut pending_text,
                        &mut pending_time,
                        &mut pending_step,
                        &mut lines,
                    );
                    pending_role = Some(role);
                }
                pending_text.push(content.clone());
                pending_time = match (pending_time, *source_time) {
                    (Some(a), Some(b)) => Some(a.merge(b)),
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                };
                pending_step = match (pending_step, *source_step) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                };
            }
            ChronoBlock::Tool { tool: t, .. } => {
                flush(
                    &mut pending_role,
                    &mut pending_text,
                    &mut pending_time,
                    &mut pending_step,
                    &mut lines,
                );
                lines.push(line(
                    ChatRole::Tool,
                    String::new(),
                    Some(t.clone()),
                    false,
                    t.source_time,
                    t.source_step,
                ));
            }
        }
    }
    flush(
        &mut pending_role,
        &mut pending_text,
        &mut pending_time,
        &mut pending_step,
        &mut lines,
    );
    lines
}

fn line(
    role: ChatRole,
    text: String,
    tool: Option<ProjectedTool>,
    reordered: bool,
    source_time: Option<SourceTimeObservation>,
    source_step: Option<u64>,
) -> ChatLine {
    ChatLine {
        role,
        text,
        tool,
        reordered,
        source_time,
        source_step,
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
                    prev.source_time = match (prev.source_time, line.source_time) {
                        (Some(a), Some(b)) => Some(a.merge(b)),
                        (Some(a), None) => Some(a),
                        (None, Some(b)) => Some(b),
                        (None, None) => None,
                    };
                    prev.source_step = match (prev.source_step, line.source_step) {
                        (Some(a), Some(b)) => Some(a.min(b)),
                        (Some(a), None) => Some(a),
                        (None, Some(b)) => Some(b),
                        (None, None) => None,
                    };
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
    annotate_source_time: bool,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "=== CHAT PROJECTION ({strategy:?} / {confidence:?}) — not ground truth ===\n"
    ));
    out.push_str(DISCLAIMER);
    out.push_str("\n");
    out.push_str(&format!("reason: {reason}\n\n"));
    for line in lines {
        let t_note = if annotate_source_time {
            format_source_meta(line.source_time, line.source_step)
                .map(|s| format!(" [{s}]"))
                .unwrap_or_default()
        } else {
            String::new()
        };
        match (&line.role, &line.tool) {
            (ChatRole::Agent, _) => {
                out.push_str(&format!("AGENT{t_note}:\n"));
                out.push_str(&line.text);
                out.push_str("\n\n");
            }
            (ChatRole::Thinking, _) => {
                out.push_str(&format!("THINKING{t_note}:\n… "));
                out.push_str(&line.text);
                out.push_str("\n\n");
            }
            (ChatRole::Status, _) => {
                out.push_str(&format!("STATUS{t_note}:\n"));
                out.push_str(&line.text);
                out.push_str("\n\n");
            }
            (ChatRole::Tool, Some(t)) => {
                out.push_str(&format!("TOOL{t_note}: {} — {}\n", t.verb, t.title));
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
    annotate_source_time: bool,
) -> String {
    let conf_class = match confidence {
        ProjectionConfidence::EmitOrder => "conf-emit",
        ProjectionConfidence::DialectSourceTime => "conf-source-time",
        ProjectionConfidence::DialectSourceStep => "conf-source-step",
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
        let t_html = if annotate_source_time {
            format_source_meta(line.source_time, line.source_step)
                .map(|s| format!(" <span class=\"chat-source-time\">{}</span>", escape(&s)))
                .unwrap_or_default()
        } else {
            String::new()
        };
        match (&line.role, &line.tool) {
            (ChatRole::Agent, _) => {
                out.push_str(&agent_bubble(&line.text, line.reordered, &t_html));
            }
            (ChatRole::Thinking, _) => {
                out.push_str("<div class=\"chat-line thinking");
                if line.reordered {
                    out.push_str(" reordered");
                }
                out.push_str("\"><div class=\"chat-role\">Thinking");
                out.push_str(&t_html);
                out.push_str("</div>");
                out.push_str("<div class=\"chat-body thinking-body\">… ");
                out.push_str(&md_html(&line.text));
                out.push_str("</div></div>\n");
            }
            (ChatRole::Status, _) => {
                out.push_str("<div class=\"chat-line status");
                if line.reordered {
                    out.push_str(" reordered");
                }
                out.push_str("\"><div class=\"chat-role\">Status");
                out.push_str(&t_html);
                out.push_str("</div>");
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
                out.push_str("<div class=\"chat-role\">Tool");
                out.push_str(&t_html);
                out.push_str("</div>");
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

fn agent_bubble(text: &str, reordered: bool, time_html: &str) -> String {
    let mut out = String::new();
    out.push_str("<div class=\"chat-line agent");
    if reordered {
        out.push_str(" reordered");
    }
    out.push_str("\"><div class=\"chat-role\">Agent");
    out.push_str(time_html);
    out.push_str("</div>");
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
        text_ev_meta(content, n, None, None)
    }

    fn text_ev_at(
        content: &str,
        n: u64,
        source_time: Option<SourceTimeObservation>,
    ) -> InterpreterOutputEvent {
        text_ev_meta(content, n, source_time, None)
    }

    fn text_ev_step(content: &str, n: u64, step: u64) -> InterpreterOutputEvent {
        text_ev_meta(content, n, None, Some(step))
    }

    fn text_ev_meta(
        content: &str,
        n: u64,
        source_time: Option<SourceTimeObservation>,
        source_step: Option<u64>,
    ) -> InterpreterOutputEvent {
        InterpreterOutputEvent::Unit(Box::new(CanonicalUnitEvent::Created(
            CanonicalUnitSnapshot {
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
                source_time,
                source_step,
                unit: CanonicalUnit::Text(TextSentence {
                    sentence_id: UnitId::new(format!("s{n}")),
                    channel: TextChannel::PublicResponse,
                    paragraph_id: None,
                    sentence_ordinal: n,
                    content: content.into(),
                }),
            },
        )))
    }

    fn tool_ev(id: &str, name: &str, n: u64) -> InterpreterOutputEvent {
        tool_ev_meta(id, name, n, None, None)
    }

    fn tool_ev_at(
        id: &str,
        name: &str,
        n: u64,
        source_time: Option<SourceTimeObservation>,
    ) -> InterpreterOutputEvent {
        tool_ev_meta(id, name, n, source_time, None)
    }

    fn tool_ev_step(id: &str, name: &str, n: u64, step: u64) -> InterpreterOutputEvent {
        tool_ev_meta(id, name, n, None, Some(step))
    }

    fn tool_ev_meta(
        id: &str,
        name: &str,
        n: u64,
        source_time: Option<SourceTimeObservation>,
        source_step: Option<u64>,
    ) -> InterpreterOutputEvent {
        InterpreterOutputEvent::Unit(Box::new(CanonicalUnitEvent::Created(
            CanonicalUnitSnapshot {
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
                source_time,
                source_step,
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
            },
        )))
    }

    fn st(ms: u64) -> SourceTimeObservation {
        SourceTimeObservation::point(ms)
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
            roles
                .windows(2)
                .any(|w| w == [ChatRole::Tool, ChatRole::Agent]),
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
        assert_eq!(roles, vec![ChatRole::Tool, ChatRole::Tool, ChatRole::Agent]);
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
        let events = vec![tool_ev("a1", "Write `f`", 1), text_ev("1. Wrote it.", 2)];
        let p = project_chat_with(
            &events,
            &ProjectChatOptions {
                allow_structural_zip: false,
                ..Default::default()
            },
        );
        assert_eq!(p.strategy, ProjectionStrategy::ChronologicalChat);
        assert!(p.strategy_reason.contains("disabled"));
    }

    /// Live Grok shape: tools emit before the complete sentence, but dialect
    /// source times show speech started earlier — human chat must not look jumbled.
    #[test]
    fn source_time_puts_earlier_speech_before_later_tools() {
        let events = vec![
            // Emit order: tools first (sentence still assembling when tools finished).
            tool_ev_at("call-1", "Write `f`", 1, Some(st(2000))),
            tool_ev_at("call-2", "Read `f`", 2, Some(st(3000))),
            text_ev_at(
                "I'll run the CRUD steps, starting by creating it.",
                3,
                Some(SourceTimeObservation {
                    first_ms: 1000,
                    last_ms: 1500,
                }),
            ),
            text_ev_at("1. **CREATE** — Wrote the file.", 4, Some(st(4000))),
        ];
        let p = project_chat(&events);
        assert_eq!(p.strategy, ProjectionStrategy::ChronologicalChat);
        assert_eq!(p.confidence, ProjectionConfidence::DialectSourceTime);
        assert!(
            p.strategy_reason.contains("source_time"),
            "{}",
            p.strategy_reason
        );
        let roles: Vec<_> = p.lines.iter().map(|l| l.role).collect();
        assert_eq!(
            roles,
            vec![
                ChatRole::Agent, // intro — earlier first_ms
                ChatRole::Tool,
                ChatRole::Tool,
                ChatRole::Agent, // list step — later first_ms
            ],
            "human order: speech intro then tools: {roles:?}"
        );
        assert!(
            p.lines[0].text.contains("I'll run the CRUD steps"),
            "intro first: {:?}",
            p.lines[0]
        );
        // Pure emit order would still show tools first when source-time is forced off.
        let emit = project_chat_with(
            &events,
            &ProjectChatOptions {
                order_by_source_time: false,
                allow_structural_zip: false,
                ..Default::default()
            },
        );
        assert_eq!(emit.confidence, ProjectionConfidence::EmitOrder);
        assert_eq!(emit.lines[0].role, ChatRole::Tool);
    }

    #[test]
    fn without_source_times_emit_order_unchanged() {
        let events = vec![tool_ev("a1", "Write `f`", 1), text_ev("I'll start.", 2)];
        let p = project_chat_with(
            &events,
            &ProjectChatOptions {
                allow_structural_zip: false,
                order_by_source_time: true,
                ..Default::default()
            },
        );
        assert_eq!(p.confidence, ProjectionConfidence::EmitOrder);
        assert_eq!(p.lines[0].role, ChatRole::Tool);
    }

    /// Antigravity-like: no wall-clock times, but stream `source_step` present.
    /// Emit order can put later-step text before earlier-step tools (or reverse);
    /// human chat follows stepIdx / messageId.
    #[test]
    fn source_step_orders_when_times_absent() {
        let events = vec![
            // Emit: tools first, then text — but text has lower step than one tool
            // (simulate out-of-order completion relative to dialect sequence).
            tool_ev_step("call-a", "Create file", 1, 5),
            tool_ev_step("call-b", "Read file", 2, 8),
            text_ev_step("I'll start the CRUD.", 3, 2),
            text_ev_step("1. **CREATE** — done.", 4, 11),
        ];
        let p = project_chat_with(
            &events,
            &ProjectChatOptions {
                allow_structural_zip: false,
                order_by_source_time: true,
                ..Default::default()
            },
        );
        assert_eq!(p.confidence, ProjectionConfidence::DialectSourceStep);
        assert!(
            p.strategy_reason.contains("source_step"),
            "{}",
            p.strategy_reason
        );
        let roles: Vec<_> = p.lines.iter().map(|l| l.role).collect();
        assert_eq!(
            roles,
            vec![
                ChatRole::Agent, // step 2
                ChatRole::Tool,  // step 5
                ChatRole::Tool,  // step 8
                ChatRole::Agent, // step 11
            ],
            "human order by source_step: {roles:?}"
        );
        assert!(
            p.lines[0].text.contains("I'll start"),
            "intro first: {:?}",
            p.lines[0]
        );
        assert!(
            p.plain_text.contains("s=2") || p.lines[0].source_step == Some(2),
            "step annotation: {}",
            p.plain_text
        );
    }

    #[test]
    fn text_only_is_one_agent_bubble() {
        let p = project_chat(&[text_ev("Hello.", 1), text_ev("World.", 2)]);
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
