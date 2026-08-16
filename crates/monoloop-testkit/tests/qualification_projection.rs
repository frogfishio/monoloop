//! Qualification matrix: Interpreter assembly + chat projection.
//!
//! Deterministic ACP fixtures (no live Grok). Covers the shapes we care about
//! for sentence assembly and human chat projection.
//!
//! Run:
//! ```bash
//! cargo test -p monoloop-testkit --test qualification_projection
//! # or: ./scripts/qualify-interpreter-projection.sh
//! ```

use monoloop_contracts::{
    CanonicalUnit, InterpreterOutputEvent, ToolRequestState, ToolTerminalOutcome,
};
use monoloop_testkit::{
    acp_binding, build_html_report, feed_chunks, project_chat, ChatRole, HtmlReportParams,
    ProjectionConfidence, ProjectionStrategy,
};

// ── ACP fixture helpers ─────────────────────────────────────────────────────

fn msg_chunk(text: &str) -> String {
    // root.params.update.content — four nested objects after `text` value.
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"s","update":{{"sessionUpdate":"agent_message_chunk","content":{{"type":"text","text":{}}}}}}}}}"#,
        serde_json::to_string(text).unwrap()
    )
}

fn tool_pending(id: &str, title: &str) -> String {
    // root.params.update — three nested objects.
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"s","update":{{"sessionUpdate":"tool_call","toolCallId":"{id}","title":{title},"status":"pending"}}}}}}"#,
        title = serde_json::to_string(title).unwrap()
    )
}

fn tool_ready(id: &str, title: &str, args_json: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"s","update":{{"sessionUpdate":"tool_call_update","toolCallId":"{id}","title":{title},"status":"pending","rawInput":{args_json}}}}}}}"#,
        title = serde_json::to_string(title).unwrap()
    )
}

fn tool_done(id: &str, title: &str, args_json: &str) -> String {
    // root.params.update.rawOutput — four nested after rawOutput object.
    format!(
        r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"s","update":{{"sessionUpdate":"tool_call_update","toolCallId":"{id}","title":{title},"status":"completed","rawInput":{args_json},"rawOutput":{{"ok":true}}}}}}}}"#,
        title = serde_json::to_string(title).unwrap()
    )
}

fn end_turn() -> &'static str {
    r#"{"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}"#
}

fn stream(parts: &[String]) -> Vec<bytes::Bytes> {
    parts
        .iter()
        .map(|p| bytes::Bytes::from(format!("{p}\n")))
        .collect()
}

fn public_texts(events: &[InterpreterOutputEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Text(t)
                    if matches!(
                        t.channel,
                        monoloop_contracts::TextChannel::PublicResponse
                    ) =>
                {
                    Some(t.content.clone())
                }
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn tool_actions(events: &[InterpreterOutputEvent]) -> Vec<(String, ToolRequestState, Option<ToolTerminalOutcome>)> {
    events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Tool(t) => Some((
                    t.tool_action_id.as_str().to_string(),
                    t.request_state,
                    t.terminal_outcome,
                )),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn unique_tool_ids(events: &[InterpreterOutputEvent]) -> Vec<String> {
    let mut seen = Vec::new();
    for (id, _, _) in tool_actions(events) {
        if !seen.contains(&id) {
            seen.push(id);
        }
    }
    seen
}

// ── scenarios ───────────────────────────────────────────────────────────────

/// Shape A: tools-first + N numbered steps matching tool count → structural zip.
#[tokio::test]
async fn q_tools_first_equal_list_steps_structural_zip() {
    let parts = vec![
        tool_pending("t1", "write"),
        tool_ready("t1", "Write `a.txt`", r#"{"file_path":"a.txt","content":"x"}"#),
        tool_done("t1", "Write `a.txt`", r#"{"file_path":"a.txt","content":"x"}"#),
        tool_pending("t2", "read_file"),
        tool_ready("t2", "Read `a.txt`", r#"{"target_file":"a.txt"}"#),
        tool_done("t2", "Read `a.txt`", r#"{"target_file":"a.txt"}"#),
        tool_pending("t3", "run_terminal_command"),
        tool_ready("t3", "Execute `rm a.txt`", r#"{"command":"rm a.txt"}"#),
        tool_done("t3", "Execute `rm a.txt`", r#"{"command":"rm a.txt"}"#),
        msg_chunk("I'll run three steps.\n\n"),
        // Avoid single-letter-before-period (`x.`) — segmenter treats it as abbrev.
        msg_chunk("1. **CREATE** — Wrote the file.\n"),
        msg_chunk("2. **READ** — File contained hello.\n"),
        msg_chunk("3. **DELETE** — Removed the file.\n\n"),
        msg_chunk("Done."),
        end_turn().into(),
    ];

    let events = feed_chunks(acp_binding(), &stream(&parts), None).await;
    let texts = public_texts(&events);
    assert!(
        texts.iter().any(|t| t.contains("1.") && t.contains("CREATE")),
        "list step attached: {texts:?}"
    );
    assert_eq!(unique_tool_ids(&events).len(), 3, "tools={:?}", unique_tool_ids(&events));

    let chat = project_chat(&events);
    assert_eq!(
        chat.strategy,
        ProjectionStrategy::StructuralOrdinalZip,
        "reason={} texts={texts:?} tools={}",
        chat.strategy_reason,
        unique_tool_ids(&events).len()
    );
    assert_eq!(chat.confidence, ProjectionConfidence::StructuralReorder);
    let roles: Vec<_> = chat.lines.iter().map(|l| l.role).collect();
    assert!(
        roles.windows(2).any(|w| w == [ChatRole::Tool, ChatRole::Agent]),
        "tool then step: {roles:?}"
    );

    let html = build_html_report(&events, &HtmlReportParams::default());
    assert!(html.full_page_html.contains("Chat projection"));
    assert!(html.full_page_html.contains("StructuralOrdinalZip") || html.chat_projection.plain_text.contains("StructuralOrdinalZip"));
    assert!(!html.assembled_markdown.contains("create.CRUD"));
}

/// Shape B: tools-first free prose → chronological (no invent pairing).
#[tokio::test]
async fn q_tools_first_free_prose_chronological() {
    let parts = vec![
        tool_pending("t1", "read_file"),
        tool_ready("t1", "Read `README.md`", r#"{"target_file":"README.md"}"#),
        tool_done("t1", "Read `README.md`", r#"{"target_file":"README.md"}"#),
        tool_pending("t2", "read_file"),
        tool_ready("t2", "Read `DECISIONS.md`", r#"{"target_file":"DECISIONS.md"}"#),
        tool_done("t2", "Read `DECISIONS.md`", r#"{"target_file":"DECISIONS.md"}"#),
        msg_chunk("I skimmed the docs. "),
        msg_chunk("Monoloop looks solid as a three-component kernel."),
        end_turn().into(),
    ];

    let events = feed_chunks(acp_binding(), &stream(&parts), None).await;
    let chat = project_chat(&events);
    assert_eq!(chat.strategy, ProjectionStrategy::ChronologicalChat);
    assert_eq!(chat.confidence, ProjectionConfidence::EmitOrder);
    assert!(chat.strategy_reason.contains("without numbered list") || chat.strategy_reason.contains("chronological") || chat.strategy_reason.contains("emit-order") || chat.strategy_reason.contains("list"));
    // Tools before agent speech in emit order.
    let first_tool = chat.lines.iter().position(|l| l.role == ChatRole::Tool);
    let first_agent = chat.lines.iter().position(|l| l.role == ChatRole::Agent);
    assert!(first_tool < first_agent, "tools-first chrono: {:?}", chat.lines);
}

/// Shape C: natural interleave text → tool → text → tool.
#[tokio::test]
async fn q_interleaved_speech_and_tools() {
    let parts = vec![
        msg_chunk("Let me start by reading the file. "),
        tool_pending("t1", "read_file"),
        tool_ready("t1", "Read `x.txt`", r#"{"target_file":"x.txt"}"#),
        tool_done("t1", "Read `x.txt`", r#"{"target_file":"x.txt"}"#),
        msg_chunk("Looks empty; I'll write content. "),
        tool_pending("t2", "write"),
        tool_ready("t2", "Write `x.txt`", r#"{"file_path":"x.txt","content":"hi"}"#),
        tool_done("t2", "Write `x.txt`", r#"{"file_path":"x.txt","content":"hi"}"#),
        msg_chunk("All set."),
        end_turn().into(),
    ];

    let events = feed_chunks(acp_binding(), &stream(&parts), None).await;
    let chat = project_chat(&events);
    assert_eq!(chat.strategy, ProjectionStrategy::ChronologicalChat);
    assert!(chat.lines.iter().all(|l| !l.reordered));
    let roles: Vec<_> = chat.lines.iter().map(|l| l.role).collect();
    assert_eq!(
        roles,
        vec![
            ChatRole::Agent,
            ChatRole::Tool,
            ChatRole::Agent,
            ChatRole::Tool,
            ChatRole::Agent,
        ],
        "{roles:?}"
    );
}

/// Shape D: missing-space glue `create.CRUD` splits correctly through Interpreter.
#[tokio::test]
async fn q_missing_space_after_period_splits() {
    // Token stream like live Grok: "create." then "CRUD" without space.
    let mut parts = Vec::new();
    for t in [
        "I'll start with create.",
        "CRUD exercise on `f.txt` only:\n\n",
        "1. **CREATE** — Wrote it.\n",
        "2. **READ** — Saw it.\n",
    ] {
        parts.push(msg_chunk(t));
    }
    parts.push(end_turn().into());

    let events = feed_chunks(acp_binding(), &stream(&parts), None).await;
    let texts = public_texts(&events);
    assert!(
        texts.iter().all(|t| !t.contains("create.CRUD")),
        "must not glue: {texts:?}"
    );
    assert!(
        texts.iter().any(|t| t.ends_with("create.") || t.contains("with create.")),
        "{texts:?}"
    );
    assert!(
        texts.iter().any(|t| t.starts_with("CRUD") || t.contains("CRUD exercise")),
        "{texts:?}"
    );
}

/// Shape E: list markers stay with items (no bare `1.` sentences).
#[tokio::test]
async fn q_list_markers_not_bare_sentences() {
    let parts = vec![
        msg_chunk("Steps:\n\n"),
        msg_chunk("1.\n\n"),
        msg_chunk("**CREATE** — Wrote the file.\n\n"),
        msg_chunk("2.\n\n"),
        msg_chunk("**READ** — Contained data.\n\n"),
        end_turn().into(),
    ];
    let events = feed_chunks(acp_binding(), &stream(&parts), None).await;
    let texts = public_texts(&events);
    for t in &texts {
        let bare = t.trim();
        assert!(
            !(bare.chars().all(|c| c.is_ascii_digit() || c == '.') && bare.ends_with('.')),
            "bare list marker leaked: {texts:?}"
        );
    }
    assert!(texts.iter().any(|t| t.contains("CREATE")));
    assert!(texts.iter().any(|t| t.contains("READ")));
}

/// Shape F: mismatched tool vs list counts refuse structural zip.
#[tokio::test]
async fn q_mismatched_counts_refuse_zip() {
    let parts = vec![
        tool_pending("t1", "write"),
        tool_ready("t1", "Write `a`", r#"{"file_path":"a"}"#),
        tool_done("t1", "Write `a`", r#"{"file_path":"a"}"#),
        tool_pending("t2", "read_file"),
        tool_ready("t2", "Read `a`", r#"{"target_file":"a"}"#),
        tool_done("t2", "Read `a`", r#"{"target_file":"a"}"#),
        msg_chunk("Only one step:\n\n"),
        msg_chunk("1. Did something.\n"),
        end_turn().into(),
    ];
    let events = feed_chunks(acp_binding(), &stream(&parts), None).await;
    let chat = project_chat(&events);
    assert_eq!(chat.strategy, ProjectionStrategy::ChronologicalChat);
    assert!(
        chat.strategy_reason.contains('≠')
            || chat.strategy_reason.contains("refuse")
            || chat.strategy_reason.contains("!=")
            || chat.strategy_reason.contains("match")
            || chat.strategy_reason.contains("chronological")
            || chat.strategy_reason.contains("emit"),
        "{}",
        chat.strategy_reason
    );
}

/// Shape G: tool lifecycle reaches Ready + terminal Success.
#[tokio::test]
async fn q_tool_lifecycle_ready_then_success() {
    let parts = vec![
        tool_pending("t1", "bash"),
        tool_ready("t1", "bash", r#"{"command":"ls"}"#),
        tool_done("t1", "bash", r#"{"command":"ls"}"#),
        msg_chunk("Listed the directory."),
        end_turn().into(),
    ];
    let events = feed_chunks(acp_binding(), &stream(&parts), None).await;
    let tools = tool_actions(&events);
    assert!(
        tools.iter().any(|(_, s, _)| *s == ToolRequestState::Ready),
        "ready: {tools:?}"
    );
    assert!(
        tools
            .iter()
            .any(|(_, _, term)| *term == Some(ToolTerminalOutcome::Success)),
        "success terminal: {tools:?}"
    );
}

/// Shape H: byte fragmentation of multi-message ACP stream is invariant.
#[tokio::test]
async fn q_acp_byte_fragmentation_invariant() {
    let mut full = String::new();
    for p in [
        msg_chunk("Hello **world**. "),
        msg_chunk("Second line!\n\n"),
        msg_chunk("1. Step one.\n"),
        end_turn().into(),
    ] {
        full.push_str(&p);
        full.push('\n');
    }
    let bytes = full.into_bytes();
    let whole = feed_chunks(
        acp_binding(),
        &[bytes::Bytes::from(bytes.clone())],
        None,
    )
    .await;
    let mid = bytes.len() / 2;
    let frag = feed_chunks(
        acp_binding(),
        &[
            bytes::Bytes::from(bytes[..mid].to_vec()),
            bytes::Bytes::from(bytes[mid..].to_vec()),
        ],
        None,
    )
    .await;
    assert_eq!(public_texts(&whole), public_texts(&frag));
}

/// Shape I: HTML report always carries disclaimer + dual sections.
#[tokio::test]
async fn q_html_report_has_projection_and_truth_sections() {
    let parts = vec![
        msg_chunk("Only text."),
        end_turn().into(),
    ];
    let events = feed_chunks(acp_binding(), &stream(&parts), None).await;
    let html = build_html_report(&events, &HtmlReportParams::default());
    assert!(html.full_page_html.contains("Chat projection"));
    assert!(html.full_page_html.contains("not ground truth") || html.chat_projection.disclaimer.contains("not ground truth"));
    assert!(html.full_page_html.contains("Interleaved stream") || html.full_page_html.contains("event order"));
    assert!(html.full_page_html.contains("Text-only assembly") || html.full_page_html.contains("timeline") || html.full_page_html.contains("Timeline"));
    assert_eq!(html.sentence_count, 1);
}

/// Shape J: optional replay of saved live dumps when present (skipped if missing).
#[tokio::test]
async fn q_replay_saved_live_dumps_if_present() {
    let roots = [
        std::path::Path::new("target/live_grok_crud.raw.txt"),
        std::path::Path::new("target/live_grok_analyze.raw.txt"),
        // When running from crate dir:
        std::path::Path::new("../../target/live_grok_crud.raw.txt"),
        std::path::Path::new("../../target/live_grok_analyze.raw.txt"),
    ];
    let mut any = false;
    for path in roots {
        if !path.is_file() {
            continue;
        }
        any = true;
        let raw = std::fs::read_to_string(path).expect("read dump");
        let frames = extract_json_frames(&raw);
        assert!(!frames.is_empty(), "empty frames in {}", path.display());
        let chunks: Vec<_> = frames
            .into_iter()
            .map(|j| bytes::Bytes::from(format!("{j}\n")))
            .collect();
        let events = feed_chunks(acp_binding(), &chunks, None).await;
        let chat = project_chat(&events);
        let html = build_html_report(&events, &HtmlReportParams::default());
        assert!(
            !chat.plain_text.is_empty() || html.sentence_count > 0 || !unique_tool_ids(&events).is_empty(),
            "replay produced nothing for {}",
            path.display()
        );
        // Never invent speech that isn't in public texts for chronological mode.
        if chat.strategy == ProjectionStrategy::ChronologicalChat {
            assert_eq!(chat.confidence, ProjectionConfidence::EmitOrder);
        }
        eprintln!(
            "replayed {} → events={} sentences={} strategy={:?} tools={}",
            path.display(),
            events.len(),
            html.sentence_count,
            chat.strategy,
            unique_tool_ids(&events).len()
        );
    }
    if !any {
        eprintln!("no saved live dumps found — skip (run live_grok_ask to capture)");
    }
}

fn extract_json_frames(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i] != b'{' {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let start = i;
        let mut depth = 0i32;
        let mut in_str = false;
        let mut escape = false;
        while i < bytes.len() {
            let c = bytes[i];
            if in_str {
                if escape {
                    escape = false;
                } else if c == b'\\' {
                    escape = true;
                } else if c == b'"' {
                    in_str = false;
                }
            } else {
                match c {
                    b'"' => in_str = true,
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            i += 1;
                            if let Ok(s) = std::str::from_utf8(&bytes[start..i]) {
                                if s.contains("jsonrpc") || s.contains("sessionUpdate") {
                                    out.push(s.to_string());
                                }
                            }
                            break;
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        if depth != 0 {
            break;
        }
    }
    out
}
