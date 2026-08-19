#![allow(clippy::field_reassign_with_default, clippy::while_let_loop, dead_code)]
//! Live Grok Build: open-ended project analysis (rich tool + prose mix).
//!
//! Produces a messier stream than CRUD — good stress for sentence assembly and
//! the chat projector (tools-first free prose, interleaved speech, many tools).
//!
//! **Serve is never started by this binary.** Run it detached, then the client:
//! ```bash
//! ./scripts/grok-serve-detached.sh
//! export GROK_AGENT_SECRET=monoloop-live-test
//! # Default prompt wait is 600s (10 min); override if needed:
//! # export GROK_PROMPT_TIMEOUT_SECS=300
//! cargo run -p monoloop-testkit --example live_grok_analyze
//! open target/live_grok_analyze.html
//! ./scripts/grok-serve-stop.sh
//! ```
//!
//! On timeout the client **salvages** streamed events so partial captures remain
//! usable for dissection.

use monoloop_connector_grok::{
    EncodedAcpSessionMessage, GrokConnector, GrokConnectorLimits, GrokServerConfig,
    GrokSessionConfig, InMemorySecretResolver, RawDumpCollector, SecretRef,
};
use monoloop_contracts::{
    CanonicalUnit, DialectBinding, DialectDescriptor, InterpretationId, InterpretationLimits,
    InterpreterOutputEvent,
};
use monoloop_interpreter::{DefaultInterpreterFactory, InterpreterFactory, StartInterpretation};
use monoloop_testkit::{
    build_html_report, write_html_report, ConsoleRenderer, ConsoleRendererConfig, ConsoleSink,
    HtmlReportParams, SyncMemorySink,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_SECRET: &str = "monoloop-live-test";
const DEFAULT_PORT: u16 = 2419;
/// Default hard stop for `session/prompt` (raise/lower via GROK_PROMPT_TIMEOUT_SECS).
const DEFAULT_PROMPT_TIMEOUT_SECS: u64 = 600;

fn env_secs(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let secret = std::env::var("GROK_AGENT_SECRET").unwrap_or_else(|_| DEFAULT_SECRET.into());
    let port: u16 = std::env::var("GROK_AGENT_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PORT);
    let prompt_timeout_secs = env_secs("GROK_PROMPT_TIMEOUT_SECS", DEFAULT_PROMPT_TIMEOUT_SECS);
    // RPC deadline tracks the outer prompt wait (plus a small margin for cleanup).
    let request_deadline_secs = env_secs(
        "GROK_REQUEST_DEADLINE_SECS",
        prompt_timeout_secs.saturating_add(15),
    );

    let out_html = project.join("target/live_grok_analyze.html");
    let out_raw = project.join("target/live_grok_analyze.raw.txt");
    let out_seq = project.join("target/live_grok_analyze.sequence.txt");
    let out_chat = project.join("target/live_grok_analyze.chat.txt");

    println!("project cwd: {}", project.display());
    println!("connecting to ws://127.0.0.1:{port} …");
    println!(
        "timeouts: prompt={prompt_timeout_secs}s request_deadline={request_deadline_secs}s \
         (env GROK_PROMPT_TIMEOUT_SECS / GROK_REQUEST_DEADLINE_SECS)"
    );

    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("GROK_WS_SECRET", &secret);
    let dump = Arc::new(RawDumpCollector::enabled());

    let connector = GrokConnector::new(secrets);
    let mut limits = GrokConnectorLimits::default();
    limits.request_deadline = Duration::from_secs(request_deadline_secs);
    let mut config = GrokServerConfig::loopback(port, SecretRef::new("GROK_WS_SECRET"))?;
    config.limits = limits;
    let config = config.with_raw_dump(Arc::clone(&dump));

    let pending = connector
        .connect(config)
        .map_err(|e| format!("connect begin: {e}"))?;
    let server = tokio::time::timeout(Duration::from_secs(30), pending.wait())
        .await
        .map_err(|_| {
            "connect timed out — is `grok agent --always-approve serve` running on :2419/ws?"
                .to_string()
        })?
        .map_err(|e| format!("connect failed: {e}"))?;

    println!("connected + initialized");

    let pending_sess = server
        .sessions
        .begin_new(GrokSessionConfig {
            cwd: Some(project.display().to_string()),
            mcp_servers: vec![],
            permission_mode: Some("always-approve".into()),
            agent_profile: None,
            extension_metadata: Some(serde_json::json!({ "yoloMode": true })),
        })
        .map_err(|e| format!("session/new begin: {e}"))?;
    let session = tokio::time::timeout(Duration::from_secs(30), pending_sess.wait())
        .await
        .map_err(|_| "session/new timed out".to_string())?
        .map_err(|e| format!("session/new failed: {e}"))?;

    println!("sessionId={}", session.session_id.as_str());

    let factory = DefaultInterpreterFactory::new();
    let interp = factory.start(StartInterpretation {
        interpretation_id: InterpretationId::generate(),
        connection_id: session.connection_id.clone(),
        external_session_id: Some(session.session_id.clone().into()),
        dialect: DialectBinding::negotiated(DialectDescriptor::acp_json_rpc("1")),
        limits: InterpretationLimits::default(),
    })?;

    let sink = Arc::new(SyncMemorySink::new());
    let renderer = ConsoleRenderer::new(
        ConsoleRendererConfig {
            show_tool_payloads: true,
            max_content_chars: 2000,
            ..Default::default()
        },
        sink.clone() as Arc<dyn ConsoleSink>,
    );

    let output = Arc::clone(&session.output);
    let input = interp.input.clone();
    let drain = tokio::spawn(async move {
        loop {
            match output.receive().await {
                Ok(Some(bytes)) => {
                    if input.push_bytes(bytes).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    });

    // Bounded open-ended analysis: free prose + several reads, not a 20-tool deep dive.
    // Good stress for assembly/projector without multi-hour agent runs.
    let prompt = format!(
        "You are in the Monoloop project at:\n{cwd}\n\n\
         Task: short architecture opinion for a human reviewer.\n\n\
         Hard limits (must follow):\n\
         - Read-only. Do NOT write, create, or delete files.\n\
         - Use at most 8 tool calls total.\n\
         - Only read these if present (pick the most useful, not all):\n\
           README.md, doc/README.md, DECISIONS.md,\n\
           crates/monoloop-contracts/src/lib.rs,\n\
           crates/monoloop-interpreter/src/lib.rs,\n\
           crates/monoloop-loop/src/lib.rs,\n\
           crates/monoloop-testkit/src/lib.rs\n\
         - Skim; do not reread the same file.\n\
         - Finish with a written opinion even if you only used 3 tools.\n\n\
         Final answer (colleague tone, short paragraphs — numbered list optional):\n\
         - What Monoloop is\n\
         - Strength of Connector/Interpreter/Loop split\n\
         - One risk or gap\n\
         - One concrete next step\n",
        cwd = project.display(),
    );

    println!("sending session/prompt (project analysis)…");
    println!("(≤8 tools; hard stop at {prompt_timeout_secs}s then salvage)\n");
    let exchange = session.input.begin_send(EncodedAcpSessionMessage {
        method: "session/prompt".into(),
        params: serde_json::json!({
            "prompt": [
                { "type": "text", "text": prompt }
            ]
        }),
    })?;

    // Outer wait for RPC; on timeout we still salvage streamed events and exit.
    let prompt_outcome =
        tokio::time::timeout(Duration::from_secs(prompt_timeout_secs), exchange.wait()).await;
    let result_note = match &prompt_outcome {
        Ok(Ok(v)) => {
            println!("prompt terminal result: {v}");
            format!("{v}")
        }
        Ok(Err(e)) => {
            eprintln!("prompt RPC error: {e} (salvaging streamed events)");
            format!("error:{e}")
        }
        Err(_) => {
            eprintln!(
                "session/prompt timed out after {prompt_timeout_secs}s (salvaging streamed events)"
            );
            "timeout".into()
        }
    };

    tokio::time::sleep(Duration::from_millis(500)).await;
    session
        .control
        .cancel(monoloop_connector_grok::CancellationReason::CallerRequested);
    let _ = tokio::time::timeout(Duration::from_secs(3), drain).await;
    let _ = interp.input.finish_clean().await;

    let mut events = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), interp.events.recv()).await {
            Ok(Some(ev)) => {
                renderer.render(&ev);
                let done = matches!(ev, InterpreterOutputEvent::Ended(_));
                events.push(ev);
                if done {
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => {
                eprintln!("event drain idle; stopping collection");
                break;
            }
        }
    }
    let result = result_note;

    let mut sequence = String::new();
    sequence.push_str("=== LIVE GROK ANALYZE — CANONICAL EVENT SEQUENCE ===\n");
    sequence.push_str(&format!("sessionId={}\n", session.session_id.as_str()));
    sequence.push_str(&format!("prompt_result={result}\n\n"));
    for (i, ev) in events.iter().enumerate() {
        sequence.push_str(&format!("{:04} {}\n", i, describe_event(ev)));
    }
    sequence.push_str(&format!("\n=== total events: {} ===\n", events.len()));
    sequence.push_str("\n=== TOOL ACTIONS (compressed) ===\n");
    for line in tool_summary(&events) {
        sequence.push_str(&line);
        sequence.push('\n');
    }

    let raw_snap = dump.snapshot();
    std::fs::write(&out_raw, raw_snap.format_text())?;
    std::fs::write(&out_seq, &sequence)?;

    let html = build_html_report(
        &events,
        &HtmlReportParams {
            title: "Live Grok Build analyze — interpretation review".into(),
            show_tool_payloads: true,
            ..Default::default()
        },
    );
    write_html_report(&out_html, &html)?;
    std::fs::write(&out_chat, &html.chat_projection.plain_text)?;

    println!("\n{}", sequence);
    println!(
        "\n--- CHAT PROJECTION ({:?} / {:?}) ---\n{}\n",
        html.chat_projection.strategy,
        html.chat_projection.confidence,
        html.chat_projection.plain_text
    );
    println!("console:\n{}", sink.join());
    println!(
        "\nartifacts:\n  sequence: {}\n  raw dump: {}\n  html:     {}\n  chat:     {}\n  frames:   {} (original_bytes={})\n  sentences={} tools_in_chat={}\n",
        out_seq.display(),
        out_raw.display(),
        out_html.display(),
        out_chat.display(),
        raw_snap.frames.len(),
        raw_snap.total_original_bytes,
        html.sentence_count,
        html.chat_projection
            .lines
            .iter()
            .filter(|l| l.role == monoloop_testkit::ChatRole::Tool)
            .count(),
    );

    Ok(())
}

fn describe_event(ev: &InterpreterOutputEvent) -> String {
    match ev {
        InterpreterOutputEvent::Unit(u) => {
            let s = u.snapshot();
            match &s.unit {
                CanonicalUnit::Text(t) => format!(
                    "TEXT ch={:?} g={} | {}",
                    t.channel,
                    s.unit_generation,
                    truncate(&t.content, 120)
                ),
                CanonicalUnit::Tool(t) => format!(
                    "TOOL action={} name={:?} req={:?} exec={:?} g={} wait={:?} args={}",
                    t.tool_action_id.as_str(),
                    t.tool_name,
                    t.request_state,
                    t.execution_state,
                    s.unit_generation,
                    t.waiting_for,
                    t.request_payload
                        .as_deref()
                        .map(|p| truncate(p, 80))
                        .unwrap_or_else(|| "-".into())
                ),
                CanonicalUnit::Boundary(b) => format!("BOUNDARY {:?}", b.kind),
                CanonicalUnit::Structure(st) => {
                    format!("STRUCTURE {:?} | {}", st.kind, truncate(&st.content, 80))
                }
                CanonicalUnit::Diagnostic(d) => {
                    format!("DIAG {:?} | {}", d.kind, truncate(&d.message, 100))
                }
                CanonicalUnit::Paragraph(p) => format!("PARAGRAPH {:?}", p.kind),
                CanonicalUnit::Usage(u) => format!("USAGE {u:?}"),
            }
        }
        InterpreterOutputEvent::Ended(e) => format!(
            "END {:?} events={} sentences={} unresolved={}",
            e.kind, e.canonical_event_count, e.completed_sentence_count, e.unresolved_text_bytes
        ),
    }
}

fn tool_summary(events: &[InterpreterOutputEvent]) -> Vec<String> {
    use std::collections::BTreeMap;
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for ev in events {
        if let InterpreterOutputEvent::Unit(u) = ev {
            if let CanonicalUnit::Tool(t) = &u.snapshot().unit {
                let id = t.tool_action_id.as_str().to_string();
                map.entry(id).or_default().push(format!(
                    "g{} {:?} name={:?} terminal={:?}",
                    u.snapshot().unit_generation,
                    t.request_state,
                    t.tool_name,
                    t.terminal_outcome
                ));
            }
        }
    }
    map.into_iter()
        .map(|(id, gens)| format!("{id}: {}", gens.join(" → ")))
        .collect()
}

fn truncate(s: &str, max: usize) -> String {
    let t: String = s.chars().take(max).collect();
    if s.chars().count() > max {
        format!("{t}…")
    } else {
        t
    }
}
