#![allow(clippy::field_reassign_with_default, clippy::while_let_loop, dead_code)]
//! Live Grok Build session: ask the agent to CRUD a test file, capture events.
//!
//! Prerequisites — **serve detached** (do not block a parent agent on `grok serve`):
//! ```bash
//! ./scripts/grok-serve-detached.sh
//! export GROK_AGENT_SECRET=monoloop-live-test
//! cargo run -p monoloop-testkit --example live_grok_crud
//! open target/live_grok_crud.html
//! ./scripts/grok-serve-stop.sh
//! ```
//!
//! Optional: `GROK_PROMPT_TIMEOUT_SECS` (default 180) bounds `session/prompt`.

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

    let out_html = project.join("target/live_grok_crud.html");
    let out_raw = project.join("target/live_grok_crud.raw.txt");
    let out_seq = project.join("target/live_grok_crud.sequence.txt");
    let test_file = project.join("monoloop_live_crud_test.txt");

    println!("project cwd: {}", project.display());
    println!("connecting to ws://127.0.0.1:{port} …");

    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("GROK_WS_SECRET", &secret);
    let dump = Arc::new(RawDumpCollector::enabled());

    let prompt_timeout_secs: u64 = std::env::var("GROK_PROMPT_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(180);
    let request_deadline_secs = prompt_timeout_secs.saturating_add(15);

    let connector = GrokConnector::new(secrets);
    let mut limits = GrokConnectorLimits::default();
    limits.request_deadline = Duration::from_secs(request_deadline_secs);
    let mut config = GrokServerConfig::loopback(port, SecretRef::new("GROK_WS_SECRET"))?;
    config.limits = limits;
    let config = config.with_raw_dump(Arc::clone(&dump));

    println!("timeouts: prompt={prompt_timeout_secs}s request_deadline={request_deadline_secs}s");

    let pending = connector
        .connect(config)
        .map_err(|e| format!("connect begin: {e}"))?;
    let server = tokio::time::timeout(Duration::from_secs(30), pending.opened)
        .await
        .map_err(|_| {
            "connect timed out — start serve with ./scripts/grok-serve-detached.sh first"
                .to_string()
        })?
        .map_err(|e| format!("connect channel: {e}"))?
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
    let session = tokio::time::timeout(Duration::from_secs(30), pending_sess.opened)
        .await
        .map_err(|_| "session/new timed out".to_string())?
        .map_err(|e| format!("session channel: {e}"))?
        .map_err(|e| format!("session/new failed: {e}"))?;

    println!("sessionId={}", session.session_id.as_str());

    // Interpretation over session output (ACP dialect).
    let factory = DefaultInterpreterFactory::new();
    let interp = factory.start(StartInterpretation {
        interpretation_id: InterpretationId::generate(),
        connection_id: session.connection_id.clone(),
        external_session_id: Some(session.session_id.clone().into_external()),
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

    // Drain session.output → interpreter until cancelled after prompt completes.
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

    let prompt = format!(
        "You are in project directory: {cwd}\n\
         Perform a small CRUD exercise on ONLY this file path (create if missing):\n\
         {file}\n\n\
         Steps (do all of them with tools, in order):\n\
         1. CREATE: write the file with exact content: hello monoloop crud\n\
         2. READ: read the file back\n\
         3. UPDATE: overwrite the file with: hello monoloop crud UPDATED\n\
         4. READ: read the file again\n\
         5. DELETE: delete the file\n\n\
         After tools finish, reply with a short summary of each step. Do not touch any other files.",
        cwd = project.display(),
        file = test_file.display(),
    );

    println!("sending session/prompt (CRUD)…");
    let exchange = session.input.begin_send(EncodedAcpSessionMessage {
        method: "session/prompt".into(),
        params: serde_json::json!({
            "prompt": [
                { "type": "text", "text": prompt }
            ]
        }),
    })?;

    // Bounded wait: real model + tools, but never hang an operator forever.
    let result =
        match tokio::time::timeout(Duration::from_secs(prompt_timeout_secs), exchange.response)
            .await
        {
            Ok(Ok(Ok(v))) => v,
            Ok(Ok(Err(e))) => return Err(format!("session/prompt failed: {e}").into()),
            Ok(Err(_)) => return Err("prompt response channel dropped".into()),
            Err(_) => {
                return Err(format!(
                    "session/prompt timed out after {prompt_timeout_secs}s \
                 (raise GROK_PROMPT_TIMEOUT_SECS if needed)"
                )
                .into());
            }
        };

    println!("prompt terminal result: {result}");

    // Give trailing updates a moment, then finish interpretation.
    tokio::time::sleep(Duration::from_millis(800)).await;
    session
        .control
        .cancel(monoloop_connector_grok::CancellationReason::CallerRequested);
    // Drain may block if output channel stays open; bound wait.
    let _ = tokio::time::timeout(Duration::from_secs(3), drain).await;
    let _ = interp.input.finish_clean().await;

    // Collect interpreter events
    let mut events = Vec::new();
    loop {
        match interp.events.recv().await {
            Some(ev) => {
                renderer.render(&ev);
                let done = matches!(ev, InterpreterOutputEvent::Ended(_));
                events.push(ev);
                if done {
                    break;
                }
            }
            None => break,
        }
    }

    // Sequence summary
    let mut sequence = String::new();
    sequence.push_str("=== LIVE GROK CRUD — CANONICAL EVENT SEQUENCE ===\n");
    sequence.push_str(&format!("sessionId={}\n", session.session_id.as_str()));
    sequence.push_str(&format!("prompt_result={result}\n\n"));
    for (i, ev) in events.iter().enumerate() {
        sequence.push_str(&format!("{:04} {}\n", i, describe_event(ev)));
    }
    sequence.push_str(&format!("\n=== total events: {} ===\n", events.len()));

    // Tool lifecycle compressed view
    sequence.push_str("\n=== TOOL ACTIONS (compressed) ===\n");
    for line in tool_summary(&events) {
        sequence.push_str(&line);
        sequence.push('\n');
    }

    // File existence after run (should be deleted if CRUD completed)
    sequence.push_str("\n=== FS CHECK ===\n");
    sequence.push_str(&format!(
        "test file {} exists? {}\n",
        test_file.display(),
        test_file.exists()
    ));

    let raw_snap = dump.snapshot();
    let raw_text = raw_snap.format_text();
    std::fs::write(&out_raw, &raw_text)?;
    std::fs::write(&out_seq, &sequence)?;

    let html = build_html_report(
        &events,
        &HtmlReportParams {
            title: "Live Grok Build CRUD — interpretation review".into(),
            show_tool_payloads: true,
            ..Default::default()
        },
    );
    write_html_report(&out_html, &html)?;

    println!("\n{}", sequence);
    println!("console:\n{}", sink.join());
    println!(
        "\nartifacts:\n  sequence: {}\n  raw dump: {}\n  html:     {}\n  frames:   {} (original_bytes={})\n",
        out_seq.display(),
        out_raw.display(),
        out_html.display(),
        raw_snap.frames.len(),
        raw_snap.total_original_bytes,
    );

    if raw_snap.frames.is_empty() {
        eprintln!("warning: raw dump empty — no inbound frames captured");
    }

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

// re-export helper for ExternalSessionId conversion
use monoloop_contracts::GrokSessionId;
trait IntoExternal {
    fn into_external(self) -> monoloop_contracts::ExternalSessionId;
}
impl IntoExternal for GrokSessionId {
    fn into_external(self) -> monoloop_contracts::ExternalSessionId {
        self.into()
    }
}
