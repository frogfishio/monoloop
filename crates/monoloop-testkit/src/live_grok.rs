//! End-to-end live Grok driver: **start serve → session → prompt → collect → stop**.
//!
//! **Test kit only.** Owns the Grok child process for the duration of the run.
//! The client waits for Grok's natural `session/prompt` completion (no short
//! artificial hang for the operator); an optional safety ceiling may still be set.

use crate::console::{ConsoleRenderer, ConsoleRendererConfig, ConsoleSink, SyncMemorySink};
use crate::grok_serve::{GrokServeOptions, ManagedGrokServe};
use crate::html_report::{build_html_report, write_html_report, HtmlReport, HtmlReportParams};
use monoloop_connector_grok::{
    EncodedAcpSessionMessage, GrokConnector, GrokConnectorLimits, GrokServerConfig,
    GrokSessionConfig, InMemorySecretResolver, RawDumpCollector, RawDumpSnapshot, SecretRef,
};
use monoloop_contracts::{
    CanonicalUnit, DialectBinding, DialectDescriptor, InterpretationId, InterpretationLimits,
    InterpreterOutputEvent,
};
use monoloop_interpreter::{DefaultInterpreterFactory, InterpreterFactory, StartInterpretation};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for a single live Grok prompt run.
#[derive(Clone, Debug)]
pub struct LiveGrokRunOptions {
    /// Prompt text sent via `session/prompt`.
    pub prompt: String,
    /// Working directory for the Grok session (`cwd`).
    pub cwd: PathBuf,
    /// Serve options (port, secret, log path, ready timeout).
    pub serve: GrokServeOptions,
    /// Title used in the HTML review page.
    pub title: String,
    /// Artifact stem directory + base name (without extension).
    /// Writes `{stem}.html`, `{stem}.raw.txt`, `{stem}.sequence.txt`, `{stem}.chat.txt`.
    pub artifact_stem: PathBuf,
    /// How long the Connector may wait for a single JSON-RPC (including the prompt).
    /// Default: 2 hours — long enough for real agent work; still fail-closed.
    pub request_deadline: Duration,
    /// Optional outer ceiling on waiting for prompt completion.
    /// `None` = wait until the RPC finishes or `request_deadline` fires.
    pub prompt_wait_ceiling: Option<Duration>,
    /// Connect / session-open timeout.
    pub connect_timeout: Duration,
    /// When true, render console lines while collecting.
    pub render_console: bool,
}

impl LiveGrokRunOptions {
    /// Sensible defaults for a repo-root live capture under `target/`.
    pub fn for_project(project: impl Into<PathBuf>, prompt: impl Into<String>) -> Self {
        let project = project.into();
        let log = project.join("target/grok-serve.managed.log");
        Self {
            prompt: prompt.into(),
            cwd: project.clone(),
            serve: GrokServeOptions {
                port: None, // ephemeral — avoids clashing with a leftover serve
                secret: std::env::var("GROK_AGENT_SECRET")
                    .unwrap_or_else(|_| "monoloop-live-test".into()),
                grok_bin: PathBuf::from(
                    std::env::var("GROK_BIN").unwrap_or_else(|_| "grok".into()),
                ),
                ready_timeout: Duration::from_secs(15),
                log_path: Some(log),
            },
            title: "Live Grok Build — interpretation review".into(),
            artifact_stem: project.join("target/live_grok_run"),
            request_deadline: Duration::from_secs(2 * 60 * 60),
            prompt_wait_ceiling: None,
            connect_timeout: Duration::from_secs(30),
            render_console: true,
        }
    }
}

/// Full report from a managed live run.
#[derive(Clone, Debug)]
pub struct LiveGrokRunReport {
    /// Grok `sessionId` string.
    pub session_id: String,
    /// Terminal JSON-RPC result body (or timeout/error note).
    pub prompt_result: String,
    /// Whether the prompt wait hit the optional outer ceiling.
    pub timed_out: bool,
    /// Canonical interpreter events (including `Ended` when available).
    pub events: Vec<InterpreterOutputEvent>,
    /// HTML review (always built).
    pub html: HtmlReport,
    /// Raw dump snapshot (may be empty if capture failed early).
    pub raw: RawDumpSnapshot,
    /// Append-only console text.
    pub console_text: String,
    /// Human sequence summary.
    pub sequence_text: String,
    /// Written artifact paths.
    pub paths: LiveGrokArtifactPaths,
    /// Port the managed serve used.
    pub port: u16,
}

/// Paths written by the live driver.
#[derive(Clone, Debug)]
pub struct LiveGrokArtifactPaths {
    /// HTML review.
    pub html: PathBuf,
    /// Raw wire dump.
    pub raw: PathBuf,
    /// Event sequence summary.
    pub sequence: PathBuf,
    /// Chat projection plain text.
    pub chat: PathBuf,
}

/// Start Grok serve, run one prompt to completion, tear everything down.
///
/// Cleanup is guaranteed: serve is stopped even if the prompt fails.
pub async fn run_live_grok_prompt(opts: LiveGrokRunOptions) -> Result<LiveGrokRunReport, String> {
    if let Some(parent) = opts.artifact_stem.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create artifact dir {}: {e}", parent.display()))?;
        }
    }

    println!(
        "live-grok: starting managed serve (ready ≤ {:?})…",
        opts.serve.ready_timeout
    );
    let serve = ManagedGrokServe::start(opts.serve.clone()).await?;
    let port = serve.port();
    let secret = serve.secret().to_string();
    println!("live-grok: serve up pid={:?} port={port}", serve.pid());

    let result = run_session_with_serve(&opts, &serve, &secret, port).await;

    println!("live-grok: stopping serve…");
    if let Err(e) = serve.stop().await {
        eprintln!("live-grok: serve stop warning: {e}");
    } else {
        println!("live-grok: serve stopped");
    }

    result
}

async fn run_session_with_serve(
    opts: &LiveGrokRunOptions,
    _serve: &ManagedGrokServe,
    secret: &str,
    port: u16,
) -> Result<LiveGrokRunReport, String> {
    let secrets = Arc::new(InMemorySecretResolver::new());
    secrets.insert("GROK_WS_SECRET", secret);
    let dump = Arc::new(RawDumpCollector::enabled());

    let mut limits = GrokConnectorLimits::default();
    limits.request_deadline = opts.request_deadline;
    limits.connect_deadline = opts.connect_timeout;

    let mut config = GrokServerConfig::loopback(port, SecretRef::new("GROK_WS_SECRET"))
        .map_err(|e| format!("server config: {e}"))?;
    config.limits = limits;
    let config = config.with_raw_dump(Arc::clone(&dump));

    let connector = GrokConnector::new(secrets);
    println!("live-grok: connecting ws://127.0.0.1:{port}/ws …");
    let pending = connector
        .connect(config)
        .map_err(|e| format!("connect begin: {e}"))?;
    let server = tokio::time::timeout(opts.connect_timeout, pending.wait())
        .await
        .map_err(|_| "connect timed out".to_string())?
        .map_err(|e| format!("connect failed: {e}"))?;
    println!("live-grok: connected + initialized");

    let pending_sess = server
        .sessions
        .begin_new(GrokSessionConfig {
            cwd: Some(opts.cwd.display().to_string()),
            mcp_servers: vec![],
            permission_mode: Some("always-approve".into()),
            agent_profile: None,
            extension_metadata: Some(serde_json::json!({ "yoloMode": true })),
        })
        .map_err(|e| format!("session/new begin: {e}"))?;
    let session = tokio::time::timeout(opts.connect_timeout, pending_sess.wait())
        .await
        .map_err(|_| "session/new timed out".to_string())?
        .map_err(|e| format!("session/new failed: {e}"))?;
    let session_id = session.session_id.as_str().to_string();
    println!("live-grok: sessionId={session_id}");

    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::generate(),
            connection_id: session.connection_id.clone(),
            external_session_id: Some(session.session_id.clone().into()),
            dialect: DialectBinding::negotiated(DialectDescriptor::acp_json_rpc("1")),
            limits: InterpretationLimits::default(),
        })
        .map_err(|e| format!("start interpretation: {e}"))?;

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

    println!(
        "live-grok: session/prompt (request_deadline={:?}, outer_ceiling={:?})…",
        opts.request_deadline, opts.prompt_wait_ceiling
    );
    let exchange = session
        .input
        .begin_send(EncodedAcpSessionMessage {
            method: "session/prompt".into(),
            params: serde_json::json!({
                "prompt": [
                    { "type": "text", "text": opts.prompt }
                ]
            }),
        })
        .map_err(|e| format!("begin_send: {e}"))?;

    let (prompt_result, timed_out) = match opts.prompt_wait_ceiling {
        Some(ceiling) => match tokio::time::timeout(ceiling, exchange.wait()).await {
            Ok(Ok(v)) => {
                println!("live-grok: prompt complete");
                (format!("{v}"), false)
            }
            Ok(Err(e)) => (format!("error:{e}"), false),
            Err(_) => {
                eprintln!(
                    "live-grok: outer ceiling {:?} hit — salvaging streamed events",
                    ceiling
                );
                ("timeout".into(), true)
            }
        },
        None => match exchange.wait().await {
            Ok(v) => {
                println!("live-grok: prompt complete");
                (format!("{v}"), false)
            }
            Err(e) => (format!("error:{e}"), false),
        },
    };

    // Brief settle for trailing session/update frames.
    tokio::time::sleep(Duration::from_millis(400)).await;
    session
        .control
        .cancel(monoloop_connector_grok::CancellationReason::CallerRequested);
    let _ = tokio::time::timeout(Duration::from_secs(3), drain).await;
    let _ = interp.input.finish_clean().await;

    let mut events = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), interp.events.recv()).await {
            Ok(Some(ev)) => {
                if opts.render_console {
                    renderer.render(&ev);
                }
                let done = matches!(ev, InterpreterOutputEvent::Ended(_));
                events.push(ev);
                if done {
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let raw = dump.snapshot();
    let html = build_html_report(
        &events,
        &HtmlReportParams {
            title: opts.title.clone(),
            show_tool_payloads: true,
            ..Default::default()
        },
    );

    let sequence_text = format_sequence(&session_id, &prompt_result, &events);
    let paths = LiveGrokArtifactPaths {
        html: PathBuf::from(format!("{}.html", opts.artifact_stem.display())),
        raw: PathBuf::from(format!("{}.raw.txt", opts.artifact_stem.display())),
        sequence: PathBuf::from(format!("{}.sequence.txt", opts.artifact_stem.display())),
        chat: PathBuf::from(format!("{}.chat.txt", opts.artifact_stem.display())),
    };

    std::fs::write(&paths.raw, raw.format_text()).map_err(|e| format!("write raw: {e}"))?;
    std::fs::write(&paths.sequence, &sequence_text).map_err(|e| format!("write sequence: {e}"))?;
    write_html_report(&paths.html, &html).map_err(|e| format!("write html: {e}"))?;
    std::fs::write(&paths.chat, &html.chat_projection.plain_text)
        .map_err(|e| format!("write chat: {e}"))?;

    Ok(LiveGrokRunReport {
        session_id,
        prompt_result,
        timed_out,
        events,
        html,
        raw,
        console_text: sink.join(),
        sequence_text,
        paths,
        port,
    })
}

fn format_sequence(
    session_id: &str,
    prompt_result: &str,
    events: &[InterpreterOutputEvent],
) -> String {
    let mut sequence = String::new();
    sequence.push_str("=== LIVE GROK MANAGED RUN — CANONICAL EVENT SEQUENCE ===\n");
    sequence.push_str(&format!("sessionId={session_id}\n"));
    sequence.push_str(&format!("prompt_result={prompt_result}\n\n"));
    for (i, ev) in events.iter().enumerate() {
        sequence.push_str(&format!("{:04} {}\n", i, describe_event(ev)));
    }
    sequence.push_str(&format!("\n=== total events: {} ===\n", events.len()));
    sequence.push_str("\n=== TOOL ACTIONS (compressed) ===\n");
    for line in tool_summary(events) {
        sequence.push_str(&line);
        sequence.push('\n');
    }
    sequence
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
