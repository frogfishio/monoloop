//! End-to-end live Cursor ACP driver: **spawn agent → session → prompt → collect → stop**.
//!
//! **Test kit only.** Requires Cursor CLI (`agent`) authenticated (`agent login`
//! or `CURSOR_API_KEY`).

use crate::console::{ConsoleRenderer, ConsoleRendererConfig, SyncMemorySink};
use crate::html_report::{build_html_report, write_html_report, HtmlReport, HtmlReportParams};
use monoloop_connector_cursor::{CursorAgentConfig, CursorAgentHandle, CursorSessionConfig};
use monoloop_contracts::{
    CanonicalUnit, DialectBinding, DialectDescriptor, InterpretationId, InterpretationLimits,
    InterpreterOutputEvent,
};
use monoloop_interpreter::{DefaultInterpreterFactory, InterpreterFactory, StartInterpretation};
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for a single live Cursor prompt run.
#[derive(Clone, Debug)]
pub struct LiveCursorRunOptions {
    /// Prompt text.
    pub prompt: String,
    /// Working directory for the session.
    pub cwd: PathBuf,
    /// Agent process config.
    pub agent: CursorAgentConfig,
    /// HTML title.
    pub title: String,
    /// Artifact stem (`{stem}.html`, `.raw.txt`, `.sequence.txt`, `.chat.txt`).
    pub artifact_stem: PathBuf,
    /// When true, print console lines while collecting.
    pub render_console: bool,
    /// Outer ceiling on collecting after prompt returns (drain late updates).
    pub drain_after_prompt: Duration,
}

impl LiveCursorRunOptions {
    /// Defaults under `target/live_cursor_run` for a project root.
    pub fn for_project(project: impl Into<PathBuf>, prompt: impl Into<String>) -> Self {
        let project = project.into();
        let stem = project.join("target/live_cursor_run");
        let mut agent = CursorAgentConfig::for_project(project.clone());
        agent.raw_dump_path = Some(PathBuf::from(format!("{}.raw.txt", stem.display())));
        agent.rpc_deadline = Duration::from_secs(10 * 60);
        agent.auto_allow_permissions = true;
        Self {
            prompt: prompt.into(),
            cwd: project,
            agent,
            title: "Live Cursor ACP — interpretation review".into(),
            artifact_stem: stem,
            render_console: true,
            drain_after_prompt: Duration::from_millis(200),
        }
    }
}

/// Artifact paths written by a live Cursor run.
#[derive(Clone, Debug)]
pub struct LiveCursorArtifactPaths {
    /// HTML review page.
    pub html: PathBuf,
    /// Raw NDJSON dump.
    pub raw: PathBuf,
    /// Sequence summary.
    pub sequence: PathBuf,
    /// Chat projection plain text.
    pub chat: PathBuf,
}

/// Report from a managed live Cursor run.
#[derive(Clone, Debug)]
pub struct LiveCursorRunReport {
    /// Cursor sessionId.
    pub session_id: String,
    /// Prompt RPC result JSON.
    pub prompt_result: String,
    /// Interpreter events.
    pub events: Vec<InterpreterOutputEvent>,
    /// HTML review.
    pub html: HtmlReport,
    /// Console text.
    pub console_text: String,
    /// Sequence summary.
    pub sequence_text: String,
    /// Paths written.
    pub paths: LiveCursorArtifactPaths,
}

/// Run one prompt against a live Cursor ACP agent and write review artifacts.
pub async fn run_live_cursor_prompt(
    opts: LiveCursorRunOptions,
) -> Result<LiveCursorRunReport, String> {
    if let Some(parent) = opts.artifact_stem.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }

    let mut agent = CursorAgentHandle::connect(opts.agent.clone())
        .await
        .map_err(|e| e.to_string())?;
    let mut updates = agent.take_updates();
    let session = agent
        .session_new(CursorSessionConfig::new(&opts.cwd))
        .await
        .map_err(|e| e.to_string())?;
    let session_id = session.session_id.clone();

    let dialect = DialectBinding::negotiated(DialectDescriptor::cursor_acp("1"));
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::generate(),
            connection_id: monoloop_contracts::ConnectionId::new("cursor-live"),
            external_session_id: Some(session.external_session_id()),
            dialect,
            limits: InterpretationLimits::default(),
        })
        .map_err(|e| e.to_string())?;

    // Feed session/update NDJSON into the Interpreter while the prompt runs.
    let input = interp.input.clone();
    let pump = tokio::spawn(async move {
        while let Some(bytes) = updates.recv().await {
            if input.push_bytes(bytes).await.is_err() {
                break;
            }
        }
    });

    let prompt_result = session
        .prompt_text(&opts.prompt)
        .await
        .map_err(|e| e.to_string())?;
    let prompt_result_s = prompt_result.to_string();

    // Brief drain for trailing updates after stopReason.
    tokio::time::sleep(opts.drain_after_prompt).await;
    // Dropping agent/session after cancel of pump: finish interpretation.
    // Abort update pump by shutting down agent (closes stdout).
    // First finish clean so segmenter seals.
    let _ = interp.input.finish_clean().await;
    agent.shutdown().await;
    let _ = pump.await;

    let mut events = Vec::new();
    let sink = std::sync::Arc::new(SyncMemorySink::new());
    let console = ConsoleRenderer::new(ConsoleRendererConfig::default(), sink.clone());
    loop {
        match interp.events.recv().await {
            Some(ev) => {
                if opts.render_console {
                    console.render(&ev);
                }
                let done = matches!(ev, InterpreterOutputEvent::Ended(_));
                events.push(ev);
                if done {
                    break;
                }
            }
            None => break,
        }
    }

    let html = build_html_report(
        &events,
        &HtmlReportParams {
            title: opts.title.clone(),
            ..HtmlReportParams::default()
        },
    );
    let html_path = PathBuf::from(format!("{}.html", opts.artifact_stem.display()));
    write_html_report(&html_path, &html).map_err(|e| e.to_string())?;

    let raw_path = PathBuf::from(format!("{}.raw.txt", opts.artifact_stem.display()));
    // dump already written live if configured; refresh from handle if empty
    if !raw_path.is_file() {
        let _ = std::fs::write(&raw_path, "");
    }

    let mut sequence_text = String::from("=== LIVE CURSOR — CANONICAL TEXT ===\n");
    for (i, e) in events.iter().enumerate() {
        if let InterpreterOutputEvent::Unit(u) = e {
            if let CanonicalUnit::Text(t) = &u.snapshot().unit {
                sequence_text.push_str(&format!("{i:04} | {}\n", t.content));
            }
        }
    }
    sequence_text.push_str(&format!(
        "\nsessionId={session_id}\nprompt_result={prompt_result_s}\n"
    ));
    let seq_path = PathBuf::from(format!("{}.sequence.txt", opts.artifact_stem.display()));
    std::fs::write(&seq_path, &sequence_text).map_err(|e| e.to_string())?;

    let chat_path = PathBuf::from(format!("{}.chat.txt", opts.artifact_stem.display()));
    std::fs::write(&chat_path, &html.chat_projection.plain_text).map_err(|e| e.to_string())?;

    let console_text = sink.join();

    Ok(LiveCursorRunReport {
        session_id,
        prompt_result: prompt_result_s,
        events,
        html,
        console_text,
        sequence_text,
        paths: LiveCursorArtifactPaths {
            html: html_path,
            raw: raw_path,
            sequence: seq_path,
            chat: chat_path,
        },
    })
}
