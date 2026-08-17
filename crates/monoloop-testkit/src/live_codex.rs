//! End-to-end live Codex ACP driver (via `codex-acp` bridge).
//!
//! **Test kit only.** Requires `codex` authenticated and an ACP bridge
//! (`codex-acp` on PATH or `npx @agentclientprotocol/codex-acp`).

use crate::console::{ConsoleRenderer, ConsoleRendererConfig, SyncMemorySink};
use crate::html_report::{build_html_report, write_html_report, HtmlReport, HtmlReportParams};
use monoloop_connector_codex::{CodexAgentConfig, CodexAgentHandle, CodexSessionConfig};
use monoloop_contracts::{
    CanonicalUnit, DialectBinding, DialectDescriptor, InterpretationId, InterpretationLimits,
    InterpreterOutputEvent,
};
use monoloop_interpreter::{DefaultInterpreterFactory, InterpreterFactory, StartInterpretation};
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for a single live codex prompt run.
#[derive(Clone, Debug)]
pub struct LiveCodexRunOptions {
    /// Prompt text.
    pub prompt: String,
    /// Working directory.
    pub cwd: PathBuf,
    /// ACP process config.
    pub agent: CodexAgentConfig,
    /// Session create options.
    pub session: CodexSessionConfig,
    /// HTML title.
    pub title: String,
    /// Artifact stem.
    pub artifact_stem: PathBuf,
    /// Render console lines while collecting.
    pub render_console: bool,
    /// Drain after prompt returns.
    pub drain_after_prompt: Duration,
}

impl LiveCodexRunOptions {
    /// Defaults under `target/live_codex_run`.
    pub fn for_project(project: impl Into<PathBuf>, prompt: impl Into<String>) -> Self {
        let project = project.into();
        let stem = project.join("target/live_codex_run");
        let mut agent = CodexAgentConfig::for_project(project.clone());
        agent.raw_dump_path = Some(PathBuf::from(format!("{}.raw.txt", stem.display())));
        agent.rpc_deadline = Duration::from_secs(10 * 60);
        agent = agent.with_auto_allow_permissions();
        agent.authenticate = false;
        Self {
            prompt: prompt.into(),
            cwd: project.clone(),
            agent,
            session: CodexSessionConfig::new(project),
            title: "Live Codex ACP — interpretation review".into(),
            artifact_stem: stem,
            render_console: true,
            drain_after_prompt: Duration::from_millis(300),
        }
    }
}

/// Artifact paths written by a live run.
#[derive(Clone, Debug)]
pub struct LiveCodexArtifactPaths {
    /// HTML review.
    pub html: PathBuf,
    /// Raw NDJSON dump.
    pub raw: PathBuf,
    /// Sequence summary.
    pub sequence: PathBuf,
    /// Chat projection plain text.
    pub chat: PathBuf,
}

/// Report from a managed live codex run.
#[derive(Clone, Debug)]
pub struct LiveCodexRunReport {
    /// Session id.
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
    pub paths: LiveCodexArtifactPaths,
}

/// Run one prompt against live Codex ACP and write review artifacts.
pub async fn run_live_codex_prompt(
    opts: LiveCodexRunOptions,
) -> Result<LiveCodexRunReport, String> {
    if let Some(parent) = opts.artifact_stem.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }

    let mut agent = CodexAgentHandle::connect(opts.agent.clone())
        .await
        .map_err(|e| e.to_string())?;
    let mut updates = agent.take_updates();
    let mut session_cfg = opts.session.clone();
    session_cfg.cwd = opts.cwd.clone();
    let session = agent
        .session_new(session_cfg)
        .await
        .map_err(|e| e.to_string())?;
    let session_id = session.session_id.clone();

    let dialect = DialectBinding::negotiated(DialectDescriptor::codex_acp("1"));
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::generate(),
            connection_id: monoloop_contracts::ConnectionId::new("codex-live"),
            external_session_id: Some(session.external_session_id()),
            dialect,
            limits: InterpretationLimits::default(),
        })
        .map_err(|e| e.to_string())?;

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

    tokio::time::sleep(opts.drain_after_prompt).await;
    let dump_text = agent.raw_dump_text();
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
    if !dump_text.is_empty() {
        let _ = std::fs::write(&raw_path, &dump_text);
    } else if !raw_path.is_file() {
        let _ = std::fs::write(&raw_path, "");
    }

    let mut sequence_text = String::from("=== LIVE CODEX — CANONICAL TEXT ===\n");
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

    Ok(LiveCodexRunReport {
        session_id,
        prompt_result: prompt_result_s,
        events,
        html,
        console_text: sink.join(),
        sequence_text,
        paths: LiveCodexArtifactPaths {
            html: html_path,
            raw: raw_path,
            sequence: seq_path,
            chat: chat_path,
        },
    })
}
