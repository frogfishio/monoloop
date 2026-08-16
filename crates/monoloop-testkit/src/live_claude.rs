//! End-to-end live Claude Code driver (`claude -p --output-format stream-json`).
//!
//! **Test kit only.** Requires `claude` installed and authenticated.

use crate::console::{ConsoleRenderer, ConsoleRendererConfig, SyncMemorySink};
use crate::html_report::{build_html_report, write_html_report, HtmlReport, HtmlReportParams};
use monoloop_connector_claude::{run_claude_print, ClaudeAgentConfig};
use monoloop_contracts::{
    CanonicalUnit, DialectBinding, DialectDescriptor, ExternalSessionId, InterpretationId,
    InterpretationLimits, InterpreterOutputEvent,
};
use monoloop_interpreter::{DefaultInterpreterFactory, InterpreterFactory, StartInterpretation};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;

/// Configuration for a single live Claude print run.
#[derive(Clone, Debug)]
pub struct LiveClaudeRunOptions {
    /// Prompt text.
    pub prompt: String,
    /// Working directory.
    pub cwd: PathBuf,
    /// Process config.
    pub agent: ClaudeAgentConfig,
    /// HTML title.
    pub title: String,
    /// Artifact stem.
    pub artifact_stem: PathBuf,
    /// Render console lines while collecting.
    pub render_console: bool,
}

impl LiveClaudeRunOptions {
    /// Defaults under `target/live_claude_run`.
    pub fn for_project(project: impl Into<PathBuf>, prompt: impl Into<String>) -> Self {
        let project = project.into();
        let stem = project.join("target/live_claude_run");
        let mut agent = ClaudeAgentConfig::for_project(project.clone());
        agent.raw_dump_path = Some(PathBuf::from(format!("{}.raw.txt", stem.display())));
        agent.run_deadline = Duration::from_secs(15 * 60);
        Self {
            prompt: prompt.into(),
            cwd: project,
            agent,
            title: "Live Claude Code — interpretation review".into(),
            artifact_stem: stem,
            render_console: true,
        }
    }
}

/// Artifact paths written by a live run.
#[derive(Clone, Debug)]
pub struct LiveClaudeArtifactPaths {
    /// HTML review.
    pub html: PathBuf,
    /// Raw stream-json dump.
    pub raw: PathBuf,
    /// Sequence summary.
    pub sequence: PathBuf,
    /// Chat projection plain text.
    pub chat: PathBuf,
}

/// Report from a managed live Claude run.
#[derive(Clone, Debug)]
pub struct LiveClaudeRunReport {
    /// Claude session id from stream init.
    pub session_id: String,
    /// Process exit code.
    pub exit_code: Option<i32>,
    /// Interpreter events.
    pub events: Vec<InterpreterOutputEvent>,
    /// HTML review.
    pub html: HtmlReport,
    /// Console text.
    pub console_text: String,
    /// Sequence summary.
    pub sequence_text: String,
    /// Paths written.
    pub paths: LiveClaudeArtifactPaths,
}

/// Run one prompt against live Claude Code print mode and write review artifacts.
pub async fn run_live_claude_prompt(
    opts: LiveClaudeRunOptions,
) -> Result<LiveClaudeRunReport, String> {
    if let Some(parent) = opts.artifact_stem.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }

    let mut agent = opts.agent.clone();
    agent.cwd = opts.cwd.clone();
    agent.raw_dump_path = Some(PathBuf::from(format!(
        "{}.raw.txt",
        opts.artifact_stem.display()
    )));

    let (tx, mut updates) = mpsc::channel(256);
    let run = tokio::spawn({
        let agent = agent.clone();
        let prompt = opts.prompt.clone();
        async move { run_claude_print(&agent, &prompt, tx).await }
    });

    let dialect = DialectBinding::negotiated(DialectDescriptor::claude_code("1"));
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::generate(),
            connection_id: monoloop_contracts::ConnectionId::new("claude-live"),
            external_session_id: None,
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

    let outcome = run
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    let _ = pump.await;

    // Attach external session id post-hoc is not needed for interpretation already started;
    // session id is reported on the outcome for artifacts.
    let _ = ExternalSessionId::new(outcome.session_id.clone());

    let _ = interp.input.finish_clean().await;

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
    if !outcome.raw_dump_text.is_empty() {
        let _ = std::fs::write(&raw_path, &outcome.raw_dump_text);
    }

    let mut sequence_text = String::from("=== LIVE CLAUDE — CANONICAL TEXT ===\n");
    for (i, ev) in events.iter().enumerate() {
        if let InterpreterOutputEvent::Unit(u) = ev {
            if let CanonicalUnit::Text(t) = &u.snapshot().unit {
                sequence_text.push_str(&format!("{i:04} | {}\n", t.content));
            }
        }
    }
    sequence_text.push_str(&format!(
        "\nsession={} exit={:?} sentences={} strategy={:?} confidence={:?}\n",
        outcome.session_id,
        outcome.exit_code,
        html.sentence_count,
        html.chat_projection.strategy,
        html.chat_projection.confidence
    ));
    let seq_path = PathBuf::from(format!("{}.sequence.txt", opts.artifact_stem.display()));
    let _ = std::fs::write(&seq_path, &sequence_text);

    let chat_path = PathBuf::from(format!("{}.chat.txt", opts.artifact_stem.display()));
    let _ = std::fs::write(&chat_path, &html.chat_projection.plain_text);

    Ok(LiveClaudeRunReport {
        session_id: outcome.session_id,
        exit_code: outcome.exit_code,
        events,
        html,
        console_text: sink.join(),
        sequence_text,
        paths: LiveClaudeArtifactPaths {
            html: html_path,
            raw: raw_path,
            sequence: seq_path,
            chat: chat_path,
        },
    })
}
