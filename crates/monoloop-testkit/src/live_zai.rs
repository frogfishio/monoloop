//! End-to-end live Z.ai CLI driver (`zai -p` headless NDJSON).
//!
//! **Test kit only.** Requires `zai` installed and configured (`zai config` / `ZAI_API_KEY`).

use crate::console::{ConsoleRenderer, ConsoleRendererConfig, SyncMemorySink};
use crate::html_report::{build_html_report, write_html_report, HtmlReport, HtmlReportParams};
use monoloop_connector_zai::{run_headless_prompt, ZaiAgentConfig};
use monoloop_contracts::{
    CanonicalUnit, DialectBinding, DialectDescriptor, InterpretationId, InterpretationLimits,
    InterpreterOutputEvent,
};
use monoloop_interpreter::{DefaultInterpreterFactory, InterpreterFactory, StartInterpretation};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;

/// Configuration for a single live zai headless run.
#[derive(Clone, Debug)]
pub struct LiveZaiRunOptions {
    /// Prompt text.
    pub prompt: String,
    /// Working directory.
    pub cwd: PathBuf,
    /// Process config.
    pub agent: ZaiAgentConfig,
    /// HTML title.
    pub title: String,
    /// Artifact stem.
    pub artifact_stem: PathBuf,
    /// Render console lines while collecting.
    pub render_console: bool,
}

impl LiveZaiRunOptions {
    /// Defaults under `target/live_zai_run`.
    pub fn for_project(project: impl Into<PathBuf>, prompt: impl Into<String>) -> Self {
        let project = project.into();
        let stem = project.join("target/live_zai_run");
        let mut agent = ZaiAgentConfig::for_project(project.clone());
        agent.raw_dump_path = Some(PathBuf::from(format!("{}.raw.txt", stem.display())));
        agent.run_deadline = Duration::from_secs(10 * 60);
        Self {
            prompt: prompt.into(),
            cwd: project,
            agent,
            title: "Live Z.ai CLI — interpretation review".into(),
            artifact_stem: stem,
            render_console: true,
        }
    }
}

/// Artifact paths written by a live run.
#[derive(Clone, Debug)]
pub struct LiveZaiArtifactPaths {
    /// HTML review.
    pub html: PathBuf,
    /// Raw NDJSON dump.
    pub raw: PathBuf,
    /// Sequence summary.
    pub sequence: PathBuf,
    /// Chat projection plain text.
    pub chat: PathBuf,
}

/// Report from a managed live zai run.
#[derive(Clone, Debug)]
pub struct LiveZaiRunReport {
    /// Synthetic session id for the headless run.
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
    pub paths: LiveZaiArtifactPaths,
}

/// Run one prompt against live Z.ai CLI headless and write review artifacts.
pub async fn run_live_zai_prompt(opts: LiveZaiRunOptions) -> Result<LiveZaiRunReport, String> {
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
        async move { run_headless_prompt(&agent, &prompt, tx).await }
    });

    let dialect = DialectBinding::negotiated(DialectDescriptor::zai_cli("1"));
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::generate(),
            connection_id: monoloop_contracts::ConnectionId::new("zai-live"),
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

    let mut sequence_text = String::from("=== LIVE ZAI — CANONICAL TEXT ===\n");
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

    Ok(LiveZaiRunReport {
        session_id: outcome.session_id,
        exit_code: outcome.exit_code,
        events,
        html,
        console_text: sink.join(),
        sequence_text,
        paths: LiveZaiArtifactPaths {
            html: html_path,
            raw: raw_path,
            sequence: seq_path,
            chat: chat_path,
        },
    })
}
