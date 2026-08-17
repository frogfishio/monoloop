#![allow(clippy::field_reassign_with_default, clippy::while_let_loop, dead_code)]
//! Managed live Grok: driver starts serve, runs one prompt, stops serve.
//!
//! ```bash
//! # Default architecture analysis (waits for Grok; owns serve lifecycle)
//! cargo run -p monoloop-testkit --example live_grok_ask
//!
//! # Custom prompt
//! cargo run -p monoloop-testkit --example live_grok_ask -- "Summarise monoloop-loop in 5 bullets"
//!
//! # Presets
//! cargo run -p monoloop-testkit --example live_grok_ask -- --preset analyze
//! cargo run -p monoloop-testkit --example live_grok_ask -- --preset crud
//!
//! # Optional safety ceiling (seconds). Default: none (wait until Grok finishes,
//! # subject to 2h connector request_deadline).
//! GROK_PROMPT_CEILING_SECS=3600 cargo run -p monoloop-testkit --example live_grok_ask
//! ```
//!
//! Artifacts under `target/live_grok_ask.*` (or `target/live_grok_{preset}.*`).

use monoloop_testkit::{run_live_grok_prompt, LiveGrokRunOptions};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let (preset, prompt) = parse_args(&args, &project)?;

    let stem = match preset.as_deref() {
        Some("crud") => project.join("target/live_grok_crud"),
        Some("analyze") => project.join("target/live_grok_analyze"),
        _ => project.join("target/live_grok_ask"),
    };

    let mut opts = LiveGrokRunOptions::for_project(&project, prompt);
    opts.artifact_stem = stem;
    opts.title = match preset.as_deref() {
        Some("crud") => "Live Grok Build CRUD — managed".into(),
        Some("analyze") => "Live Grok Build analyze — managed".into(),
        _ => "Live Grok Build ask — managed".into(),
    };

    if let Ok(secs) = std::env::var("GROK_PROMPT_CEILING_SECS") {
        if let Ok(n) = secs.parse::<u64>() {
            if n > 0 {
                opts.prompt_wait_ceiling = Some(Duration::from_secs(n));
            }
        }
    }
    if let Ok(secs) = std::env::var("GROK_REQUEST_DEADLINE_SECS") {
        if let Ok(n) = secs.parse::<u64>() {
            if n > 0 {
                opts.request_deadline = Duration::from_secs(n);
            }
        }
    }

    println!("project: {}", project.display());
    println!(
        "artifacts: {}.{{html,raw.txt,sequence.txt,chat.txt}}",
        opts.artifact_stem.display()
    );
    println!(
        "deadlines: request={:?} outer_ceiling={:?}",
        opts.request_deadline, opts.prompt_wait_ceiling
    );

    let report = run_live_grok_prompt(opts).await?;

    println!("\n{}", report.sequence_text);
    println!(
        "\n--- CHAT PROJECTION ({:?} / {:?}) ---\n{}\n",
        report.html.chat_projection.strategy,
        report.html.chat_projection.confidence,
        report.html.chat_projection.plain_text
    );
    if !report.console_text.is_empty() {
        println!("console:\n{}", report.console_text);
    }
    println!(
        "\nartifacts:\n  sequence: {}\n  raw:      {}\n  html:     {}\n  chat:     {}\n  port:     {}\n  timed_out:{}\n  frames:   {} (original_bytes={})\n  events:  {} sentences={}\n",
        report.paths.sequence.display(),
        report.paths.raw.display(),
        report.paths.html.display(),
        report.paths.chat.display(),
        report.port,
        report.timed_out,
        report.raw.frames.len(),
        report.raw.total_original_bytes,
        report.events.len(),
        report.html.sentence_count,
    );

    Ok(())
}

fn parse_args(
    args: &[String],
    project: &std::path::Path,
) -> Result<(Option<String>, String), String> {
    if args.is_empty() {
        return Ok((Some("analyze".into()), analyze_prompt(project)));
    }
    if args[0] == "--preset" {
        let name = args
            .get(1)
            .ok_or_else(|| "usage: --preset analyze|crud".to_string())?
            .as_str();
        let prompt = match name {
            "analyze" => analyze_prompt(project),
            "crud" => crud_prompt(project),
            other => return Err(format!("unknown preset: {other}")),
        };
        return Ok((Some(name.into()), prompt));
    }
    // Remaining args = free-form prompt
    Ok((None, args.join(" ")))
}

fn analyze_prompt(project: &std::path::Path) -> String {
    format!(
        "You are in the Monoloop project at:\n{cwd}\n\n\
         Task: short architecture opinion for a human reviewer.\n\n\
         Hard limits:\n\
         - Read-only. Do NOT write, create, or delete files.\n\
         - Prefer at most 8 tool calls.\n\
         - Useful paths: README.md, DECISIONS.md,\n\
           crates/monoloop-contracts/src/lib.rs,\n\
           crates/monoloop-interpreter/src/lib.rs,\n\
           crates/monoloop-loop/src/lib.rs,\n\
           crates/monoloop-testkit/src/lib.rs\n\
         - Skim; do not reread the same file.\n\
         - Always finish with a written opinion.\n\n\
         Cover: what Monoloop is; strength of Connector/Interpreter/Loop;\n\
         one risk; one concrete next step. Colleague tone.",
        cwd = project.display(),
    )
}

fn crud_prompt(project: &std::path::Path) -> String {
    let file = project.join("monoloop_live_crud_test.txt");
    format!(
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
        file = file.display(),
    )
}
