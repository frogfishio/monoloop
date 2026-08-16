#![allow(clippy::field_reassign_with_default, clippy::while_let_loop, dead_code)]
//! Live Antigravity ACP **tool** exercise → HTML review (Cursor parity).
//!
//! Uses the `agy-acp` bridge with `--dangerously-skip-permissions` so unattended
//! file tools can complete. Requires `agy` installed and authenticated.
//!
//! ```bash
//! cargo run -p monoloop-testkit --example live_agy_crud
//! open target/live_agy_crud.html
//! ```

use monoloop_connector_agy::AgyAgentConfig;
use monoloop_testkit::{run_live_agy_prompt, LiveAgyRunOptions};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;

    let file = project.join("target/monoloop_agy_crud_test.txt");
    let file_s = file.display().to_string();
    // Clean slate for a deterministic exercise.
    let _ = std::fs::remove_file(&file);

    let prompt = format!(
        "Work only on this one file path: `{file_s}`.\n\
         1. CREATE — write exactly: hello monoloop agy crud\\n\n\
         2. READ — read it back and confirm contents.\n\
         3. UPDATE — overwrite with: hello monoloop agy crud UPDATED\\n\n\
         4. READ — confirm updated contents.\n\
         5. Do NOT delete the file.\n\
         After tools finish, reply with a short numbered summary of what you did. \
         Touch no other paths."
    );

    let mut opts = LiveAgyRunOptions::for_project(&project, prompt);
    opts.artifact_stem = project.join("target/live_agy_crud");
    opts.title = "Live Antigravity ACP — tool CRUD exercise".into();
    opts.drain_after_prompt = Duration::from_millis(800);
    opts.session = opts.session.with_accept_edits_mode();
    opts.agent = AgyAgentConfig::for_project(&project)
        .with_raw_dump(project.join("target/live_agy_crud.raw.txt"))
        .with_skip_permissions();
    opts.agent.rpc_deadline = Duration::from_secs(15 * 60);
    opts.agent.authenticate = false;

    println!(
        "spawning agy ACP (cmd={} {:?}, mode=accept-edits, file={file_s})…",
        opts.agent.command.display(),
        opts.agent.args,
    );
    let report = run_live_agy_prompt(opts).await?;
    println!("sessionId={}", report.session_id);
    println!("prompt_result={}", report.prompt_result);
    println!("events={}", report.events.len());
    println!("sentences={}", report.html.sentence_count);
    println!("strategy={:?}", report.html.chat_projection.strategy);
    println!("confidence={:?}", report.html.chat_projection.confidence);

    // Tool unit count for parity checks.
    let tool_events = report
        .events
        .iter()
        .filter(|e| {
            matches!(
                e,
                monoloop_contracts::InterpreterOutputEvent::Unit(u)
                    if matches!(
                        u.snapshot().unit,
                        monoloop_contracts::CanonicalUnit::Tool(_)
                    )
            )
        })
        .count();
    println!("tool_unit_events={tool_events}");
    println!("html={}", report.paths.html.display());
    println!("chat={}", report.paths.chat.display());
    println!("raw={}", report.paths.raw.display());
    if file.is_file() {
        let body = std::fs::read_to_string(&file).unwrap_or_default();
        println!(
            "file left at {} ({} bytes): {:?}",
            file.display(),
            body.len(),
            body
        );
    } else {
        println!("note: expected file not found at {}", file.display());
    }
    println!("--- chat projection ---");
    println!("{}", report.html.chat_projection.plain_text);
    Ok(())
}
