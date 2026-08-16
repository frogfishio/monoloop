//! Live Codex ACP **tool** exercise → HTML review (Cursor/Agy parity).
//!
//! Uses `@agentclientprotocol/codex-acp` with `session/set_mode` =
//! `agent-full-access` so unattended file tools can complete in a sandbox.
//! Requires `codex` installed and authenticated (`codex login` or API key).
//!
//! ```bash
//! cargo run -p monoloop-testkit --example live_codex_crud
//! open target/live_codex_crud.html
//! ```

use monoloop_connector_codex::CodexAgentConfig;
use monoloop_testkit::{run_live_codex_prompt, LiveCodexRunOptions};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;

    let file = project.join("target/monoloop_codex_crud_test.txt");
    let file_s = file.display().to_string();
    // Clean slate for a deterministic exercise.
    let _ = std::fs::remove_file(&file);

    let prompt = format!(
        "Work only on this one file path: `{file_s}`.\n\
         1. CREATE — write exactly: hello monoloop codex crud\\n\n\
         2. READ — read it back and confirm contents.\n\
         3. UPDATE — overwrite with: hello monoloop codex crud UPDATED\\n\n\
         4. READ — confirm updated contents.\n\
         5. Do NOT delete the file.\n\
         After tools finish, reply with a short numbered summary of what you did. \
         Touch no other paths."
    );

    let mut opts = LiveCodexRunOptions::for_project(&project, prompt);
    opts.artifact_stem = project.join("target/live_codex_crud");
    opts.title = "Live Codex ACP — tool CRUD exercise".into();
    opts.drain_after_prompt = Duration::from_millis(800);
    opts.session = opts.session.with_agent_full_access_mode();
    opts.agent = CodexAgentConfig::for_project(&project)
        .with_raw_dump(project.join("target/live_codex_crud.raw.txt"))
        .with_auto_allow_permissions();
    opts.agent.rpc_deadline = Duration::from_secs(15 * 60);
    opts.agent.authenticate = false;

    println!(
        "spawning codex ACP (cmd={} {:?}, mode=agent-full-access, file={file_s})…",
        opts.agent.command.display(),
        opts.agent.args,
    );
    let report = run_live_codex_prompt(opts).await?;
    println!("sessionId={}", report.session_id);
    println!("prompt_result={}", report.prompt_result);
    println!("events={}", report.events.len());
    println!("sentences={}", report.html.sentence_count);
    println!("strategy={:?}", report.html.chat_projection.strategy);
    println!("confidence={:?}", report.html.chat_projection.confidence);

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
