//! Live Claude Code **tool** exercise → HTML review.
//!
//! Uses `--dangerously-skip-permissions` so unattended file tools complete.
//!
//! ```bash
//! cargo run -p monoloop-testkit --example live_claude_crud
//! open target/live_claude_crud.html
//! ```

use monoloop_connector_claude::ClaudeAgentConfig;
use monoloop_testkit::{run_live_claude_prompt, LiveClaudeRunOptions};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;

    let file = project.join("target/monoloop_claude_crud_test.txt");
    let file_s = file.display().to_string();
    let _ = std::fs::remove_file(&file);

    let prompt = format!(
        "Work only on this one file path: `{file_s}`.\n\
         1. CREATE — write exactly: hello monoloop claude crud\\n\n\
         2. READ — read it back and confirm contents.\n\
         3. UPDATE — overwrite with: hello monoloop claude crud UPDATED\\n\n\
         4. READ — confirm updated contents.\n\
         5. Do NOT delete the file.\n\
         After tools finish, reply with a short numbered summary of what you did. \
         Touch no other paths."
    );

    let mut opts = LiveClaudeRunOptions::for_project(&project, prompt);
    opts.artifact_stem = project.join("target/live_claude_crud");
    opts.title = "Live Claude Code — tool CRUD exercise".into();
    opts.agent = ClaudeAgentConfig::for_project(&project)
        .with_raw_dump(project.join("target/live_claude_crud.raw.txt"))
        .with_skip_permissions();
    opts.agent.run_deadline = Duration::from_secs(15 * 60);

    println!(
        "spawning claude print (cmd={}, skip_permissions, file={file_s})…",
        opts.agent.command.display(),
    );
    let report = run_live_claude_prompt(opts).await?;
    println!("sessionId={}", report.session_id);
    println!("exit_code={:?}", report.exit_code);
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
