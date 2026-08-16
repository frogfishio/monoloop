//! Live Z.ai CLI **tool** exercise → HTML review.
//!
//! Headless `zai -p` auto-approves tools inside the CLI. Monoloop observes the
//! OpenAI-chat NDJSON transcript (tool_calls + tool results + final text).
//!
//! ```bash
//! cargo run -p monoloop-testkit --example live_zai_crud
//! open target/live_zai_crud.html
//! ```

use monoloop_connector_zai::ZaiAgentConfig;
use monoloop_testkit::{run_live_zai_prompt, LiveZaiRunOptions};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;

    let file = project.join("target/monoloop_zai_crud_test.txt");
    let file_s = file.display().to_string();
    let _ = std::fs::remove_file(&file);

    let prompt = format!(
        "Work only on this one file path: `{file_s}`.\n\
         1. CREATE — write exactly: hello monoloop zai crud\\n\n\
         2. READ — read it back and confirm contents.\n\
         3. UPDATE — overwrite with: hello monoloop zai crud UPDATED\\n\n\
         4. READ — confirm updated contents.\n\
         5. Do NOT delete the file.\n\
         After tools finish, reply with a short numbered summary of what you did. \
         Touch no other paths."
    );

    let mut opts = LiveZaiRunOptions::for_project(&project, prompt);
    opts.artifact_stem = project.join("target/live_zai_crud");
    opts.title = "Live Z.ai CLI — tool CRUD exercise".into();
    opts.agent = ZaiAgentConfig::for_project(&project)
        .with_raw_dump(project.join("target/live_zai_crud.raw.txt"));
    opts.agent.run_deadline = Duration::from_secs(15 * 60);
    opts.agent.max_tool_rounds = 80;

    println!(
        "spawning zai headless (cmd={}, file={file_s})…",
        opts.agent.command.display(),
    );
    let report = run_live_zai_prompt(opts).await?;
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
