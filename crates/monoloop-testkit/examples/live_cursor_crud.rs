#![allow(clippy::field_reassign_with_default, clippy::while_let_loop, dead_code)]
//! Live Cursor ACP **agent-mode** tool exercise → HTML review.
//!
//! Asks Cursor to create/read/update a single temp file under `target/`, then
//! summarize. Permissions auto-allow for unattended capture.
//!
//! ```bash
//! cargo run -p monoloop-testkit --example live_cursor_crud
//! open target/live_cursor_crud.html
//! ```

use monoloop_connector_cursor::CursorAgentConfig;
use monoloop_testkit::{run_live_cursor_prompt, LiveCursorRunOptions};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;

    let file = project.join("target/monoloop_cursor_crud_test.txt");
    let file_s = file.display().to_string();

    let prompt = format!(
        "Work only on this one file path: `{file_s}`.\n\
         1. CREATE — write exactly: hello monoloop cursor crud\\n\n\
         2. READ — read it back and confirm contents.\n\
         3. UPDATE — overwrite with: hello monoloop cursor crud UPDATED\\n\n\
         4. READ — confirm updated contents.\n\
         5. Do NOT delete the file.\n\
         After tools finish, reply with a short numbered summary of what you did. \
         Touch no other paths."
    );

    let mut opts = LiveCursorRunOptions::for_project(&project, prompt).with_agent_mode();
    opts.artifact_stem = project.join("target/live_cursor_crud");
    opts.title = "Live Cursor ACP — tool CRUD exercise".into();
    opts.drain_after_prompt = Duration::from_millis(500);
    opts.agent = CursorAgentConfig {
        rpc_deadline: Duration::from_secs(15 * 60),
        raw_dump_path: Some(project.join("target/live_cursor_crud.raw.txt")),
        auto_allow_permissions: true,
        advertise_fs: false,
        ..CursorAgentConfig::for_project(&project)
    };

    println!(
        "spawning cursor agent acp (cwd={}, mode=agent, file={file_s})…",
        opts.cwd.display()
    );
    let report = run_live_cursor_prompt(opts).await?;
    println!("sessionId={}", report.session_id);
    println!("prompt_result={}", report.prompt_result);
    println!("events={}", report.events.len());
    println!("sentences={}", report.html.sentence_count);
    println!("strategy={:?}", report.html.chat_projection.strategy);
    println!("confidence={:?}", report.html.chat_projection.confidence);
    println!("html={}", report.paths.html.display());
    println!("chat={}", report.paths.chat.display());
    println!("raw={}", report.paths.raw.display());
    if file.is_file() {
        println!(
            "file left at {} ({} bytes)",
            file.display(),
            std::fs::metadata(&file).map(|m| m.len()).unwrap_or(0)
        );
    } else {
        println!("note: expected file not found at {}", file.display());
    }
    println!("--- chat projection ---");
    println!("{}", report.html.chat_projection.plain_text);
    Ok(())
}
