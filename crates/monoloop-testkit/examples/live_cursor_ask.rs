//! Live Cursor ACP smoke: short **ask-mode** prompt → HTML review.
//!
//! ```bash
//! # requires: agent on PATH, `agent login` or CURSOR_API_KEY
//! cargo run -p monoloop-testkit --example live_cursor_ask
//! open target/live_cursor_ask.html
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
    let prompt = std::env::args().nth(1).unwrap_or_else(|| {
        "In one short sentence, say hello and name the monoloop project purpose \
         (three-component async kernel). Do not use tools."
            .into()
    });

    let mut opts = LiveCursorRunOptions::for_project(&project, prompt).with_ask_mode();
    opts.artifact_stem = project.join("target/live_cursor_ask");
    opts.title = "Live Cursor ACP — ask smoke".into();
    opts.agent = CursorAgentConfig {
        rpc_deadline: Duration::from_secs(5 * 60),
        raw_dump_path: Some(project.join("target/live_cursor_ask.raw.txt")),
        auto_allow_permissions: true,
        ..CursorAgentConfig::for_project(&project)
    };

    println!(
        "spawning cursor agent acp (cwd={}, mode=ask)…",
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
    println!("--- chat projection ---");
    println!("{}", report.html.chat_projection.plain_text);
    Ok(())
}
