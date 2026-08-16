#![allow(clippy::field_reassign_with_default, clippy::while_let_loop, dead_code)]
//! Live Claude Code smoke (`stream-json`) → HTML review.
//!
//! ```bash
//! cargo run -p monoloop-testkit --example live_claude_ask
//! open target/live_claude_ask.html
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
    let prompt = std::env::args().nth(1).unwrap_or_else(|| {
        "In one short sentence, say hello and name the monoloop project purpose \
         (three-component async kernel). Do not use tools."
            .into()
    });

    let mut opts = LiveClaudeRunOptions::for_project(&project, prompt);
    opts.artifact_stem = project.join("target/live_claude_ask");
    opts.title = "Live Claude Code — ask smoke".into();
    opts.agent = ClaudeAgentConfig {
        run_deadline: Duration::from_secs(5 * 60),
        raw_dump_path: Some(project.join("target/live_claude_ask.raw.txt")),
        ..ClaudeAgentConfig::for_project(&project)
    };

    println!(
        "spawning claude print (cmd={}, cwd={})…",
        opts.agent.command.display(),
        opts.cwd.display()
    );
    let report = run_live_claude_prompt(opts).await?;
    println!("sessionId={}", report.session_id);
    println!("exit_code={:?}", report.exit_code);
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
