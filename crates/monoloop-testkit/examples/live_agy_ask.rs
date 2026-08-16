#![allow(clippy::field_reassign_with_default, clippy::while_let_loop, dead_code)]
//! Live Antigravity ACP smoke (via `agy-acp` bridge) → HTML review.
//!
//! ```bash
//! # requires: agy installed + logged in; agy-acp on PATH or network for npx
//! cargo run -p monoloop-testkit --example live_agy_ask
//! open target/live_agy_ask.html
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
    let prompt = std::env::args().nth(1).unwrap_or_else(|| {
        "In one short sentence, say hello and name the monoloop project purpose \
         (three-component async kernel). Do not use tools."
            .into()
    });

    let mut opts = LiveAgyRunOptions::for_project(&project, prompt);
    opts.artifact_stem = project.join("target/live_agy_ask");
    opts.title = "Live Antigravity ACP — ask smoke".into();
    opts.agent = AgyAgentConfig {
        rpc_deadline: Duration::from_secs(5 * 60),
        raw_dump_path: Some(project.join("target/live_agy_ask.raw.txt")),
        auto_allow_permissions: true,
        authenticate: false,
        ..AgyAgentConfig::for_project(&project)
    };

    println!(
        "spawning agy ACP bridge (cmd={} {:?}, cwd={})…",
        opts.agent.command.display(),
        opts.agent.args,
        opts.cwd.display()
    );
    let report = run_live_agy_prompt(opts).await?;
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
