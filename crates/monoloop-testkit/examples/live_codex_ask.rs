//! Live Codex ACP smoke (via `codex-acp` bridge) → HTML review.
//!
//! ```bash
//! # requires: codex installed + logged in; codex-acp on PATH or network for npx
//! cargo run -p monoloop-testkit --example live_codex_ask
//! open target/live_codex_ask.html
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
    let prompt = std::env::args().nth(1).unwrap_or_else(|| {
        "In one short sentence, say hello and name the monoloop project purpose \
         (three-component async kernel). Do not use tools."
            .into()
    });

    let mut opts = LiveCodexRunOptions::for_project(&project, prompt);
    opts.artifact_stem = project.join("target/live_codex_ask");
    opts.title = "Live Codex ACP — ask smoke".into();
    opts.agent = CodexAgentConfig {
        rpc_deadline: Duration::from_secs(5 * 60),
        raw_dump_path: Some(project.join("target/live_codex_ask.raw.txt")),
        auto_allow_permissions: true,
        authenticate: false,
        ..CodexAgentConfig::for_project(&project)
    };

    println!(
        "spawning codex ACP bridge (cmd={} {:?}, cwd={})…",
        opts.agent.command.display(),
        opts.agent.args,
        opts.cwd.display()
    );
    let report = run_live_codex_prompt(opts).await?;
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
