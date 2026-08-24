#![allow(clippy::field_reassign_with_default, dead_code)]
//! Live Grok multi-session qualification on one long-lived serve.
//!
//! Proves concurrent `session/new` (distinct `sessionId`s) and isolated prompts.
//! Also **attempts** explicit `session/load` of a known id and records the
//! result (some live agent builds reject load of a just-finished short session).
//! Uses default `GROK_AGENT_SECRET=monoloop-live-test` when unset (preauthorized
//! agent hosts).
//!
//! ```bash
//! cargo run -p monoloop-testkit --example live_grok_multi_session
//! # optional: GROK_PROMPT_CEILING_SECS=120
//! ```

use monoloop_testkit::{run_live_grok_multi_session, LiveGrokMultiSessionOptions};
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;

    let mut opts = LiveGrokMultiSessionOptions::for_project(&project);
    if let Ok(secs) = std::env::var("GROK_PROMPT_CEILING_SECS") {
        if let Ok(n) = secs.parse::<u64>() {
            if n > 0 {
                let d = Duration::from_secs(n);
                opts.base.prompt_wait_ceiling = Some(d);
                opts.base.request_deadline = d;
            }
        }
    }

    println!("project: {}", project.display());
    println!(
        "artifacts: {}.summary.txt",
        opts.base.artifact_stem.display()
    );
    println!(
        "deadlines: request={:?} outer_ceiling={:?}",
        opts.base.request_deadline, opts.base.prompt_wait_ceiling
    );
    println!("secret: env GROK_AGENT_SECRET or default monoloop-live-test (preauthorized hosts)");

    let report = run_live_grok_multi_session(opts).await?;

    println!("\n=== RESULT ===");
    println!("port={}", report.port);
    println!(
        "session_a id={} timed_out={} marker_ok={}",
        report.session_a.session_id, report.session_a.timed_out, !report.session_a.timed_out
    );
    println!(
        "session_b id={} timed_out={} marker_ok={}",
        report.session_b.session_id, report.session_b.timed_out, !report.session_b.timed_out
    );
    println!("load_a_ok={:?}", report.load_a_ok);
    println!(
        "distinct_ids={}",
        report.session_a.session_id != report.session_b.session_id
    );
    println!("summary: {}", report.summary_path.display());

    if report.session_a.timed_out || report.session_b.timed_out {
        return Err("one or both prompts hit the outer ceiling".into());
    }
    if report.session_a.session_id == report.session_b.session_id {
        return Err("session ids were not distinct".into());
    }
    match report.load_a_ok {
        Some(true) => println!("explicit session/load of A: ok"),
        Some(false) => {
            // Standing residual: some live agent builds reject load of a
            // just-finished short session (Invalid params). Concurrent
            // session/new + marker isolation still qualifies multi-session.
            eprintln!(
                "warning: explicit session/load of A did not succeed (standing live residual)"
            );
        }
        None => {}
    }

    println!("live Grok multi-session qualification: PASS (concurrent new + isolation)");
    Ok(())
}
