//! Write `target/interpretation.html` from a synthetic ACP stream.
//!
//! ```bash
//! cargo run -p monoloop-testkit --example write_html_review
//! open target/interpretation.html   # macOS
//! ```

use monoloop_testkit::{acp_binding, run_bytes_pipeline_with_params, PipelineParams};
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let out = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/interpretation.html");

    // Valid complete JSON-RPC messages, then split mid-stream for fragmentation.
    let messages = [
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "demo",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": "Hello **world**. " }
                }
            }
        }),
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "demo",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {
                        "type": "text",
                        "text": "Here is a second complete sentence with `code`. "
                    }
                }
            }
        }),
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "demo",
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "T1",
                    "title": "bash",
                    "status": "pending"
                }
            }
        }),
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "demo",
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": "T1",
                    "title": "bash",
                    "status": "pending",
                    "rawInput": { "command": "echo hi" }
                }
            }
        }),
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "stopReason": "end_turn" }
        }),
    ];

    let mut stream = Vec::new();
    for m in &messages {
        stream.extend_from_slice(&serde_json::to_vec(m).expect("json"));
    }

    let mid = stream.len() / 2;
    let chunks = [
        bytes::Bytes::copy_from_slice(&stream[..mid]),
        bytes::Bytes::copy_from_slice(&stream[mid..]),
    ];

    let report = run_bytes_pipeline_with_params(
        acp_binding(),
        &chunks,
        PipelineParams::with_raw_and_html(&out),
    )
    .await;

    println!("console (append-only):\n{}", report.console_text);
    if let Some(raw) = &report.raw_dump {
        println!("{}", raw.format_text());
    }
    let path = report
        .html_dump_path
        .clone()
        .unwrap_or(out)
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("target/interpretation.html"));
    println!(
        "HTML review written to {}\n  sentences={} timeline_rows={}",
        path.display(),
        report
            .html_report
            .as_ref()
            .map(|h| h.sentence_count)
            .unwrap_or(0),
        report
            .html_report
            .as_ref()
            .map(|h| h.timeline_rows)
            .unwrap_or(0),
    );
    println!("Open that file in a browser to verify interpretation serialisation.");
}
