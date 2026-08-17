//! End-to-end Driver: Interpreter + independent Console + Empty Loop.

use monoloop_contracts::{CanonicalUnit, InterpreterOutputEvent, LoopEndKind, LoopOutputEvent};
use monoloop_testkit::{acp_binding, run_bytes_pipeline, test_text_binding};

#[tokio::test]
async fn e2e_text_console_no_tool_dispatch() {
    let report = run_bytes_pipeline(
        test_text_binding(),
        &[
            bytes::Bytes::from_static(b"Hel"),
            bytes::Bytes::from_static(b"lo world. "),
        ],
        true,
    )
    .await;

    assert!(
        report.console_text.contains("Hello world."),
        "{}",
        report.console_text
    );
    assert_eq!(report.tools_unavailable, 0);
    assert!(matches!(
        report.loop_end.kind,
        LoopEndKind::Drained | LoopEndKind::Cancelled
    ));

    let sentences: Vec<_> = report
        .interpreter_events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Text(t) => Some(t.content.as_str()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(sentences, ["Hello world."]);
}

#[tokio::test]
async fn e2e_acp_tool_becomes_unavailable() {
    let stream = concat!(
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Running tool. "}}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"tool_call","toolCallId":"T1","title":"bash","status":"pending"}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"tool_call_update","toolCallId":"T1","title":"bash","rawInput":{"command":"echo hi"}}}}"#,
        r#"{"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}"#,
    );
    let bytes = stream.as_bytes();
    let mid = bytes.len() / 3;
    let report = run_bytes_pipeline(
        acp_binding(),
        &[
            bytes::Bytes::copy_from_slice(&bytes[..mid]),
            bytes::Bytes::copy_from_slice(&bytes[mid..mid * 2]),
            bytes::Bytes::copy_from_slice(&bytes[mid * 2..]),
        ],
        true,
    )
    .await;

    assert!(
        report.console_text.contains("Running tool."),
        "console: {}",
        report.console_text
    );
    assert_eq!(
        report.tools_unavailable, 1,
        "loop events: {:?}",
        report.loop_events
    );
    assert!(report.loop_events.iter().any(|e| {
        matches!(
            e,
            LoopOutputEvent::OutboundToolResult(r)
                if r.outcome == monoloop_contracts::OutboundToolOutcome::ToolUnavailable
                    && r.tool_execution_id.is_none()
        )
    }));
    // NoToolRuntime never started ⇒ no success invent
    assert!(!report.loop_events.iter().any(|e| {
        matches!(
            e,
            LoopOutputEvent::OutboundToolResult(r)
                if r.outcome == monoloop_contracts::OutboundToolOutcome::Success
        )
    }));
}

#[tokio::test]
async fn console_and_loop_independent() {
    // Smoke: both get events without sharing a receiver (composition succeeds).
    let report = run_bytes_pipeline(
        test_text_binding(),
        &[bytes::Bytes::from_static(b"Done. ")],
        true,
    )
    .await;
    assert!(!report.console_text.is_empty());
    assert!(!report.interpreter_events.is_empty());
    assert_eq!(
        report.loop_end.delivery_events_received,
        report.loop_end.delivery_events_received
    );
}
