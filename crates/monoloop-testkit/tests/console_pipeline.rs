//! Console renderer + interpreter pipeline (test kit only).

use monoloop_contracts::{CanonicalUnit, InterpreterOutputEvent};
use monoloop_testkit::{
    acp_binding, collect_interpretation, feed_chunks, interpret_and_render, test_text_binding,
    ConsoleRenderRecord, ConsoleRenderer, ConsoleRendererConfig, SyncMemorySink,
};
use std::sync::Arc;

#[tokio::test]
async fn console_prints_complete_sentences_not_tokens() {
    let (events, text) = interpret_and_render(
        test_text_binding(),
        &[
            bytes::Bytes::from_static(b"Hel"),
            bytes::Bytes::from_static(b"lo world. "),
            bytes::Bytes::from_static(b"More text!"),
        ],
    )
    .await;

    assert!(
        !text.contains("text/complete assistant Hel\n"),
        "must not print partial tokens: {text}"
    );
    assert!(text.contains("Hello world."), "{text}");
    assert!(text.contains("More text!"), "{text}");
    assert!(text.contains("interpretation/"), "{text}");

    let sentences: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Text(t) => Some(t.content.as_str()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(sentences, ["Hello world.", "More text!"]);
}

#[tokio::test]
async fn console_renders_acp_session_stream() {
    let msg = concat!(
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-9","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Working on it. "}}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-9","update":{"sessionUpdate":"tool_call","toolCallId":"tc1","title":"bash","status":"pending"}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-9","update":{"sessionUpdate":"tool_call_update","toolCallId":"tc1","title":"bash","status":"pending","rawInput":{"command":"ls"}}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-9","update":{"sessionUpdate":"tool_call_update","toolCallId":"tc1","status":"completed","rawOutput":{"ok":true}}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-9","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Done."}}}}"#,
        r#"{"jsonrpc":"2.0","id":7,"result":{"stopReason":"end_turn"}}"#,
    );

    // fragment mid-JSON
    let bytes = msg.as_bytes();
    let mid = bytes.len() / 2;
    let (events, text) = interpret_and_render(
        acp_binding(),
        &[
            bytes::Bytes::copy_from_slice(&bytes[..mid]),
            bytes::Bytes::copy_from_slice(&bytes[mid..]),
        ],
    )
    .await;

    assert!(text.contains("Working on it."), "{text}");
    assert!(
        text.contains("tool/waiting") || text.contains("tool/ready"),
        "{text}"
    );
    assert!(text.contains("bash"), "{text}");
    assert!(text.contains("Done."), "{text}");
    assert!(text.contains("interpretation/Complete"), "{text}");

    // Ensure tool ready exposed complete args only once ready
    let mut saw_ready = false;
    for e in &events {
        if let InterpreterOutputEvent::Unit(u) = e {
            if let CanonicalUnit::Tool(t) = &u.snapshot().unit {
                if t.request_state == monoloop_contracts::ToolRequestState::Ready {
                    saw_ready = true;
                    assert!(t.request_payload.as_ref().unwrap().contains("command"));
                }
                if t.request_state == monoloop_contracts::ToolRequestState::Assembling {
                    assert!(t.request_payload.is_none());
                }
            }
        }
    }
    assert!(saw_ready);
}

#[tokio::test]
async fn append_only_preserves_tool_generations() {
    let events = feed_chunks(
        acp_binding(),
        &[bytes::Bytes::from(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"tool_call","toolCallId":"T1","title":"read_file","status":"pending"}}}"#.as_bytes().to_vec(),
        ),
        bytes::Bytes::from(
            r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"tool_call_update","toolCallId":"T1","title":"read_file","rawInput":{"path":"a"}}}}"#.as_bytes().to_vec(),
        )],
        None,
    )
    .await;

    let sink = Arc::new(SyncMemorySink::new());
    let renderer = ConsoleRenderer::new(ConsoleRendererConfig::default(), sink.clone());
    let mut records: Vec<ConsoleRenderRecord> = Vec::new();
    for ev in &events {
        if matches!(
            ev,
            InterpreterOutputEvent::Unit(u) if matches!(u.snapshot().unit, CanonicalUnit::Tool(_))
        ) {
            records.push(renderer.render(ev));
        }
    }
    assert!(records.len() >= 2, "waiting + ready generations");
    assert!(records[0].line.contains("g:1"), "{}", records[0].line);
    assert!(
        records.iter().any(|r| r.line.contains("g:2")),
        "{records:?}"
    );
}

#[tokio::test]
async fn collect_helper_ends_cleanly() {
    use monoloop_interpreter::{
        ConnectionId, DefaultInterpreterFactory, InterpretationId, InterpretationLimits,
        InterpreterFactory, StartInterpretation,
    };
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::new("x"),
            connection_id: ConnectionId::new("y"),
            external_session_id: None,
            dialect: test_text_binding(),
            limits: InterpretationLimits::default(),
        })
        .unwrap();
    interp
        .input
        .push_bytes(bytes::Bytes::from_static(b"Ok. "))
        .await
        .unwrap();
    interp.input.finish_clean().await.unwrap();
    let events = collect_interpretation(&interp).await;
    assert!(matches!(
        events.last(),
        Some(InterpreterOutputEvent::Ended(_))
    ));
}
