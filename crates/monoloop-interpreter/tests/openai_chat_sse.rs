//! WP-09: OpenAI Chat Completions SSE Interpreter fragmentation suite.

use monoloop_contracts::{
    CanonicalUnit, ConnectionId, DialectBinding, DialectDescriptor, InterpretationEndKind,
    InterpretationId, InterpretationLimits, InterpreterOutputEvent, LoopId, ToolRequestState,
};
use monoloop_interpreter::{DefaultInterpreterFactory, InterpreterFactory, StartInterpretation};
use std::time::Duration;

fn start() -> monoloop_interpreter::Interpretation {
    DefaultInterpreterFactory::new()
        .start(StartInterpretation {
            interpretation_id: InterpretationId::generate(),
            connection_id: ConnectionId::generate(),
            external_session_id: None,
            dialect: DialectBinding::fixed(DialectDescriptor::openai_chat_completions("v1")),
            limits: InterpretationLimits::default(),
        })
        .unwrap()
}

async fn feed_and_finish(chunks: &[&[u8]]) -> (Vec<InterpreterOutputEvent>, InterpretationEndKind) {
    let interp = start();
    for c in chunks {
        interp
            .input
            .push_bytes(bytes::Bytes::copy_from_slice(c))
            .await
            .unwrap();
    }
    interp.input.finish_clean().await.unwrap();
    let mut events = Vec::new();
    while let Some(ev) = interp.events.recv().await {
        match &ev {
            InterpreterOutputEvent::Ended(_) => {
                events.push(ev);
                break;
            }
            _ => events.push(ev),
        }
    }
    let end = interp.completion.wait().await;
    (events, end.kind)
}

#[tokio::test]
async fn text_only_fragmented_bytes() {
    let chunks: &[&[u8]] = &[
        br#"data: {"choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"}}]}"#,
        b"\n\n",
        br#"data: {"choices":[{"index":0,"delta":{"content":" world."},"finish_reason":"stop"}]}"#,
        b"\n\n",
        b"data: [DONE]\n\n",
    ];
    let (events, kind) = feed_and_finish(chunks).await;
    assert_eq!(kind, InterpretationEndKind::Complete);
    let texts: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Text(t) => Some(t.content.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(!texts.is_empty());
    let joined = texts.join("");
    assert!(joined.contains("Hello") || joined.contains("world"));
}

#[tokio::test]
async fn tool_calls_fragmented_args() {
    let chunks: &[&[u8]] = &[
        br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_abc","type":"function","function":{"name":"search","arguments":"{\"q\":"}}]}}]}"#,
        b"\n\n",
        br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"monoloop\"}"}}]}}]}"#,
        b"\n\n",
        br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
        b"\n\n",
        b"data: [DONE]\n\n",
    ];
    let (events, kind) = feed_and_finish(chunks).await;
    assert_eq!(kind, InterpretationEndKind::Complete);
    let ready = events.iter().any(|e| match e {
        InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
            CanonicalUnit::Tool(t) => {
                t.request_state == ToolRequestState::Ready
                    && t.tool_name.as_deref() == Some("search")
                    && t.request_payload
                        .as_ref()
                        .is_some_and(|p| p.contains("monoloop"))
            }
            _ => false,
        },
        _ => false,
    });
    assert!(ready, "expected ToolRequestReady for search");
}

#[tokio::test]
async fn invalid_json_arguments_not_ready() {
    let chunks: &[&[u8]] = &[
        br#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"x","arguments":"{bad"}}]},"finish_reason":"tool_calls"}]}"#,
        b"\n\n",
        b"data: [DONE]\n\n",
    ];
    let (events, kind) = feed_and_finish(chunks).await;
    // D-016: incomplete/invalid args at tool_calls finish must not Ready-execute.
    assert_ne!(kind, InterpretationEndKind::Complete);
    let ready = events.iter().any(|e| match e {
        InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
            CanonicalUnit::Tool(t) => t.request_state == ToolRequestState::Ready,
            _ => false,
        },
        _ => false,
    });
    assert!(!ready);
}

#[tokio::test]
async fn missing_done_fails() {
    let chunks: &[&[u8]] = &[
        br#"data: {"choices":[{"index":0,"delta":{"content":"Hi."},"finish_reason":"stop"}]}"#,
        b"\n\n",
    ];
    let (_events, kind) = feed_and_finish(chunks).await;
    assert_ne!(kind, InterpretationEndKind::Complete);
}

#[tokio::test]
async fn crlf_and_split_across_boundary() {
    // Split mid-line and use CRLF.
    let chunks: &[&[u8]] = &[
        b"da",
        b"ta: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"A.\"}}]}\r\n\r\n",
        b"data: [DONE]\r\n\r\n",
    ];
    let (_events, kind) = feed_and_finish(chunks).await;
    assert_eq!(kind, InterpretationEndKind::Complete);
}

#[tokio::test]
async fn unsupported_finish_reason_fails() {
    let chunks: &[&[u8]] = &[
        br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"function_call"}]}"#,
        b"\n\n",
    ];
    let (_events, kind) = feed_and_finish(chunks).await;
    assert_ne!(kind, InterpretationEndKind::Complete);
}

// silence unused import if LoopId drifts
#[allow(dead_code)]
fn _unused() {
    let _ = Duration::from_secs(1);
    let _ = LoopId::generate();
}
