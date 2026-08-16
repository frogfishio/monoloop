#![allow(clippy::while_let_loop)]
//! Fixture: Cursor-shaped ACP NDJSON → Interpreter canonical units (no live agent).

use monoloop_contracts::{
    CanonicalUnit, DialectBinding, DialectDescriptor, InterpretationId, InterpretationLimits,
    InterpreterOutputEvent, TextChannel,
};
use monoloop_interpreter::{
    ConnectionId, DefaultInterpreterFactory, InterpreterFactory, StartInterpretation,
};

fn cursor_dialect() -> DialectBinding {
    DialectBinding::negotiated(DialectDescriptor::cursor_acp("1"))
}

async fn run_ndjson(lines: &[&str]) -> Vec<InterpreterOutputEvent> {
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::new("cursor-i1"),
            connection_id: ConnectionId::new("cursor-c1"),
            external_session_id: Some(monoloop_contracts::ExternalSessionId::new("sess-cursor-1")),
            dialect: cursor_dialect(),
            limits: InterpretationLimits::default(),
        })
        .unwrap();
    for line in lines {
        let chunk = format!("{line}\n");
        interp
            .input
            .push_bytes(bytes::Bytes::from(chunk))
            .await
            .unwrap();
    }
    interp.input.finish_clean().await.unwrap();
    let mut out = Vec::new();
    loop {
        match interp.events.recv().await {
            Some(ev) => {
                let done = matches!(ev, InterpreterOutputEvent::Ended(_));
                out.push(ev);
                if done {
                    break;
                }
            }
            None => break,
        }
    }
    out
}

#[tokio::test]
async fn cursor_message_chunks_assemble_sentence() {
    let lines = [
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Hi"}}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":" there."}}}}"#,
        r#"{"jsonrpc":"2.0","id":4,"result":{"stopReason":"end_turn"}}"#,
    ];
    let events = run_ndjson(&lines).await;
    let texts: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Text(t) if t.channel == TextChannel::PublicResponse => {
                    Some(t.content.clone())
                }
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert_eq!(texts, vec!["Hi there.".to_string()]);
}

#[tokio::test]
async fn cursor_tool_call_ready_and_thought_suppressed() {
    let lines = [
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"private cot"}}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call","toolCallId":"call-1","title":"read_file","rawInput":{"path":"/tmp/x"}}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call-1","title":"Read `/tmp/x`","status":"completed","content":[{"type":"text","text":"ok"}]}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Done."}}}}"#,
        r#"{"jsonrpc":"2.0","id":9,"result":{"stopReason":"end_turn"}}"#,
    ];
    let events = run_ndjson(&lines).await;
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
    assert!(
        texts.iter().all(|t| !t.contains("private cot")),
        "private thought must not escape: {texts:?}"
    );
    assert!(texts.iter().any(|t| t.contains("Done.")));
    let tools: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Tool(t) => Some((
                    t.tool_name.clone(),
                    t.request_state,
                    t.terminal_outcome,
                )),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(
        tools.iter().any(|(n, st, _)| {
            n.as_deref() == Some("read_file")
                || n.as_ref().is_some_and(|s| s.starts_with("Read"))
                    && *st == monoloop_contracts::ToolRequestState::Ready
        }),
        "tool ready: {tools:?}"
    );
    assert!(
        tools.iter().any(|(_, _, term)| {
            *term == Some(monoloop_contracts::ToolTerminalOutcome::Success)
        }),
        "tool terminal: {tools:?}"
    );
}

#[tokio::test]
async fn cursor_fragmented_ndjson_invariant() {
    let full = concat!(
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Hello world."}}}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}"#,
        "\n",
    );
    let bytes = full.as_bytes();
    let mid = bytes.len() / 3;
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::new("frag"),
            connection_id: ConnectionId::new("c"),
            external_session_id: None,
            dialect: cursor_dialect(),
            limits: InterpretationLimits::default(),
        })
        .unwrap();
    for chunk in [&bytes[..mid], &bytes[mid..mid * 2], &bytes[mid * 2..]] {
        interp
            .input
            .push_bytes(bytes::Bytes::copy_from_slice(chunk))
            .await
            .unwrap();
    }
    interp.input.finish_clean().await.unwrap();
    let mut texts = Vec::new();
    loop {
        match interp.events.recv().await {
            Some(InterpreterOutputEvent::Unit(u)) => {
                if let CanonicalUnit::Text(t) = &u.snapshot().unit {
                    texts.push(t.content.clone());
                }
            }
            Some(InterpreterOutputEvent::Ended(_)) | None => break,
        }
    }
    assert_eq!(texts, vec!["Hello world.".to_string()]);
}
