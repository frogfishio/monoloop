//! Fixture: Codex/codex ACP-shaped NDJSON → Interpreter canonical units.

use monoloop_contracts::{
    CanonicalUnit, DialectBinding, DialectDescriptor, InterpretationId, InterpretationLimits,
    InterpreterOutputEvent, TextChannel,
};
use monoloop_interpreter::{
    ConnectionId, DefaultInterpreterFactory, InterpreterFactory, StartInterpretation,
};

fn codex_dialect() -> DialectBinding {
    DialectBinding::negotiated(DialectDescriptor::codex_acp("1"))
}

async fn run_ndjson(lines: &[&str]) -> Vec<InterpreterOutputEvent> {
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::new("codex-i1"),
            connection_id: ConnectionId::new("codex-c1"),
            external_session_id: Some(monoloop_contracts::ExternalSessionId::new("sess-codex-1")),
            dialect: codex_dialect(),
            limits: InterpretationLimits::default(),
        })
        .unwrap();
    for line in lines {
        interp
            .input
            .push_bytes(bytes::Bytes::from(format!("{line}\n")))
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
async fn codex_message_chunks_assemble_sentence() {
    // Shape observed from codex-acp live probe (messageId optional field).
    let lines = [
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","messageId":"2","content":{"type":"text","text":"Hi there, "}}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","messageId":"2","content":{"type":"text","text":"how can I assist you today?"}}}}"#,
        r#"{"jsonrpc":"2.0","id":11,"result":{"stopReason":"end_turn"}}"#,
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
    assert_eq!(
        texts,
        vec!["Hi there, how can I assist you today?".to_string()]
    );
}

#[tokio::test]
async fn codex_tool_call_lifecycle() {
    let lines = [
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call","toolCallId":"call-1","title":"write_file","rawInput":{"path":"/tmp/x","content":"hi"}}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call-1","title":"Write `/tmp/x`","status":"completed","content":[{"type":"text","text":"ok"}]}}}"#,
        r#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Done."}}}}"#,
        r#"{"jsonrpc":"2.0","id":9,"result":{"stopReason":"end_turn"}}"#,
    ];
    let events = run_ndjson(&lines).await;
    let tools: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Tool(t) => Some((t.request_state, t.terminal_outcome)),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(
        tools
            .iter()
            .any(|(s, _)| *s == monoloop_contracts::ToolRequestState::Ready),
        "{tools:?}"
    );
    assert!(
        tools.iter().any(|(_, t)| {
            *t == Some(monoloop_contracts::ToolTerminalOutcome::Success)
        }),
        "{tools:?}"
    );
}
