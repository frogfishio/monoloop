//! Interpreter fragmentation, sentence, and ACP mapping tests.

use monoloop_contracts::{
    CanonicalUnit, CanonicalUnitEvent, DialectBinding, DialectDescriptor, InterpretationEndKind,
    InterpreterOutputEvent, TextChannel, ToolRequestState, UnitState,
};
use monoloop_interpreter::{
    ConnectionId, DefaultInterpreterFactory, InterpretationId, InterpretationLimits,
    InterpreterFactory, StartInterpretation,
};

fn acp() -> DialectBinding {
    DialectBinding::negotiated(DialectDescriptor::acp_json_rpc("1"))
}

fn test_d() -> DialectBinding {
    DialectBinding::fixed(DialectDescriptor::test_raw())
}

async fn run_chunks(dialect: DialectBinding, chunks: &[&[u8]]) -> Vec<InterpreterOutputEvent> {
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::new("i1"),
            connection_id: ConnectionId::new("c1"),
            external_session_id: None,
            dialect,
            limits: InterpretationLimits::default(),
        })
        .unwrap();
    for c in chunks {
        interp
            .input
            .push_bytes(bytes::Bytes::copy_from_slice(c))
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

fn text_contents(events: &[InterpreterOutputEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(s)) => match &s.unit {
                CanonicalUnit::Text(t) => Some(t.content.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn fragmentation_invariant_plain_text() {
    let full = b"Hello world. Next sentence!";
    let a = run_chunks(test_d(), &[full]).await;
    let b = run_chunks(
        test_d(),
        &[b"Hel", b"lo wor", b"ld. Ne", b"xt sentence!"],
    )
    .await;
    let c = run_chunks(
        test_d(),
        &full.iter().map(|x| std::slice::from_ref(x)).collect::<Vec<_>>(),
    )
    .await;

    assert_eq!(text_contents(&a), text_contents(&b));
    assert_eq!(text_contents(&a), text_contents(&c));
    assert_eq!(
        text_contents(&a),
        vec!["Hello world.".to_string(), "Next sentence!".to_string()]
    );
}

#[tokio::test]
async fn no_partial_sentence_on_abrupt_end() {
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::new("i2"),
            connection_id: ConnectionId::new("c2"),
            external_session_id: None,
            dialect: test_d(),
            limits: InterpretationLimits::default(),
        })
        .unwrap();
    interp
        .input
        .push_bytes(bytes::Bytes::from_static(b"The implementation will"))
        .await
        .unwrap();
    interp.input.transport_failed().await.unwrap();
    let mut texts = Vec::new();
    let mut end_kind = None;
    loop {
        match interp.events.recv().await {
            Some(InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(s))) => {
                if let CanonicalUnit::Text(t) = &s.unit {
                    texts.push(t.content.clone());
                }
            }
            Some(InterpreterOutputEvent::Ended(e)) => {
                end_kind = Some(e.kind);
                assert!(e.unresolved_text_bytes > 0);
                break;
            }
            Some(_) => {}
            None => break,
        }
    }
    assert!(texts.is_empty());
    assert_eq!(end_kind, Some(InterpretationEndKind::TransportFailed));
}

#[tokio::test]
async fn acp_message_chunks_assemble_sentences() {
    let m1 = br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Hello "}}}}"#;
    let m2 = br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"world."}}}}"#;
    let m3 = br#"{"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}"#;

    // Also test byte-level fragmentation across JSON boundaries
    let mut stream = Vec::new();
    stream.extend_from_slice(m1);
    stream.extend_from_slice(m2);
    stream.extend_from_slice(m3);

    let whole = run_chunks(acp(), &[&stream]).await;
    let split_at = stream.len() / 3;
    let frag = run_chunks(
        acp(),
        &[&stream[..split_at], &stream[split_at..split_at * 2], &stream[split_at * 2..]],
    )
    .await;

    assert_eq!(text_contents(&whole), text_contents(&frag));
    assert_eq!(text_contents(&whole), vec!["Hello world.".to_string()]);
}

#[tokio::test]
async fn acp_tool_waiting_then_ready() {
    let wait = br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call","toolCallId":"t1","title":"read_file","status":"pending"}}}"#;
    let ready = br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call_update","toolCallId":"t1","title":"read_file","status":"pending","rawInput":{"path":"/tmp/x"}}}}"#;

    let events = run_chunks(acp(), &[wait, ready]).await;
    let tools: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Tool(t) => Some((
                    t.tool_action_id.as_str().to_string(),
                    t.request_state,
                    t.request_payload.clone(),
                    u.snapshot().unit_state,
                )),
                _ => None,
            },
            _ => None,
        })
        .collect();

    assert!(tools.len() >= 2);
    assert_eq!(tools[0].1, ToolRequestState::Assembling);
    assert!(tools[0].2.is_none(), "waiting must not expose partial args");
    assert_eq!(tools[0].3, UnitState::Waiting);
    let ready_ev = tools.iter().find(|t| t.1 == ToolRequestState::Ready).unwrap();
    assert!(ready_ev.2.as_ref().unwrap().contains("path"));
}

#[tokio::test]
async fn sentence_complete_before_response_end() {
    let factory = DefaultInterpreterFactory::new();
    let interp = factory
        .start(StartInterpretation {
            interpretation_id: InterpretationId::new("i3"),
            connection_id: ConnectionId::new("c3"),
            external_session_id: None,
            dialect: test_d(),
            limits: InterpretationLimits::default(),
        })
        .unwrap();
    interp
        .input
        .push_bytes(bytes::Bytes::from_static(b"First sentence. "))
        .await
        .unwrap();
    // Should already have a sentence event without finish
    let ev = tokio::time::timeout(std::time::Duration::from_secs(1), interp.events.recv())
        .await
        .expect("timeout")
        .expect("event");
    match ev {
        InterpreterOutputEvent::Unit(CanonicalUnitEvent::Created(s)) => {
            assert_eq!(s.unit_state, UnitState::Complete);
            match s.unit {
                CanonicalUnit::Text(t) => {
                    assert_eq!(t.content, "First sentence.");
                    assert_eq!(t.channel, TextChannel::PublicResponse);
                }
                _ => panic!("expected text"),
            }
        }
        other => panic!("unexpected {other:?}"),
    }
    interp.input.finish_clean().await.unwrap();
}
