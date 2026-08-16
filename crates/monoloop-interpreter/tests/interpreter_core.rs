#![allow(clippy::while_let_loop)]
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
            InterpreterOutputEvent::Unit(u) => match u.as_ref() {
                CanonicalUnitEvent::Created(s) => match &s.unit {
                    CanonicalUnit::Text(t) => Some(t.content.clone()),
                    _ => None,
                },
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
        &full.iter().map(std::slice::from_ref).collect::<Vec<_>>(),
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
            Some(InterpreterOutputEvent::Unit(u)) => {
                if let CanonicalUnitEvent::Created(s) = u.as_ref() {
                    if let CanonicalUnit::Text(t) = &s.unit {
                        texts.push(t.content.clone());
                    }
                }
            }
            Some(InterpreterOutputEvent::Ended(e)) => {
                end_kind = Some(e.kind);
                assert!(e.unresolved_text_bytes > 0);
                break;
            }
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

/// Live Grok Build: no status field; result arrives as `content` on tool_call_update.
#[tokio::test]
async fn acp_grok_tool_content_update_reaches_terminal_success() {
    let call = br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call","toolCallId":"call-1","title":"write","rawInput":{"file_path":"/tmp/x.txt","content":"hello monoloop crud\n"}}}}"#;
    let upd = br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call_update","toolCallId":"call-1","title":"Write `/tmp/x.txt`","kind":"edit","rawInput":{"file_path":"/tmp/x.txt","content":"hello monoloop crud\n"},"content":[{"type":"diff","path":"/tmp/x.txt","oldText":"","newText":"hello monoloop crud\n"}],"locations":[]}}}"#;

    let events = run_chunks(acp(), &[call, upd]).await;
    let tools: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Tool(t) => Some((
                    t.request_state,
                    t.execution_state,
                    t.terminal_outcome,
                    t.request_payload.clone(),
                    t.result_payload.clone(),
                    u.snapshot().unit_state,
                )),
                _ => None,
            },
            _ => None,
        })
        .collect();

    assert!(
        tools.iter().any(|(req, _, _, payload, _, _)| {
            *req == ToolRequestState::Ready && payload.as_ref().is_some_and(|p| p.contains("file_path"))
        }),
        "ready with args: {tools:?}"
    );
    assert!(
        tools.iter().any(|(_, exec, term, _, result, state)| {
            *exec == monoloop_contracts::ToolExecutionState::Terminal
                && *term == Some(monoloop_contracts::ToolTerminalOutcome::Success)
                && result.as_ref().is_some_and(|r| r.contains("diff"))
                && *state == UnitState::Complete
        }),
        "terminal success with result: {tools:?}"
    );
}

/// Dialect stream steps (`stepIdx` / numeric `messageId`) attach as source_step.
#[tokio::test]
async fn acp_source_step_propagates_to_sentences_and_tools() {
    let tool = br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"tool_call","toolCallId":"call-1","title":"Create","status":"completed","rawInput":{"path":"/tmp/x"},"content":[{"type":"diff","path":"/tmp/x"}],"_meta":{"stepIdx":3}}}}"#;
    let t1 = br#"{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","messageId":"11","content":{"type":"text","text":"All done."}}}}"#;

    let events = run_chunks(acp(), &[tool, t1]).await;

    let tool_steps: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Tool(_) => u.snapshot().source_step,
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(
        tool_steps.contains(&3),
        "tool source_step: {tool_steps:?}"
    );

    let text_steps: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Text(t) => Some((t.content.clone(), u.snapshot().source_step)),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(
        text_steps
            .iter()
            .any(|(c, s)| c.contains("All done") && *s == Some(11)),
        "text source_step: {text_steps:?}"
    );
}

/// Dialect `agentTimestampMs` is attached as observational source_time on complete units.
#[tokio::test]
async fn acp_agent_timestamp_propagates_to_sentences_and_tools() {
    let t1 = br#"{"jsonrpc":"2.0","method":"session/update","params":{"_meta":{"agentTimestampMs":1000},"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Hello "}}}}"#;
    let t2 = br#"{"jsonrpc":"2.0","method":"session/update","params":{"_meta":{"agentTimestampMs":1005},"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"world."}}}}"#;
    let tool = br#"{"jsonrpc":"2.0","method":"session/update","params":{"_meta":{"agentTimestampMs":2000},"sessionId":"s1","update":{"sessionUpdate":"tool_call","toolCallId":"call-1","title":"write","rawInput":{"path":"/tmp/x"}}}}"#;
    let t3 = br#"{"jsonrpc":"2.0","method":"session/update","params":{"_meta":{"agentTimestampMs":3000},"sessionId":"s1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":" Done."}}}}"#;

    let events = run_chunks(acp(), &[t1, t2, tool, t3]).await;

    let texts: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Text(t) => Some((t.content.clone(), u.snapshot().source_time)),
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(
        texts.iter().any(|(c, st)| {
            c == "Hello world."
                && st.is_some_and(|s| s.first_ms == 1000 && s.last_ms == 1005)
        }),
        "sentence must span first/last fragment times: {texts:?}"
    );
    // " Done." may seal at clean end without trailing space after period.
    assert!(
        texts.iter().any(|(c, st)| {
            c.trim() == "Done." && st.is_some_and(|s| s.first_ms == 3000 && s.last_ms == 3000)
        }),
        "later sentence time: {texts:?}"
    );

    let tools: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            InterpreterOutputEvent::Unit(u) => match &u.snapshot().unit {
                CanonicalUnit::Tool(_) => u.snapshot().source_time,
                _ => None,
            },
            _ => None,
        })
        .collect();
    assert!(
        tools.iter().any(|s| s.first_ms == 2000 && s.last_ms == 2000),
        "tool source time: {tools:?}"
    );

    // Emit order can place the tool *before* the completed sentence when the
    // period lacks trailing whitespace until a later chunk — that is why
    // source_time is observational metadata, not emit-order causality.
    let first_sentence_t = texts
        .iter()
        .find(|(c, _)| c == "Hello world.")
        .and_then(|(_, st)| *st)
        .unwrap();
    assert!(
        first_sentence_t.first_ms < 2000,
        "dialect source time of prose starts before tool: {first_sentence_t:?}"
    );
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
        InterpreterOutputEvent::Unit(u) => {
            let CanonicalUnitEvent::Created(s) = u.as_ref() else {
                panic!("expected created unit");
            };
            assert_eq!(s.unit_state, UnitState::Complete);
            match &s.unit {
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

/// Reasoning and response lanes get independent ordinal sequences (D-006).
#[tokio::test]
async fn reasoning_and_response_lane_ordinals_independent() {
    // Public reasoning summary requires public:true in ACP mapping; feed via test dialect
    // by using ACP public thought + response chunks.
    let thought = br#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_thought_chunk","public":true,"content":{"type":"text","text":"Thinking carefully. "}}}}"#;
    let resp = br#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Answer here."}}}}"#;
    let events = run_chunks(acp(), &[thought, resp]).await;

    let mut response_ords = Vec::new();
    let mut reasoning_ords = Vec::new();
    for e in &events {
        let InterpreterOutputEvent::Unit(u) = e else { continue };
        let CanonicalUnitEvent::Created(s) = u.as_ref() else { continue };
        let CanonicalUnit::Text(t) = &s.unit else { continue };
        match t.channel {
            TextChannel::PublicResponse => {
                response_ords.push((s.lane_id.as_str().to_string(), s.lane_ordinal, t.sentence_ordinal));
            }
            TextChannel::PublicReasoningSummary => {
                reasoning_ords.push((s.lane_id.as_str().to_string(), s.lane_ordinal, t.sentence_ordinal));
            }
            _ => {}
        }
    }
    // At least one response unit with matching ordinals on response lane.
    assert!(
        response_ords.iter().any(|(lane, lo, so)| lane == "response" && *lo == *so && *lo >= 1),
        "response lane ordinals: {response_ords:?}"
    );
    if !reasoning_ords.is_empty() {
        assert!(
            reasoning_ords.iter().all(|(lane, lo, so)| lane == "reasoning" && *lo == *so && *lo >= 1),
            "reasoning lane ordinals: {reasoning_ords:?}"
        );
        // Independent sequences: both can start at 1.
        assert_eq!(reasoning_ords[0].1, 1);
        assert_eq!(response_ords[0].1, 1);
    }
}
