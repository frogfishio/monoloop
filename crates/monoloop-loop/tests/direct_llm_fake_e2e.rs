//! DirectLlm FakeConnector parity (D-053 Golden residual).
//!
//! Same OpenAI Chat Completions encoder/Interpreter path as
//! `direct_llm_openai_e2e.rs`, but transport is in-process FakeConnector with
//! scripted SSE rounds — no HTTP server / network.

use bytes::Bytes;
use monoloop_connector::{FakeConnectorConfig, FakeConnectorFactory, FakeEndpoint};
use monoloop_contracts::{
    transaction_delivery, user_text_input, CanonicalToolOutput, CanonicalUnit, ChannelCapabilities,
    ChannelDefaults, ChannelId, ChannelKind, ChannelLimits, ContinuationPolicy, DeliveryLimits,
    DialectDescriptor, ExchangeMode, InvocationConfig, JsonSchema, McpConfigurationCapability,
    McpReachability, OptionPolicy, SessionMode, ShutdownWaitOutcome, ToolCompletion,
    ToolExecutionClass, ToolExecutionMode, ToolId, ToolLifecycleEvent, ToolLimits, ToolName,
    ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionEndKind, TransactionEvent,
    TransactionEventPayload, TransactionLimits, TransactionReceiver, TransactionSubmitRequest,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, HostToolRegistry, ImmediateToolHandler,
    OpenAiChatCompletionsEncoder, OpenAiEncoderOptions, RegisteredTool, RuntimeBootstrap,
    RuntimeConfig, StartedRuntime,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn test_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime")
}

fn fragmented_text_sse(content: &str) -> Vec<Bytes> {
    let content_json = serde_json::to_string(content).unwrap();
    let first = format!(
        r#"data: {{"choices":[{{"index":0,"delta":{{"role":"assistant","content":{content_json}}}}}]}}"#
    );
    vec![
        Bytes::from(first),
        Bytes::from_static(b"\n\n"),
        Bytes::from_static(br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#),
        Bytes::from_static(b"\n\n"),
        Bytes::from_static(b"data: [DONE]\n\n"),
    ]
}

fn fragmented_tool_call_sse(call_id: &str, name: &str, args_json: &str) -> Vec<Bytes> {
    let args_escaped = serde_json::to_string(args_json).unwrap();
    let first = format!(
        r#"data: {{"choices":[{{"index":0,"delta":{{"tool_calls":[{{"index":0,"id":"{call_id}","type":"function","function":{{"name":"{name}","arguments":{args_escaped}}}}}]}}}}]}}"#
    );
    vec![
        Bytes::from(first),
        Bytes::from_static(b"\n\n"),
        Bytes::from_static(
            br#"data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
        ),
        Bytes::from_static(b"\n\n"),
        Bytes::from_static(b"data: [DONE]\n\n"),
    ]
}

fn tool_object_schema() -> JsonSchema {
    JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "q": { "type": "string" } },
        "additionalProperties": true
    }))
    .unwrap()
}

fn echo_tool_registry() -> HostToolRegistry {
    let schema = tool_object_schema();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo tool",
        schema.clone(),
        ToolOutputContract {
            success: ToolSuccessContract::json(schema),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            execution_deadline: Duration::from_secs(2),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                serde_json::json!({"ok": true}),
            )))
        })),
    )])
    .unwrap()
}

struct FakeOpenAiChannel {
    binding: ChannelBinding,
    sequence: FakeEndpoint,
    input_log: Option<Arc<Mutex<Vec<Bytes>>>>,
}

fn openai_fake_channel(
    id: &str,
    model: &str,
    rounds: Vec<Vec<Bytes>>,
    continuation_policies: BTreeSet<ContinuationPolicy>,
    with_input_log: bool,
) -> FakeOpenAiChannel {
    let d = DialectDescriptor::openai_chat_completions("v1");
    let input_log = if with_input_log {
        Some(Arc::new(Mutex::new(Vec::new())))
    } else {
        None
    };
    let sequence = match &input_log {
        Some(log) => FakeEndpoint::scripted_sequence_with_input_log(rounds, Arc::clone(log)),
        None => FakeEndpoint::scripted_sequence(rounds),
    };
    let mut endpoints = HashMap::new();
    endpoints.insert("script".into(), sequence.clone());
    let connector_cfg = FakeConnectorConfig {
        endpoints,
        output_dialect: d.clone(),
        ..FakeConnectorConfig::default()
    };
    let defaults = ChannelDefaults {
        model: Some(model.into()),
        ..Default::default()
    };
    let binding = ChannelBinding {
        id: ChannelId::try_new(id).unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::ModelToolCalls,
        connector_factory: Arc::new(FakeConnectorFactory::direct_llm_with_config(connector_cfg)),
        encoder: Arc::new(OpenAiChatCompletionsEncoder::new(OpenAiEncoderOptions {
            use_max_completion_tokens: false,
            allow_reasoning_effort: false,
            max_encoded_bytes: 1024 * 1024,
        })),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "script".into(),
        credential_ref: None,
        defaults,
        capabilities: ChannelCapabilities {
            session_mode: SessionMode::Stateless,
            mcp_configuration: McpConfigurationCapability::None,
            mcp_reachability: McpReachability::None,
            exchange_mode: ExchangeMode::RequestResponse,
            continuation_policies,
            supports_distinct_session_concurrency: true,
            input_dialect: d.clone(),
            output_dialect: d,
            option_policy: OptionPolicy::direct_llm(),
        },
        limits: ChannelLimits::default(),
    };
    FakeOpenAiChannel {
        binding,
        sequence,
        input_log,
    }
}

fn start_runtime(bindings: Vec<ChannelBinding>, tools: HostToolRegistry) -> StartedRuntime {
    start_runtime_with_limits(bindings, tools, default_tx_limits())
}

fn default_tx_limits() -> TransactionLimits {
    TransactionLimits {
        max_active_transactions: 8,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(10),
        cleanup_deadline: Duration::from_secs(2),
        ..TransactionLimits::default()
    }
}

fn start_runtime_with_limits(
    bindings: Vec<ChannelBinding>,
    tools: HostToolRegistry,
    limits: TransactionLimits,
) -> StartedRuntime {
    StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(bindings).unwrap(),
        tools,
    })
    .expect("start")
}

async fn drain_until_completed(
    receiver: TransactionReceiver,
) -> (
    monoloop_contracts::TransactionCompletion,
    Vec<String>,
    Vec<&'static str>,
    Vec<TransactionEventPayload>,
) {
    let mut events = receiver.events;
    let completion_fut = receiver.completion.recv();
    tokio::pin!(completion_fut);
    let mut texts = Vec::new();
    let mut labels = Vec::new();
    let mut payloads = Vec::new();
    let mut saw_ended = false;
    let mut completion = None;
    let push_event = |labels: &mut Vec<&'static str>,
                      texts: &mut Vec<String>,
                      payloads: &mut Vec<TransactionEventPayload>,
                      saw_ended: &mut bool,
                      ev: TransactionEvent| {
        let label = match &ev.payload {
            TransactionEventPayload::SessionEstablished { .. } => "session",
            TransactionEventPayload::CanonicalUnit(u) => u.snapshot().unit.kind_label(),
            TransactionEventPayload::ToolLifecycle(_) => "tool_life",
            TransactionEventPayload::Diagnostic(_) => "diag",
            TransactionEventPayload::Ended(_) => "ended",
            TransactionEventPayload::EndedEvent(_) => "ended_event",
        };
        if matches!(
            &ev.payload,
            TransactionEventPayload::Ended(_) | TransactionEventPayload::EndedEvent(_)
        ) {
            *saw_ended = true;
        }
        labels.push(label);
        if let TransactionEventPayload::CanonicalUnit(unit) = &ev.payload {
            if let CanonicalUnit::Text(t) = &unit.snapshot().unit {
                texts.push(t.content.clone());
            }
        }
        payloads.push(ev.payload);
    };
    let completion = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if completion.is_some() && saw_ended {
                break;
            }
            tokio::select! {
                c = &mut completion_fut, if completion.is_none() => {
                    completion = Some(c.expect("completion channel closed"));
                }
                ev = events.recv() => {
                    match ev {
                        Some(ev) => push_event(
                            &mut labels,
                            &mut texts,
                            &mut payloads,
                            &mut saw_ended,
                            ev,
                        ),
                        None => {
                            if completion.is_none() {
                                completion = Some(
                                    completion_fut.await.expect("completion channel closed"),
                                );
                            }
                            break;
                        }
                    }
                }
            }
        }
        let grace = tokio::time::Instant::now() + Duration::from_millis(50);
        while tokio::time::Instant::now() < grace {
            match events.try_recv() {
                Ok(ev) => push_event(&mut labels, &mut texts, &mut payloads, &mut saw_ended, ev),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        completion.expect("completion")
    })
    .await
    .expect("completion timed out");
    (completion, texts, labels, payloads)
}

fn completed_tool_results(payloads: &[TransactionEventPayload]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for p in payloads {
        if let TransactionEventPayload::ToolLifecycle(ToolLifecycleEvent::Completed { result }) = p
        {
            out.push((
                result.tool_action_id.as_str().to_string(),
                result.provider_tool_call_id.clone(),
            ));
        }
    }
    out
}

fn shutdown(started: StartedRuntime, rt: &tokio::runtime::Runtime) {
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
}

#[test]
fn fake_text_only_emits_text_and_completed() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-text",
        "gpt-test",
        vec![fragmented_text_sse("Hello from Fake OpenAI SSE.")],
        BTreeSet::from([ContinuationPolicy::CallerControlled]),
        false,
    );
    let started = start_runtime(vec![ch.binding], HostToolRegistry::empty());
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-text").unwrap(),
            session_id: None,
            input: user_text_input("Say hi.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    let joined = texts.join("");
    assert!(
        joined.contains("Hello") || joined.contains("Fake"),
        "unexpected text units: {texts:?} labels={labels:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::Completed,
        "Fake DirectLlm must Complete; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    assert_eq!(ch.sequence.opens(), 1);
    shutdown(started, &rt);
}

#[test]
fn fake_caller_controlled_tool_ends_continuation_required_without_second_open() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-tools",
        "gpt-test",
        vec![fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#)],
        BTreeSet::from([ContinuationPolicy::CallerControlled]),
        true,
    );
    let input_log = ch.input_log.clone().expect("input log");
    let started = start_runtime(vec![ch.binding], echo_tool_registry());
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-tools").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    let bodies = input_log.lock().unwrap();
    assert_eq!(bodies.len(), 1, "CallerControlled must open exactly once");
    let body = String::from_utf8_lossy(&bodies[0]);
    assert!(
        body.contains("\"tools\"") && body.contains("echo"),
        "encode_initial must project admitted tools; body={body}"
    );
    assert_eq!(
        ch.sequence.opens(),
        1,
        "CallerControlled must not open a second provider exchange; kind={:?} labels={labels:?}",
        completion.end.kind
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::ContinuationRequired,
        "CallerControlled tool path must end ContinuationRequired; got {:?} labels={labels:?} texts={texts:?} diagnostics={:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    assert!(
        labels.iter().any(|l| *l == "tool_life" || *l == "tool"),
        "expected Tool CanonicalUnit and/or ToolLifecycle; labels={labels:?}"
    );
    shutdown(started, &rt);
}

#[test]
fn fake_inline_tool_continuation_second_exchange_emits_text() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-inline",
        "gpt-test",
        vec![
            fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#),
            fragmented_text_sse("tool result acknowledged."),
        ],
        BTreeSet::from([
            ContinuationPolicy::CallerControlled,
            ContinuationPolicy::InlineToolContinuation,
        ]),
        true,
    );
    let input_log = ch.input_log.clone().expect("input log");
    let started = start_runtime(vec![ch.binding], echo_tool_registry());
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-inline").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::InlineToolContinuation,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        2,
        "InlineToolContinuation must open a second provider exchange; kind={:?} labels={labels:?}",
        completion.end.kind
    );
    assert!(
        labels.contains(&"tool_life"),
        "expected ToolLifecycle after Ready dispatch; labels={labels:?}"
    );
    let joined = texts.join("");
    assert!(
        joined.contains("tool result") || joined.contains("acknowledged"),
        "expected final Text from second exchange; texts={texts:?} labels={labels:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::Completed,
        "inline text continuation must Complete; got {:?} diagnostics={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    let bodies = input_log.lock().unwrap();
    assert_eq!(bodies.len(), 2, "expected initial + continuation bodies");
    let cont = String::from_utf8_lossy(&bodies[1]);
    assert!(
        cont.contains("\"role\":\"tool\"") || cont.contains("\"role\": \"tool\""),
        "continuation must include role:tool; body={cont}"
    );
    assert!(
        cont.contains("tool_call_id") && cont.contains("call_1"),
        "continuation must correlate tool_call_id=call_1; body={cont}"
    );
    shutdown(started, &rt);
}

#[test]
fn fake_inline_multi_round_completes_after_second_tool() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-inline-multi",
        "gpt-test",
        vec![
            fragmented_tool_call_sse("call_a", "echo", r#"{"q":"a"}"#),
            fragmented_tool_call_sse("call_b", "echo", r#"{"q":"b"}"#),
            fragmented_text_sse("done after two tools"),
        ],
        BTreeSet::from([
            ContinuationPolicy::CallerControlled,
            ContinuationPolicy::InlineToolContinuation,
        ]),
        true,
    );
    let input_log = ch.input_log.clone().expect("input log");
    let started = start_runtime(vec![ch.binding], echo_tool_registry());
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-inline-multi").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool twice.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::InlineToolContinuation,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        3,
        "multi-round inline must open three provider exchanges; kind={:?} labels={labels:?}",
        completion.end.kind
    );
    let tool_life_rounds = labels.iter().filter(|l| **l == "tool_life").count();
    assert!(
        tool_life_rounds >= 2,
        "expected at least two ToolLifecycle rounds; tool_life={tool_life_rounds} labels={labels:?}"
    );
    let joined = texts.join("");
    assert!(
        joined.contains("done"),
        "expected final text containing done; texts={texts:?} labels={labels:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::Completed,
        "multi-round inline must Complete; got {:?} diagnostics={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    let bodies = input_log.lock().unwrap();
    assert_eq!(bodies.len(), 3);
    for (i, body) in bodies.iter().skip(1).enumerate() {
        let s = String::from_utf8_lossy(body);
        assert!(
            s.contains("\"role\":\"tool\"") || s.contains("\"role\": \"tool\""),
            "continuation POST {i} must include role:tool; body={s}"
        );
    }
    shutdown(started, &rt);
}

#[test]
fn fake_concurrent_admits_are_isolated() {
    let rt = test_rt();
    let ch_a = openai_fake_channel(
        "fake-a",
        "m",
        vec![fragmented_text_sse("reply-a.")],
        BTreeSet::from([ContinuationPolicy::CallerControlled]),
        false,
    );
    let ch_b = openai_fake_channel(
        "fake-b",
        "m",
        vec![fragmented_text_sse("reply-b.")],
        BTreeSet::from([ContinuationPolicy::CallerControlled]),
        false,
    );
    let started = start_runtime(vec![ch_a.binding, ch_b.binding], HostToolRegistry::empty());
    let handle = started.handle.clone();
    let (delivery_a, receiver_a) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let (delivery_b, receiver_b) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();

    let receipt_a = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-a").unwrap(),
            session_id: None,
            input: user_text_input("msg-a please.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            delivery: delivery_a,
        })
        .expect("admit a");
    let receipt_b = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-b").unwrap(),
            session_id: None,
            input: user_text_input("msg-b please.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            delivery: delivery_b,
        })
        .expect("admit b");

    let ((comp_a, texts_a, labels_a, _), (comp_b, texts_b, labels_b, _)) = rt.block_on(async {
        tokio::join!(
            drain_until_completed(receiver_a),
            drain_until_completed(receiver_b),
        )
    });

    assert_eq!(
        comp_a.end.kind,
        TransactionEndKind::Completed,
        "{labels_a:?}"
    );
    assert_eq!(
        comp_b.end.kind,
        TransactionEndKind::Completed,
        "{labels_b:?}"
    );
    assert_eq!(comp_a.end.transaction_id, receipt_a.transaction_id);
    assert_eq!(comp_b.end.transaction_id, receipt_b.transaction_id);
    assert_ne!(receipt_a.transaction_id, receipt_b.transaction_id);

    let joined_a = texts_a.join("");
    let joined_b = texts_b.join("");
    assert!(
        joined_a.contains("reply-a") && !joined_a.contains("reply-b"),
        "channel a cross-talk; got {texts_a:?}"
    );
    assert!(
        joined_b.contains("reply-b") && !joined_b.contains("reply-a"),
        "channel b cross-talk; got {texts_b:?}"
    );
    assert_eq!(ch_a.sequence.opens(), 1);
    assert_eq!(ch_b.sequence.opens(), 1);
    shutdown(started, &rt);
}

#[test]
fn fake_reused_provider_call_id_across_admits_distinct_action_ids() {
    let rt = test_rt();
    // Two sequential admits each get one tool round (CallerControlled).
    // Reuse the same Fake factory config with enough sequence slots for both.
    let d = DialectDescriptor::openai_chat_completions("v1");
    let next = Arc::new(AtomicUsize::new(0));
    let sequence = FakeEndpoint::ScriptedSequence {
        rounds: Arc::new(vec![
            fragmented_tool_call_sse("call_reuse", "echo", r#"{"q":"x"}"#),
            fragmented_tool_call_sse("call_reuse", "echo", r#"{"q":"x"}"#),
        ]),
        next: Arc::clone(&next),
        input_log: None,
    };
    let mut endpoints = HashMap::new();
    endpoints.insert("script".into(), sequence.clone());
    let connector_cfg = FakeConnectorConfig {
        endpoints,
        output_dialect: d.clone(),
        ..FakeConnectorConfig::default()
    };
    let binding = ChannelBinding {
        id: ChannelId::try_new("fake-reuse").unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::ModelToolCalls,
        connector_factory: Arc::new(FakeConnectorFactory::direct_llm_with_config(connector_cfg)),
        encoder: Arc::new(OpenAiChatCompletionsEncoder::new(OpenAiEncoderOptions {
            use_max_completion_tokens: false,
            allow_reasoning_effort: false,
            max_encoded_bytes: 1024 * 1024,
        })),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "script".into(),
        credential_ref: None,
        defaults: ChannelDefaults {
            model: Some("gpt-test".into()),
            ..Default::default()
        },
        capabilities: ChannelCapabilities {
            session_mode: SessionMode::Stateless,
            mcp_configuration: McpConfigurationCapability::None,
            mcp_reachability: McpReachability::None,
            exchange_mode: ExchangeMode::RequestResponse,
            continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
            supports_distinct_session_concurrency: true,
            input_dialect: d.clone(),
            output_dialect: d,
            option_policy: OptionPolicy::direct_llm(),
        },
        limits: ChannelLimits::default(),
    };

    let started = start_runtime(vec![binding], echo_tool_registry());
    let handle = started.handle.clone();

    let mut action_ids = Vec::new();
    let mut provider_ids = Vec::new();
    for _ in 0..2 {
        let (delivery, receiver) =
            transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
        handle
            .submit(TransactionSubmitRequest {
                channel_id: ChannelId::try_new("fake-reuse").unwrap(),
                session_id: None,
                input: user_text_input("Use echo.").unwrap(),
                session_config: None,
                invocation_config: InvocationConfig {
                    deadline: Some(Duration::from_secs(5)),
                    continuation_policy: ContinuationPolicy::CallerControlled,
                    ..Default::default()
                },
                tools: vec![ToolId::try_new("echo").unwrap()],
                delivery,
            })
            .expect("admit");
        let (completion, _texts, labels, payloads) = rt.block_on(drain_until_completed(receiver));
        assert_eq!(
            completion.end.kind,
            TransactionEndKind::ContinuationRequired,
            "labels={labels:?}"
        );
        let results = completed_tool_results(&payloads);
        assert_eq!(
            results.len(),
            1,
            "expected one Completed tool; labels={labels:?}"
        );
        action_ids.push(results[0].0.clone());
        provider_ids.push(results[0].1.clone());
    }

    assert_eq!(provider_ids[0], "call_reuse");
    assert_eq!(provider_ids[1], "call_reuse");
    assert_ne!(
        action_ids[0], action_ids[1],
        "reused provider id must map to distinct internal action ids"
    );
    assert!(
        action_ids[0].contains("call_reuse"),
        "action id must contain provider id; got {}",
        action_ids[0]
    );
    assert!(
        action_ids[1].contains("call_reuse"),
        "action id must contain provider id; got {}",
        action_ids[1]
    );
    assert_eq!(sequence.opens(), 2);
    shutdown(started, &rt);
}

/// Exact bound: `max_continuations = 0` → InlineToolContinuation fails closed
/// after the initial tool exchange without opening a continuation.
#[test]
fn fake_inline_max_continuations_zero_ends_limit_exceeded() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-max0",
        "gpt-test",
        vec![fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#)],
        BTreeSet::from([
            ContinuationPolicy::CallerControlled,
            ContinuationPolicy::InlineToolContinuation,
        ]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_continuations = 0;
    let started = start_runtime_with_limits(vec![ch.binding], echo_tool_registry(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-max0").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::InlineToolContinuation,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        1,
        "max_continuations=0 must not open a continuation; labels={labels:?} texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "zero continuation ceiling must fail closed as LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    shutdown(started, &rt);
}

/// Exact bound: `max_continuations = 1` with a second tool response exhausts the
/// ceiling and ends LimitExceeded (no third provider open for text).
#[test]
fn fake_inline_max_continuations_one_exhausted_ends_limit_exceeded() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-max1",
        "gpt-test",
        vec![
            fragmented_tool_call_sse("call_a", "echo", r#"{"q":"a"}"#),
            fragmented_tool_call_sse("call_b", "echo", r#"{"q":"b"}"#),
            // Would be a third open if the bound were not enforced.
            fragmented_text_sse("should never be reached"),
        ],
        BTreeSet::from([
            ContinuationPolicy::CallerControlled,
            ContinuationPolicy::InlineToolContinuation,
        ]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_continuations = 1;
    let started = start_runtime_with_limits(vec![ch.binding], echo_tool_registry(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-max1").unwrap(),
            session_id: None,
            input: user_text_input("Keep calling echo.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::InlineToolContinuation,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        2,
        "max_continuations=1 allows one continuation open only; labels={labels:?} texts={texts:?}"
    );
    assert!(
        !texts.iter().any(|t| t.contains("should never be reached")),
        "exhausted ceiling must not consume the third scripted round; texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "exhausted max_continuations must fail closed as LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    shutdown(started, &rt);
}

fn sse_byte_len(chunks: &[Bytes]) -> usize {
    chunks.iter().map(|c| c.len()).sum()
}

/// Exact bound: `max_provider_exchanges = 2` allows one continuation tool round,
/// then fails closed without a third open.
#[test]
fn fake_inline_max_provider_exchanges_two_exact_then_limit_exceeded() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-pex2",
        "gpt-test",
        vec![
            fragmented_tool_call_sse("call_a", "echo", r#"{"q":"a"}"#),
            fragmented_tool_call_sse("call_b", "echo", r#"{"q":"b"}"#),
            fragmented_text_sse("should never be reached"),
        ],
        BTreeSet::from([
            ContinuationPolicy::CallerControlled,
            ContinuationPolicy::InlineToolContinuation,
        ]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    limits.max_provider_exchanges = 2;
    let started = start_runtime_with_limits(vec![ch.binding], echo_tool_registry(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-pex2").unwrap(),
            session_id: None,
            input: user_text_input("Keep calling echo.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::InlineToolContinuation,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        2,
        "max_provider_exchanges=2 allows initial+one continuation only; labels={labels:?} texts={texts:?}"
    );
    assert!(
        !texts.iter().any(|t| t.contains("should never be reached")),
        "third scripted round must not run; texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "exact exchange ceiling must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    shutdown(started, &rt);
}

/// Cumulative remaining output: first exchange consumes the full byte ceiling;
/// second open is refused with remaining_output == 0 (no extra open).
#[test]
fn fake_inline_cumulative_output_budget_exhausted_blocks_second_open() {
    let rt = test_rt();
    let round0 = fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#);
    let first_out = sse_byte_len(&round0);
    assert!(first_out > 0, "scripted tool SSE must be non-empty");
    let ch = openai_fake_channel(
        "fake-cum-out",
        "gpt-test",
        vec![round0, fragmented_text_sse("should never be reached")],
        BTreeSet::from([
            ContinuationPolicy::CallerControlled,
            ContinuationPolicy::InlineToolContinuation,
        ]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    limits.max_total_provider_output_bytes = first_out;
    let started = start_runtime_with_limits(vec![ch.binding], echo_tool_registry(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-cum-out").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::InlineToolContinuation,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        1,
        "exhausted remaining_output must block continuation open; labels={labels:?} texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "cumulative output exhaustion must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    shutdown(started, &rt);
}

/// Continuation remaining-input == 0: probe first encoded size, then Inline
/// with budget exactly equal so the second open is refused before Connector open.
#[test]
fn fake_inline_cumulative_input_budget_exhausted_blocks_second_open() {
    let rt = test_rt();
    // Probe: measure first encoded request body under CallerControlled.
    let probe = openai_fake_channel(
        "fake-cum-in-probe",
        "gpt-test",
        vec![fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#)],
        BTreeSet::from([ContinuationPolicy::CallerControlled]),
        true,
    );
    let probe_log = probe.input_log.clone().expect("input log");
    let started_probe = start_runtime_with_limits(
        vec![probe.binding],
        echo_tool_registry(),
        default_tx_limits(),
    );
    let handle = started_probe.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-cum-in-probe").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit probe");
    let (completion, _, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::ContinuationRequired,
        "probe must end ContinuationRequired; labels={labels:?}"
    );
    let first_in = probe_log.lock().unwrap()[0].len();
    assert!(first_in > 0, "probe must capture a non-empty encoded body");
    assert_eq!(probe.sequence.opens(), 1);
    shutdown(started_probe, &rt);

    // Real: budget == first encode size → after first, remaining_input == 0.
    let ch = openai_fake_channel(
        "fake-cum-in",
        "gpt-test",
        vec![
            fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#),
            fragmented_text_sse("should never be reached"),
        ],
        BTreeSet::from([
            ContinuationPolicy::CallerControlled,
            ContinuationPolicy::InlineToolContinuation,
        ]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    limits.max_total_provider_input_bytes = first_in;
    let started = start_runtime_with_limits(vec![ch.binding], echo_tool_registry(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-cum-in").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::InlineToolContinuation,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        1,
        "exhausted remaining_input must block continuation open; labels={labels:?} texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "cumulative input exhaustion must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    shutdown(started, &rt);
}

/// Cumulative remaining output plus-one: first fits; second opens then fails mid-pump.
#[test]
fn fake_inline_cumulative_output_budget_plus_one_fails_second_pump() {
    let rt = test_rt();
    let round0 = fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#);
    let round1 = fragmented_text_sse("second round text that must overflow remaining.");
    let first_out = sse_byte_len(&round0);
    let second_out = sse_byte_len(&round1);
    assert!(second_out > 1, "second SSE must exceed a 1-byte remainder");
    let ch = openai_fake_channel(
        "fake-cum-out-plus",
        "gpt-test",
        vec![round0, round1],
        BTreeSet::from([
            ContinuationPolicy::CallerControlled,
            ContinuationPolicy::InlineToolContinuation,
        ]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    // First exchange fits exactly; one leftover byte lets the second open start,
    // then mid-pump LimitExceeded before complete text is published.
    limits.max_total_provider_output_bytes = first_out.saturating_add(1);
    let started = start_runtime_with_limits(vec![ch.binding], echo_tool_registry(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-cum-out-plus").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::InlineToolContinuation,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        2,
        "plus-one remainder must allow the second open to start; labels={labels:?}"
    );
    assert!(
        !texts.iter().any(|t| t.contains("second round text")),
        "LimitExceeded mid-pump must not publish complete second-round text; texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "second-pump output overflow must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    shutdown(started, &rt);
}

/// Independent bound: `max_provider_exchanges = 1` blocks Inline continuation
/// after the initial tool exchange (no second open).
#[test]
fn fake_inline_max_provider_exchanges_one_ends_limit_exceeded() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-pex1",
        "gpt-test",
        vec![
            fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#),
            fragmented_text_sse("should never be reached"),
        ],
        BTreeSet::from([
            ContinuationPolicy::CallerControlled,
            ContinuationPolicy::InlineToolContinuation,
        ]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    limits.max_provider_exchanges = 1;
    let started = start_runtime_with_limits(vec![ch.binding], echo_tool_registry(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-pex1").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::InlineToolContinuation,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        1,
        "max_provider_exchanges=1 must not open a continuation; labels={labels:?} texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "provider exchange ceiling must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    shutdown(started, &rt);
}

/// §23: `TransactionLimits.max_tool_payload_bytes` via `limits_from_transaction`
/// rejects oversize tool arguments before execution.
#[test]
fn fake_transaction_limits_max_tool_payload_bytes_plus_one_rejects() {
    let rt = test_rt();
    // Arguments JSON is longer than a 5-byte transaction-wide payload cap.
    let ch = openai_fake_channel(
        "fake-tool-in",
        "gpt-test",
        vec![fragmented_tool_call_sse(
            "call_1",
            "echo",
            r#"{"q":"hello-world"}"#,
        )],
        BTreeSet::from([ContinuationPolicy::CallerControlled]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_tool_payload_bytes = 5;
    let started = start_runtime_with_limits(vec![ch.binding], echo_tool_registry(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-tool-in").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, _texts, labels, payloads) = rt.block_on(drain_until_completed(receiver));
    let saw_oversized = payloads.iter().any(|p| match p {
        TransactionEventPayload::ToolLifecycle(ToolLifecycleEvent::RuntimeFailed {
            code, ..
        }) => code.contains("oversized") || code == "oversized_input",
        TransactionEventPayload::ToolLifecycle(ToolLifecycleEvent::Completed { result }) => {
            matches!(
                &result.outcome,
                monoloop_contracts::CanonicalToolResultOutcome::DomainFailed(err)
                    if err.code.contains("oversized")
                        || err.message.contains("oversized")
                        || err.message.contains("exceeds limit")
            )
        }
        _ => false,
    });
    assert!(
        saw_oversized,
        "TransactionLimits.max_tool_payload_bytes must reject oversize args; kind={:?} labels={labels:?} payloads={payloads:?}",
        completion.end.kind
    );
    shutdown(started, &rt);
}

/// §23: `TransactionLimits.max_tool_output_bytes` (not bare DispatcherLimits)
/// plus-one fails closed on the production `limits_from_transaction` path.
#[test]
fn fake_transaction_limits_max_tool_output_bytes_plus_one_fails_closed() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-tool-out",
        "gpt-test",
        vec![fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#)],
        BTreeSet::from([ContinuationPolicy::CallerControlled]),
        false,
    );
    let mut limits = default_tx_limits();
    // Echo returns `{"ok":true}` which exceeds 4 bytes after JSON serialization.
    limits.max_tool_output_bytes = 4;
    let started = start_runtime_with_limits(vec![ch.binding], echo_tool_registry(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-tool-out").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, _texts, labels, payloads) = rt.block_on(drain_until_completed(receiver));
    let saw_output_violation = payloads.iter().any(|p| match p {
        TransactionEventPayload::ToolLifecycle(ToolLifecycleEvent::RuntimeFailed {
            code, ..
        }) => code == "output_contract_violated",
        TransactionEventPayload::ToolLifecycle(ToolLifecycleEvent::Completed { result }) => {
            matches!(
                &result.outcome,
                monoloop_contracts::CanonicalToolResultOutcome::DomainFailed(err)
                    if err.code.contains("tool_execution_failed")
                        || err.message.contains("output_contract_violated")
            )
        }
        _ => false,
    });
    assert!(
        saw_output_violation,
        "TransactionLimits.max_tool_output_bytes must fail closed via limits_from_transaction; kind={:?} labels={labels:?} payloads={payloads:?}",
        completion.end.kind
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::ContinuationRequired,
        "CallerControlled still ends ContinuationRequired after tool runtime failure; got {:?}",
        completion.end.kind
    );
    shutdown(started, &rt);
}

/// Independent bound: tiny total provider input fails closed before open.
#[test]
fn fake_total_provider_input_bytes_limit_exceeded_before_open() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-pin",
        "gpt-test",
        vec![fragmented_text_sse("should never be reached")],
        BTreeSet::from([ContinuationPolicy::CallerControlled]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_total_provider_input_bytes = 10;
    let started = start_runtime_with_limits(vec![ch.binding], HostToolRegistry::empty(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-pin").unwrap(),
            session_id: None,
            input: user_text_input("Say hi with enough prompt to exceed ten bytes.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        0,
        "total provider input ceiling must fail before Connector open; labels={labels:?} texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "total provider input overflow must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    shutdown(started, &rt);
}

/// Independent bound: tiny total provider output fails closed during pump.
#[test]
fn fake_total_provider_output_bytes_limit_exceeded() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-pout",
        "gpt-test",
        vec![fragmented_text_sse("Hello from oversized Fake SSE output.")],
        BTreeSet::from([ContinuationPolicy::CallerControlled]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_total_provider_output_bytes = 1;
    let started = start_runtime_with_limits(vec![ch.binding], HostToolRegistry::empty(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-pout").unwrap(),
            session_id: None,
            input: user_text_input("Say hi.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        1,
        "output ceiling is checked after open during pump; labels={labels:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "total provider output overflow must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    shutdown(started, &rt);
}

/// Context-byte ceiling: tiny `max_continuation_context_bytes` fails closed
/// before the second provider open.
#[test]
fn fake_inline_continuation_context_bytes_limit_exceeded() {
    let rt = test_rt();
    let ch = openai_fake_channel(
        "fake-ctx",
        "gpt-test",
        vec![
            fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#),
            fragmented_text_sse("should never be reached"),
        ],
        BTreeSet::from([
            ContinuationPolicy::CallerControlled,
            ContinuationPolicy::InlineToolContinuation,
        ]),
        false,
    );
    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    limits.max_continuation_context_bytes = 1;
    let started = start_runtime_with_limits(vec![ch.binding], echo_tool_registry(), limits);
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("fake-ctx").unwrap(),
            session_id: None,
            input: user_text_input("Use the echo tool.").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::InlineToolContinuation,
                ..Default::default()
            },
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");

    let (completion, texts, labels, _) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        ch.sequence.opens(),
        1,
        "context-byte ceiling must fail before continuation open; labels={labels:?} texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "max_continuation_context_bytes overflow must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    shutdown(started, &rt);
}
