//! DirectLlm Golden Phase A+B (partial): HTTP/OpenAI SSE through `StartedRuntime`.
//!
//! Phase A: text-only + concurrent admits.
//! Phase B: CallerControlled tool path; InlineToolContinuation second exchange;
//! call-ID reuse across sequential admits (distinct exchange-scoped action ids).

use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use monoloop_connector::{
    AnonymousCredentialResolver, StreamingHttpConfig, StreamingHttpConnectorFactory,
};
use monoloop_contracts::{
    transaction_delivery, user_text_input, CanonicalToolOutput, CanonicalUnit, ChannelCapabilities,
    ChannelDefaults, ChannelId, ChannelKind, ChannelLimits, ContinuationPolicy, DeliveryLimits,
    DialectBinding, DialectDescriptor, ExchangeMode, InvocationConfig, JsonSchema,
    McpConfigurationCapability, McpReachability, OptionPolicy, SessionMode, ShutdownWaitOutcome,
    ToolCompletion, ToolExecutionClass, ToolExecutionMode, ToolId, ToolLifecycleEvent, ToolLimits,
    ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionEndKind,
    TransactionEventPayload, TransactionLimits, TransactionReceiver, TransactionSubmitRequest,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, HostToolRegistry, ImmediateToolHandler,
    OpenAiChatCompletionsEncoder, OpenAiEncoderOptions, RegisteredTool, RuntimeBootstrap,
    RuntimeConfig, StartedRuntime,
};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

fn suite_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn test_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("test runtime")
}

async fn bind_fragmented_openai_sse(
    script: Arc<dyn Fn(String) -> Vec<Bytes> + Send + Sync>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/healthz", axum::routing::get(|| async { StatusCode::OK }))
        .route(
            "/v1/chat/completions",
            post(move |req: Request| {
                let script = Arc::clone(&script);
                async move {
                    let body = axum::body::to_bytes(req.into_body(), 2 * 1024 * 1024)
                        .await
                        .unwrap_or_default();
                    let body_str = String::from_utf8_lossy(&body).into_owned();
                    let chunks = script(body_str);
                    let stream =
                        futures_util::stream::iter(chunks.into_iter().map(Ok::<_, std::io::Error>));
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "text/event-stream")
                        .body(Body::from_stream(stream))
                        .unwrap()
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let join = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let health = format!("http://{addr}/healthz");
    for _ in 0..100 {
        let client = reqwest::Client::new();
        match client.get(&health).send().await {
            Ok(r) if r.status().is_success() => break,
            _ => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
    (addr, join)
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

fn echo_tool_registry() -> HostToolRegistry {
    let schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "q": { "type": "string" } },
        "additionalProperties": true
    }))
    .unwrap();
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

fn openai_http_channel(id: &str, endpoint: String, model: &str) -> ChannelBinding {
    openai_http_channel_with_policies(
        id,
        endpoint,
        model,
        BTreeSet::from([ContinuationPolicy::CallerControlled]),
    )
}

fn openai_http_channel_with_policies(
    id: &str,
    endpoint: String,
    model: &str,
    continuation_policies: BTreeSet<ContinuationPolicy>,
) -> ChannelBinding {
    let d = DialectDescriptor::openai_chat_completions("v1");
    let defaults = ChannelDefaults {
        model: Some(model.into()),
        ..Default::default()
    };
    let http_cfg = StreamingHttpConfig {
        dialect: DialectBinding::fixed(d.clone()),
        require_https: false,
        headers: vec![("content-type".into(), "application/json".into())],
        ..Default::default()
    };
    ChannelBinding {
        id: ChannelId::try_new(id).unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::ModelToolCalls,
        connector_factory: Arc::new(StreamingHttpConnectorFactory::new(
            http_cfg,
            Arc::new(AnonymousCredentialResolver),
        )),
        encoder: Arc::new(OpenAiChatCompletionsEncoder::new(OpenAiEncoderOptions {
            use_max_completion_tokens: false,
            allow_reasoning_effort: false,
            max_encoded_bytes: 1024 * 1024,
        })),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: endpoint,
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
    }
}

async fn drain_until_completed(
    receiver: TransactionReceiver,
) -> (
    monoloop_contracts::TransactionCompletion,
    Vec<String>,
    Vec<&'static str>,
) {
    let (completion, texts, labels, _) = drain_until_completed_detailed(receiver).await;
    (completion, texts, labels)
}

async fn drain_until_completed_detailed(
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
    let push_label = |labels: &mut Vec<&'static str>, ev: &TransactionEventPayload| {
        labels.push(match ev {
            TransactionEventPayload::SessionEstablished { .. } => "session",
            TransactionEventPayload::CanonicalUnit(u) => u.snapshot().unit.kind_label(),
            TransactionEventPayload::ToolLifecycle(_) => "tool_life",
            TransactionEventPayload::Diagnostic(_) => "diag",
            TransactionEventPayload::Ended(_) => "ended",
            TransactionEventPayload::EndedEvent(_) => "ended_event",
        });
    };
    let completion = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                biased;
                c = &mut completion_fut => {
                    break c.expect("completion channel closed");
                }
                ev = events.recv() => {
                    if let Some(ev) = ev {
                        push_label(&mut labels, &ev.payload);
                        if let TransactionEventPayload::CanonicalUnit(unit) = &ev.payload {
                            if let CanonicalUnit::Text(t) = &unit.snapshot().unit {
                                texts.push(t.content.clone());
                            }
                        }
                        payloads.push(ev.payload);
                    }
                }
            }
        }
    })
    .await
    .expect("completion timed out");
    while let Ok(ev) = events.try_recv() {
        push_label(&mut labels, &ev.payload);
        if let TransactionEventPayload::CanonicalUnit(unit) = &ev.payload {
            if let CanonicalUnit::Text(t) = &unit.snapshot().unit {
                texts.push(t.content.clone());
            }
        }
        payloads.push(ev.payload);
    }
    (completion, texts, labels, payloads)
}

fn start_openai_runtime(bindings: Vec<ChannelBinding>) -> StartedRuntime {
    start_openai_runtime_with_tools(bindings, HostToolRegistry::empty())
}

fn start_openai_runtime_with_tools(
    bindings: Vec<ChannelBinding>,
    tools: HostToolRegistry,
) -> StartedRuntime {
    let limits = TransactionLimits {
        max_active_transactions: 8,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(10),
        cleanup_deadline: Duration::from_secs(2),
        ..TransactionLimits::default()
    };
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

#[test]
fn text_only_http_openai_emits_text_and_completed() {
    let _guard = suite_lock();
    let rt = test_rt();
    let script = Arc::new(|_body: String| fragmented_text_sse("Hello from OpenAI SSE."));
    let (addr, join) = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = format!("http://{addr}/v1/chat/completions");

    let started = start_openai_runtime(vec![openai_http_channel("openai-a", endpoint, "gpt-test")]);
    std::thread::sleep(Duration::from_millis(50));
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-a").unwrap(),
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

    let (completion, texts, labels) = rt.block_on(drain_until_completed(receiver));
    assert!(
        !texts.is_empty(),
        "expected at least one CanonicalUnit Text from HTTP/OpenAI composition; kind={:?}",
        completion.end.kind
    );
    let joined = texts.join("");
    assert!(
        joined.contains("Hello") || joined.contains("OpenAI"),
        "unexpected text units: {texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::Completed,
        "HTTP/OpenAI DirectLlm must Complete; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
    join.abort();
}

#[test]
fn concurrent_http_openai_admits_are_isolated() {
    let _guard = suite_lock();
    let rt = test_rt();
    // Separate loopback servers per channel so concurrent admits do not share one
    // hyper accept/stream path (keeps isolation proof about runtime routing).
    let script_a = Arc::new(|_body: String| fragmented_text_sse("reply-a."));
    let script_b = Arc::new(|_body: String| fragmented_text_sse("reply-b."));
    let (addr_a, join_a) = rt.block_on(bind_fragmented_openai_sse(script_a));
    let (addr_b, join_b) = rt.block_on(bind_fragmented_openai_sse(script_b));
    let endpoint_a = format!("http://{addr_a}/v1/chat/completions");
    let endpoint_b = format!("http://{addr_b}/v1/chat/completions");

    let started = start_openai_runtime(vec![
        openai_http_channel("c-a", endpoint_a, "m"),
        openai_http_channel("c-b", endpoint_b, "m"),
    ]);
    std::thread::sleep(Duration::from_millis(50));
    let handle = started.handle.clone();
    let (delivery_a, receiver_a) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let (delivery_b, receiver_b) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();

    let receipt_a = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("c-a").unwrap(),
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
            channel_id: ChannelId::try_new("c-b").unwrap(),
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

    let ((comp_a, texts_a, labels_a), (comp_b, texts_b, labels_b)) = rt.block_on(async {
        tokio::join!(
            drain_until_completed(receiver_a),
            drain_until_completed(receiver_b),
        )
    });

    assert_eq!(
        comp_a.end.kind,
        TransactionEndKind::Completed,
        "a diagnostics={:?} texts={texts_a:?} labels={labels_a:?} delivery={:?}",
        comp_a.end.diagnostics,
        comp_a.terminal_event_delivery
    );
    assert_eq!(
        comp_b.end.kind,
        TransactionEndKind::Completed,
        "b diagnostics={:?} texts={texts_b:?} labels={labels_b:?} delivery={:?}",
        comp_b.end.diagnostics,
        comp_b.terminal_event_delivery
    );
    assert_eq!(comp_a.end.transaction_id, receipt_a.transaction_id);
    assert_eq!(comp_b.end.transaction_id, receipt_b.transaction_id);
    assert_ne!(receipt_a.transaction_id, receipt_b.transaction_id);

    let joined_a = texts_a.join("");
    let joined_b = texts_b.join("");
    assert!(
        joined_a.contains("reply-a"),
        "channel a must not receive cross-routed text; got {texts_a:?}"
    );
    assert!(
        !joined_a.contains("reply-b"),
        "channel a must not see reply-b; got {texts_a:?}"
    );
    assert!(
        joined_b.contains("reply-b"),
        "channel b must not receive cross-routed text; got {texts_b:?}"
    );
    assert!(
        !joined_b.contains("reply-a"),
        "channel b must not see reply-a; got {texts_b:?}"
    );

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
    join_a.abort();
    join_b.abort();
}

/// Phase B: CallerControlled tool exchange encodes admitted tools, runs Loop,
/// and ends ContinuationRequired without a second HTTP open.
#[test]
fn caller_controlled_tool_exchange_ends_continuation_required_without_second_open() {
    let _guard = suite_lock();
    let rt = test_rt();
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let saw_tools = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw_tools_c = Arc::clone(&saw_tools);
    let script = Arc::new(move |body: String| {
        posts_c.fetch_add(1, Ordering::SeqCst);
        if body.contains("\"tools\"") && body.contains("echo") {
            saw_tools_c.store(true, Ordering::SeqCst);
        }
        fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#)
    });
    let (addr, join) = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = format!("http://{addr}/v1/chat/completions");

    let started = start_openai_runtime_with_tools(
        vec![openai_http_channel("openai-tools", endpoint, "gpt-test")],
        echo_tool_registry(),
    );
    std::thread::sleep(Duration::from_millis(50));
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-tools").unwrap(),
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

    let (completion, texts, labels) = rt.block_on(drain_until_completed(receiver));
    assert!(
        saw_tools.load(Ordering::SeqCst),
        "encode_initial must project admitted tool specs into the OpenAI request body; kind={:?} labels={labels:?} texts={texts:?} diagnostics={:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    assert_eq!(
        posts.load(Ordering::SeqCst),
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
        "expected Tool CanonicalUnit and/or ToolLifecycle after Ready dispatch; labels={labels:?}"
    );

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
    join.abort();
}

/// InlineToolContinuation: first POST returns tool_calls; second returns text.
#[test]
fn inline_tool_continuation_second_exchange_emits_text() {
    let _guard = suite_lock();
    let rt = test_rt();
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let second_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
    let second_bodies_c = Arc::clone(&second_bodies);
    let script = Arc::new(move |body: String| {
        let n = posts_c.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#)
        } else {
            second_bodies_c.lock().unwrap().push(body);
            fragmented_text_sse("tool result acknowledged.")
        }
    });
    let (addr, join) = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = format!("http://{addr}/v1/chat/completions");

    let started = start_openai_runtime_with_tools(
        vec![openai_http_channel_with_policies(
            "openai-inline",
            endpoint,
            "gpt-test",
            BTreeSet::from([
                ContinuationPolicy::CallerControlled,
                ContinuationPolicy::InlineToolContinuation,
            ]),
        )],
        echo_tool_registry(),
    );
    std::thread::sleep(Duration::from_millis(50));
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-inline").unwrap(),
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

    let (completion, texts, labels) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        posts.load(Ordering::SeqCst),
        2,
        "InlineToolContinuation must open a second provider exchange; kind={:?} labels={labels:?}",
        completion.end.kind
    );
    assert!(
        labels.iter().any(|l| *l == "tool_life"),
        "expected ToolLifecycle after Ready dispatch; labels={labels:?}"
    );
    let joined = texts.join("");
    assert!(
        joined.contains("tool result") || joined.contains("acknowledged"),
        "expected final Text unit from second exchange; texts={texts:?} labels={labels:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::Completed,
        "inline text continuation must Complete; got {:?} diagnostics={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind,
        completion.end.diagnostics
    );
    let bodies = second_bodies.lock().unwrap();
    assert_eq!(bodies.len(), 1, "expected exactly one second-exchange body");
    let body = &bodies[0];
    assert!(
        body.contains("\"role\":\"tool\"") || body.contains("\"role\": \"tool\""),
        "second POST must include role:tool; body={body}"
    );
    assert!(
        body.contains("tool_call_id") && body.contains("call_1"),
        "second POST must correlate tool_call_id=call_1; body={body}"
    );

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
    join.abort();
}

/// Same provider call id across sequential admits yields distinct action ids.
#[test]
fn reused_provider_call_id_across_exchanges_distinct_action_ids() {
    let _guard = suite_lock();
    let rt = test_rt();
    let script =
        Arc::new(|_body: String| fragmented_tool_call_sse("call_reuse", "echo", r#"{"q":"x"}"#));
    let (addr, join) = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = format!("http://{addr}/v1/chat/completions");

    let started = start_openai_runtime_with_tools(
        vec![openai_http_channel("openai-reuse", endpoint, "gpt-test")],
        echo_tool_registry(),
    );
    std::thread::sleep(Duration::from_millis(50));
    let handle = started.handle.clone();

    let mut action_ids = Vec::new();
    let mut provider_ids = Vec::new();
    for i in 0..2 {
        let (delivery, receiver) =
            transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
        handle
            .submit(TransactionSubmitRequest {
                channel_id: ChannelId::try_new("openai-reuse").unwrap(),
                session_id: None,
                input: user_text_input(format!("reuse round {i}")).unwrap(),
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
        let (completion, _texts, labels, payloads) =
            rt.block_on(drain_until_completed_detailed(receiver));
        assert_eq!(
            completion.end.kind,
            TransactionEndKind::ContinuationRequired,
            "round {i}: expected ContinuationRequired; got {:?} labels={labels:?}",
            completion.end.kind
        );
        let mut found = false;
        for payload in payloads {
            if let TransactionEventPayload::ToolLifecycle(ToolLifecycleEvent::Completed {
                result,
            }) = payload
            {
                assert_eq!(
                    result.provider_tool_call_id, "call_reuse",
                    "provider id must be preserved exactly"
                );
                assert!(
                    result.tool_action_id.as_str().contains("call_reuse"),
                    "action id must contain provider id; got {}",
                    result.tool_action_id.as_str()
                );
                provider_ids.push(result.provider_tool_call_id);
                action_ids.push(result.tool_action_id.as_str().to_string());
                found = true;
            }
        }
        assert!(
            found,
            "round {i}: expected ToolLifecycle Completed; labels={labels:?}"
        );
    }

    assert_eq!(action_ids.len(), 2);
    assert_ne!(
        action_ids[0], action_ids[1],
        "same provider id across exchanges must yield distinct tool_action_id; got {action_ids:?}"
    );
    assert_eq!(provider_ids[0], "call_reuse");
    assert_eq!(provider_ids[1], "call_reuse");

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
    join.abort();
}
