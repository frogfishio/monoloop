//! DirectLlm Golden Phase A+B (partial): HTTP/OpenAI SSE through `StartedRuntime`.
//!
//! Phase A: text-only + concurrent admits.
//! Phase B: CallerControlled tool path; InlineToolContinuation one- and multi-round
//! continuation; call-ID reuse across sequential admits (distinct exchange-scoped
//! action ids).

use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use monoloop_connector::{
    AnonymousCredentialResolver, ConnectorTargetResolver, ResolvedConnectorTarget,
    ResolvedCredential, StreamingHttpConfig, StreamingHttpConnectorFactory,
};
use monoloop_contracts::{
    transaction_delivery, user_text_input, CanonicalToolOutput, CanonicalUnit, ChannelCapabilities,
    ChannelDefaults, ChannelId, ChannelKind, ChannelLimits, ConnectorError, ContinuationPolicy,
    DeliveryLimits, DialectBinding, DialectDescriptor, ExchangeMode, InvocationConfig, JsonSchema,
    McpConfigurationCapability, McpReachability, OptionPolicy, SessionConfig, SessionMode,
    ShutdownWaitOutcome, ToolCompletion, ToolExecutionClass, ToolExecutionMode, ToolId,
    ToolLifecycleEvent, ToolLimits, ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract,
    TransactionEndKind, TransactionEvent, TransactionEventPayload, TransactionLimits,
    TransactionReceiver, TransactionSubmitRequest,
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

/// Shared suite runtime — creating/destroying multi-thread runtimes per test
/// races under the default cargo harness and produced intermittent `Cancelled`.
fn test_rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("test runtime")
    })
}

/// Loopback SSE server with graceful shutdown (no `JoinHandle::abort`).
struct SseServer {
    addr: std::net::SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<()>,
}

impl SseServer {
    fn endpoint(&self) -> String {
        format!("http://{}/v1/chat/completions", self.addr)
    }

    async fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), self.join).await;
    }
}

async fn bind_fragmented_openai_sse(
    script: Arc<dyn Fn(String) -> Vec<Bytes> + Send + Sync>,
) -> SseServer {
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
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });
    let health = format!("http://{addr}/healthz");
    let mut ready = false;
    for _ in 0..200 {
        let client = reqwest::Client::new();
        match client.get(&health).send().await {
            Ok(r) if r.status().is_success() => {
                ready = true;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
    assert!(ready, "SSE server healthz never became ready at {addr}");
    SseServer {
        addr,
        shutdown: Some(shutdown_tx),
        join,
    }
}

fn finish_http_test(
    started: StartedRuntime,
    rt: &tokio::runtime::Runtime,
    servers: Vec<SseServer>,
) {
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        let stopped = owner.wait_stopped(Duration::from_secs(2)).await;
        for server in servers {
            server.stop().await;
        }
        // Let Hyper/reqwest connection closeouts settle on the shared runtime
        // before the next suite_lock holder binds a new loopback listener.
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        stopped
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
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
    // Prefer draining the event stream (including Ended) alongside completion so a
    // late final Text unit is not lost when completion races ahead of mailbox delivery.
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
                            // Event stream closed — wait for completion if still pending.
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
        // Terminal fence: after Ended∧completion, any further event is a violation.
        match events.try_recv() {
            Ok(ev) => panic!(
                "post-terminal event after Ended+completion: {:?}",
                ev.payload
            ),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            | Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {}
        }
        completion.expect("completion")
    })
    .await
    .expect("completion timed out");
    (completion, texts, labels, payloads)
}

fn start_openai_runtime(bindings: Vec<ChannelBinding>) -> StartedRuntime {
    start_openai_runtime_with_tools(bindings, HostToolRegistry::empty())
}

fn start_openai_runtime_with_tools(
    bindings: Vec<ChannelBinding>,
    tools: HostToolRegistry,
) -> StartedRuntime {
    start_openai_runtime_with_tools_and_limits(bindings, tools, default_tx_limits())
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

fn start_openai_runtime_with_tools_and_limits(
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

#[test]
fn text_only_http_openai_emits_text_and_completed() {
    let _guard = suite_lock();
    let rt = test_rt();
    let script = Arc::new(|_body: String| fragmented_text_sse("Hello from OpenAI SSE."));
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let started = start_openai_runtime(vec![openai_http_channel("openai-a", endpoint, "gpt-test")]);
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

    finish_http_test(started, rt, vec![server]);
}

/// D-064: one Channel, many equivalent backends. `SessionConfig::connector_ref`
/// carried per-transaction — not the Channel's fixed `endpoint_ref` — decides
/// which of two independent mock servers each turn actually reaches, proving
/// "Direct" no longer needs one Channel per backend to serve many otherwise-
/// identical OpenAI-compatible providers.
struct TwoServerResolver {
    a_endpoint: String,
    b_endpoint: String,
}

impl ConnectorTargetResolver for TwoServerResolver {
    fn resolve(&self, connector_ref: &str) -> Result<ResolvedConnectorTarget, ConnectorError> {
        let endpoint = match connector_ref {
            "server-a" => self.a_endpoint.clone(),
            "server-b" => self.b_endpoint.clone(),
            other => {
                return Err(ConnectorError::configuration_invalid(format!(
                    "unknown connector_ref: {other}"
                )))
            }
        };
        Ok(ResolvedConnectorTarget {
            endpoint,
            credential: ResolvedCredential::bearer(format!("key-for-{connector_ref}")),
        })
    }
}

#[test]
fn one_dynamic_channel_routes_by_connector_ref_per_transaction() {
    let _guard = suite_lock();
    let rt = test_rt();
    let script_a = Arc::new(|_body: String| fragmented_text_sse("Hello from server A."));
    let script_b = Arc::new(|_body: String| fragmented_text_sse("Hello from server B."));
    let server_a = rt.block_on(bind_fragmented_openai_sse(script_a));
    let server_b = rt.block_on(bind_fragmented_openai_sse(script_b));

    let d = DialectDescriptor::openai_chat_completions("v1");
    let resolver = Arc::new(TwoServerResolver {
        a_endpoint: server_a.endpoint(),
        b_endpoint: server_b.endpoint(),
    });
    let binding = ChannelBinding {
        id: ChannelId::try_new("direct").unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::None,
        // Fixed endpoint_ref is never used when connector_ref resolves —
        // deliberately unreachable, to prove nothing falls back to it.
        connector_factory: Arc::new(StreamingHttpConnectorFactory::new_dynamic(
            StreamingHttpConfig {
                dialect: DialectBinding::fixed(d.clone()),
                require_https: false,
                ..Default::default()
            },
            Arc::new(AnonymousCredentialResolver),
            resolver,
        )),
        encoder: Arc::new(OpenAiChatCompletionsEncoder::new(OpenAiEncoderOptions {
            use_max_completion_tokens: false,
            allow_reasoning_effort: false,
            max_encoded_bytes: 1024 * 1024,
        })),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "http://unreachable.invalid/should-never-be-used".into(),
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

    let started = start_openai_runtime(vec![binding]);
    let handle = started.handle.clone();

    let submit_to = |connector_ref: &str| {
        let (delivery, receiver) =
            transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
        handle
            .submit(TransactionSubmitRequest {
                channel_id: ChannelId::try_new("direct").unwrap(),
                session_id: None,
                input: user_text_input("Say hi.").unwrap(),
                session_config: Some(SessionConfig {
                    connector_ref: Some(connector_ref.to_string()),
                    ..Default::default()
                }),
                invocation_config: InvocationConfig {
                    deadline: Some(Duration::from_secs(5)),
                    continuation_policy: ContinuationPolicy::CallerControlled,
                    ..Default::default()
                },
                tools: vec![],
                delivery,
            })
            .expect("admit");
        receiver
    };

    let (completion_a, texts_a, _) = rt.block_on(drain_until_completed(submit_to("server-a")));
    let (completion_b, texts_b, _) = rt.block_on(drain_until_completed(submit_to("server-b")));

    assert_eq!(completion_a.end.kind, TransactionEndKind::Completed);
    assert_eq!(completion_b.end.kind, TransactionEndKind::Completed);
    assert!(
        texts_a.join("").contains("server A"),
        "connector_ref=server-a must reach server A; got {texts_a:?}"
    );
    assert!(
        texts_b.join("").contains("server B"),
        "connector_ref=server-b must reach server B; got {texts_b:?}"
    );

    finish_http_test(started, rt, vec![server_a, server_b]);
}

#[test]
fn concurrent_http_openai_admits_are_isolated() {
    let _guard = suite_lock();
    let rt = test_rt();
    // Separate loopback servers per channel so concurrent admits do not share one
    // hyper accept/stream path (keeps isolation proof about runtime routing).
    let script_a = Arc::new(|_body: String| fragmented_text_sse("reply-a."));
    let script_b = Arc::new(|_body: String| fragmented_text_sse("reply-b."));
    let server_a = rt.block_on(bind_fragmented_openai_sse(script_a));
    let server_b = rt.block_on(bind_fragmented_openai_sse(script_b));
    let endpoint_a = server_a.endpoint();
    let endpoint_b = server_b.endpoint();

    let started = start_openai_runtime(vec![
        openai_http_channel("c-a", endpoint_a, "m"),
        openai_http_channel("c-b", endpoint_b, "m"),
    ]);
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

    finish_http_test(started, rt, vec![server_a, server_b]);
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
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let started = start_openai_runtime_with_tools(
        vec![openai_http_channel("openai-tools", endpoint, "gpt-test")],
        echo_tool_registry(),
    );
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

    finish_http_test(started, rt, vec![server]);
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
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

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
        labels.contains(&"tool_life"),
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

    finish_http_test(started, rt, vec![server]);
}

/// InlineToolContinuation multi-round: tool → tool → text completes after two tools.
#[test]
fn inline_multi_round_tool_continuation_completes_after_second_tool() {
    let _guard = suite_lock();
    let rt = test_rt();
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let continuation_bodies = Arc::new(Mutex::new(Vec::<String>::new()));
    let continuation_bodies_c = Arc::clone(&continuation_bodies);
    let script = Arc::new(move |body: String| {
        let n = posts_c.fetch_add(1, Ordering::SeqCst);
        match n {
            0 => fragmented_tool_call_sse("call_a", "echo", r#"{"q":"a"}"#),
            1 => {
                continuation_bodies_c.lock().unwrap().push(body);
                fragmented_tool_call_sse("call_b", "echo", r#"{"q":"b"}"#)
            }
            _ => {
                continuation_bodies_c.lock().unwrap().push(body);
                fragmented_text_sse("done after two tools")
            }
        }
    });
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let started = start_openai_runtime_with_tools(
        vec![openai_http_channel_with_policies(
            "openai-inline-multi",
            endpoint,
            "gpt-test",
            BTreeSet::from([
                ContinuationPolicy::CallerControlled,
                ContinuationPolicy::InlineToolContinuation,
            ]),
        )],
        echo_tool_registry(),
    );
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-inline-multi").unwrap(),
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

    let (completion, texts, labels) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        posts.load(Ordering::SeqCst),
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
    let bodies = continuation_bodies.lock().unwrap();
    assert_eq!(
        bodies.len(),
        2,
        "expected two continuation request bodies; got {}",
        bodies.len()
    );
    // First continuation carries call_a + its tool result in order (no call_b yet).
    let b0 = &bodies[0];
    let call_a = b0
        .find("\"id\":\"call_a\"")
        .or_else(|| b0.find("\"id\": \"call_a\""))
        .expect("first continuation must include assistant tool_calls id call_a");
    let tool_a = b0[call_a..]
        .find("\"tool_call_id\":\"call_a\"")
        .or_else(|| b0[call_a..].find("\"tool_call_id\": \"call_a\""))
        .expect("first continuation must include tool result for call_a after the call");
    assert!(
        !b0.contains("call_b"),
        "first continuation must not yet include call_b; body={b0}"
    );
    let _ = tool_a;
    // Second continuation retains both prior pairs in order (call then result each).
    let b1 = &bodies[1];
    let a_call = b1
        .find("\"id\":\"call_a\"")
        .or_else(|| b1.find("\"id\": \"call_a\""))
        .expect("second continuation must retain call_a");
    let a_res = b1[a_call..]
        .find("\"tool_call_id\":\"call_a\"")
        .or_else(|| b1[a_call..].find("\"tool_call_id\": \"call_a\""))
        .map(|o| a_call + o)
        .expect("second continuation must retain tool result for call_a");
    let b_call = b1
        .find("\"id\":\"call_b\"")
        .or_else(|| b1.find("\"id\": \"call_b\""))
        .expect("second continuation must include call_b");
    let b_res = b1[b_call..]
        .find("\"tool_call_id\":\"call_b\"")
        .or_else(|| b1[b_call..].find("\"tool_call_id\": \"call_b\""))
        .map(|o| b_call + o)
        .expect("second continuation must include tool result for call_b");
    assert!(
        a_call < a_res && a_res < b_call && b_call < b_res,
        "expected call_a < result_a < call_b < result_b absolute order; \
         a_call={a_call} a_res={a_res} b_call={b_call} b_res={b_res}; body={b1}"
    );

    finish_http_test(started, rt, vec![server]);
}

/// Repeated multi-round HTTP continuation must not Complete with empty final text.
#[test]
fn inline_multi_round_tool_continuation_repeated_keeps_final_text() {
    for i in 0..20 {
        let _guard = suite_lock();
        let rt = test_rt();
        let posts = Arc::new(AtomicUsize::new(0));
        let posts_c = Arc::clone(&posts);
        let script = Arc::new(move |_body: String| {
            let n = posts_c.fetch_add(1, Ordering::SeqCst);
            match n {
                0 => fragmented_tool_call_sse("call_a", "echo", r#"{"q":"a"}"#),
                1 => fragmented_tool_call_sse("call_b", "echo", r#"{"q":"b"}"#),
                _ => fragmented_text_sse("done after two tools"),
            }
        });
        let server = rt.block_on(bind_fragmented_openai_sse(script));
        let endpoint = server.endpoint();
        let started = start_openai_runtime_with_tools(
            vec![openai_http_channel_with_policies(
                "openai-inline-multi-stress",
                endpoint,
                "gpt-test",
                BTreeSet::from([
                    ContinuationPolicy::CallerControlled,
                    ContinuationPolicy::InlineToolContinuation,
                ]),
            )],
            echo_tool_registry(),
        );
        let handle = started.handle.clone();
        let (delivery, receiver) =
            transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
        handle
            .submit(TransactionSubmitRequest {
                channel_id: ChannelId::try_new("openai-inline-multi-stress").unwrap(),
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
        let (completion, texts, labels) = rt.block_on(drain_until_completed(receiver));
        let joined = texts.join("");
        assert!(
            joined.contains("done"),
            "iteration {i}: expected final text containing done; texts={texts:?} labels={labels:?} kind={:?}",
            completion.end.kind
        );
        assert_eq!(
            completion.end.kind,
            TransactionEndKind::Completed,
            "iteration {i}: must Complete; got {:?} labels={labels:?} texts={texts:?}",
            completion.end.kind
        );
        finish_http_test(started, rt, vec![server]);
    }
}

/// Same provider call id across sequential admits yields distinct action ids.
#[test]
fn reused_provider_call_id_across_exchanges_distinct_action_ids() {
    let _guard = suite_lock();
    let rt = test_rt();
    let script =
        Arc::new(|_body: String| fragmented_tool_call_sse("call_reuse", "echo", r#"{"q":"x"}"#));
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let started = start_openai_runtime_with_tools(
        vec![openai_http_channel("openai-reuse", endpoint, "gpt-test")],
        echo_tool_registry(),
    );
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

    finish_http_test(started, rt, vec![server]);
}

/// HTTP twin: context-byte ceiling fails closed before the second open.
#[test]
fn http_inline_continuation_context_bytes_limit_exceeded() {
    let _guard = suite_lock();
    let rt = test_rt();
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let script = Arc::new(move |_body: String| {
        let n = posts_c.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#)
        } else {
            fragmented_text_sse("should never be reached")
        }
    });
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    limits.max_continuation_context_bytes = 1;
    let started = start_openai_runtime_with_tools_and_limits(
        vec![openai_http_channel_with_policies(
            "openai-ctx",
            endpoint,
            "gpt-test",
            BTreeSet::from([
                ContinuationPolicy::CallerControlled,
                ContinuationPolicy::InlineToolContinuation,
            ]),
        )],
        echo_tool_registry(),
        limits,
    );
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-ctx").unwrap(),
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
        1,
        "context-byte ceiling must fail before continuation open; kind={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "max_continuation_context_bytes overflow must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );

    finish_http_test(started, rt, vec![server]);
}

fn sse_byte_len(chunks: &[bytes::Bytes]) -> usize {
    chunks.iter().map(|c| c.len()).sum()
}

/// HTTP twin: `max_provider_exchanges = 2` exact then LimitExceeded.
#[test]
fn http_inline_max_provider_exchanges_two_exact_then_limit_exceeded() {
    let _guard = suite_lock();
    let rt = test_rt();
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let script = Arc::new(move |_body: String| {
        let n = posts_c.fetch_add(1, Ordering::SeqCst);
        match n {
            0 => fragmented_tool_call_sse("call_a", "echo", r#"{"q":"a"}"#),
            1 => fragmented_tool_call_sse("call_b", "echo", r#"{"q":"b"}"#),
            _ => fragmented_text_sse("should never be reached"),
        }
    });
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    limits.max_provider_exchanges = 2;
    let started = start_openai_runtime_with_tools_and_limits(
        vec![openai_http_channel_with_policies(
            "openai-pex2",
            endpoint,
            "gpt-test",
            BTreeSet::from([
                ContinuationPolicy::CallerControlled,
                ContinuationPolicy::InlineToolContinuation,
            ]),
        )],
        echo_tool_registry(),
        limits,
    );
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-pex2").unwrap(),
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

    let (completion, texts, labels) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        posts.load(Ordering::SeqCst),
        2,
        "max_provider_exchanges=2 allows initial+one continuation only; kind={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind
    );
    assert!(
        !texts.iter().any(|t| t.contains("should never be reached")),
        "third POST must not run; texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "exact exchange ceiling must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );

    finish_http_test(started, rt, vec![server]);
}

/// HTTP twin: cumulative remaining-output plus-one — second open starts, then
/// mid-pump LimitExceeded without publishing complete second-round text.
#[test]
fn http_inline_cumulative_output_budget_plus_one_fails_second_pump() {
    let _guard = suite_lock();
    let rt = test_rt();
    let round0 = fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#);
    let round1 = fragmented_text_sse("second round text that must overflow remaining.");
    let first_out = sse_byte_len(&round0);
    let second_out = sse_byte_len(&round1);
    assert!(second_out > 1, "second SSE must exceed a 1-byte remainder");
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let round0_c = round0.clone();
    let round1_c = round1.clone();
    let script = Arc::new(move |_body: String| {
        let n = posts_c.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            round0_c.clone()
        } else {
            round1_c.clone()
        }
    });
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    limits.max_total_provider_output_bytes = first_out.saturating_add(1);
    let started = start_openai_runtime_with_tools_and_limits(
        vec![openai_http_channel_with_policies(
            "openai-cum-out-plus",
            endpoint,
            "gpt-test",
            BTreeSet::from([
                ContinuationPolicy::CallerControlled,
                ContinuationPolicy::InlineToolContinuation,
            ]),
        )],
        echo_tool_registry(),
        limits,
    );
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-cum-out-plus").unwrap(),
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
        "plus-one remainder must allow the second open to start; kind={:?} labels={labels:?}",
        completion.end.kind
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

    finish_http_test(started, rt, vec![server]);
}

/// HTTP twin: first exchange consumes full output ceiling; second open blocked.
#[test]
fn http_inline_cumulative_output_budget_exhausted_blocks_second_open() {
    let _guard = suite_lock();
    let rt = test_rt();
    let round0 = fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#);
    let first_out = sse_byte_len(&round0);
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let round0_c = round0.clone();
    let script = Arc::new(move |_body: String| {
        let n = posts_c.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            round0_c.clone()
        } else {
            fragmented_text_sse("should never be reached")
        }
    });
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    limits.max_total_provider_output_bytes = first_out;
    let started = start_openai_runtime_with_tools_and_limits(
        vec![openai_http_channel_with_policies(
            "openai-cum-out",
            endpoint,
            "gpt-test",
            BTreeSet::from([
                ContinuationPolicy::CallerControlled,
                ContinuationPolicy::InlineToolContinuation,
            ]),
        )],
        echo_tool_registry(),
        limits,
    );
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-cum-out").unwrap(),
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
        1,
        "exhausted remaining_output must block continuation open; kind={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "cumulative output exhaustion must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );

    finish_http_test(started, rt, vec![server]);
}

/// HTTP twin: `max_provider_exchanges = 1` blocks Inline continuation.
#[test]
fn http_inline_max_provider_exchanges_one_ends_limit_exceeded() {
    let _guard = suite_lock();
    let rt = test_rt();
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let script = Arc::new(move |_body: String| {
        let n = posts_c.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#)
        } else {
            fragmented_text_sse("should never be reached")
        }
    });
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let mut limits = default_tx_limits();
    limits.max_continuations = 8;
    limits.max_provider_exchanges = 1;
    let started = start_openai_runtime_with_tools_and_limits(
        vec![openai_http_channel_with_policies(
            "openai-pex1",
            endpoint,
            "gpt-test",
            BTreeSet::from([
                ContinuationPolicy::CallerControlled,
                ContinuationPolicy::InlineToolContinuation,
            ]),
        )],
        echo_tool_registry(),
        limits,
    );
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-pex1").unwrap(),
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
        1,
        "max_provider_exchanges=1 must not open a continuation; kind={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "provider exchange ceiling must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );

    finish_http_test(started, rt, vec![server]);
}

/// HTTP twin: tiny total provider input fails closed before the HTTP POST.
#[test]
fn http_total_provider_input_bytes_limit_exceeded_before_open() {
    let _guard = suite_lock();
    let rt = test_rt();
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let script = Arc::new(move |_body: String| {
        posts_c.fetch_add(1, Ordering::SeqCst);
        fragmented_text_sse("should never be reached")
    });
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let mut limits = default_tx_limits();
    limits.max_total_provider_input_bytes = 10;
    let started = start_openai_runtime_with_tools_and_limits(
        vec![openai_http_channel("openai-pin", endpoint, "gpt-test")],
        HostToolRegistry::empty(),
        limits,
    );
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-pin").unwrap(),
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

    let (completion, texts, labels) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        posts.load(Ordering::SeqCst),
        0,
        "total provider input ceiling must fail before HTTP open; kind={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "total provider input overflow must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );

    finish_http_test(started, rt, vec![server]);
}

/// HTTP twin: tiny total provider output fails closed during SSE pump.
#[test]
fn http_total_provider_output_bytes_limit_exceeded() {
    let _guard = suite_lock();
    let rt = test_rt();
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let script = Arc::new(move |_body: String| {
        posts_c.fetch_add(1, Ordering::SeqCst);
        fragmented_text_sse("Hello from oversized HTTP SSE output.")
    });
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let mut limits = default_tx_limits();
    limits.max_total_provider_output_bytes = 1;
    let started = start_openai_runtime_with_tools_and_limits(
        vec![openai_http_channel("openai-pout", endpoint, "gpt-test")],
        HostToolRegistry::empty(),
        limits,
    );
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-pout").unwrap(),
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
    assert_eq!(
        posts.load(Ordering::SeqCst),
        1,
        "output ceiling is checked after open during pump; kind={:?} labels={labels:?}",
        completion.end.kind
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "total provider output overflow must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind,
        completion.end.diagnostics
    );

    finish_http_test(started, rt, vec![server]);
}

/// HTTP twin: `max_continuations = 0` ends LimitExceeded without a second open.
#[test]
fn http_inline_max_continuations_zero_ends_limit_exceeded() {
    let _guard = suite_lock();
    let rt = test_rt();
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let script = Arc::new(move |_body: String| {
        posts_c.fetch_add(1, Ordering::SeqCst);
        fragmented_tool_call_sse("call_1", "echo", r#"{"q":"hi"}"#)
    });
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let mut limits = default_tx_limits();
    limits.max_continuations = 0;
    let started = start_openai_runtime_with_tools_and_limits(
        vec![openai_http_channel_with_policies(
            "openai-max0",
            endpoint,
            "gpt-test",
            BTreeSet::from([
                ContinuationPolicy::CallerControlled,
                ContinuationPolicy::InlineToolContinuation,
            ]),
        )],
        echo_tool_registry(),
        limits,
    );
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-max0").unwrap(),
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
        1,
        "max_continuations=0 must not open a continuation; kind={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "zero continuation ceiling must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );

    finish_http_test(started, rt, vec![server]);
}

/// HTTP twin: `max_continuations = 1` with a second tool response → LimitExceeded.
#[test]
fn http_inline_max_continuations_one_exhausted_ends_limit_exceeded() {
    let _guard = suite_lock();
    let rt = test_rt();
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_c = Arc::clone(&posts);
    let script = Arc::new(move |_body: String| {
        let n = posts_c.fetch_add(1, Ordering::SeqCst);
        match n {
            0 => fragmented_tool_call_sse("call_a", "echo", r#"{"q":"a"}"#),
            1 => fragmented_tool_call_sse("call_b", "echo", r#"{"q":"b"}"#),
            _ => fragmented_text_sse("should never be reached"),
        }
    });
    let server = rt.block_on(bind_fragmented_openai_sse(script));
    let endpoint = server.endpoint();

    let mut limits = default_tx_limits();
    limits.max_continuations = 1;
    let started = start_openai_runtime_with_tools_and_limits(
        vec![openai_http_channel_with_policies(
            "openai-max1",
            endpoint,
            "gpt-test",
            BTreeSet::from([
                ContinuationPolicy::CallerControlled,
                ContinuationPolicy::InlineToolContinuation,
            ]),
        )],
        echo_tool_registry(),
        limits,
    );
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("openai-max1").unwrap(),
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

    let (completion, texts, labels) = rt.block_on(drain_until_completed(receiver));
    assert_eq!(
        posts.load(Ordering::SeqCst),
        2,
        "max_continuations=1 allows one continuation open only; kind={:?} labels={labels:?} texts={texts:?}",
        completion.end.kind
    );
    assert!(
        !texts.iter().any(|t| t.contains("should never be reached")),
        "exhausted ceiling must not open a third exchange; texts={texts:?}"
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::LimitExceeded,
        "exhausted max_continuations must be LimitExceeded; got {:?} diagnostics={:?} labels={labels:?}",
        completion.end.kind,
        completion.end.diagnostics
    );

    finish_http_test(started, rt, vec![server]);
}
