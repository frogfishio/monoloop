//! WP-10: direct-LLM vertical integration through public TransactionRuntime.
//!
//! Uses StreamingHttpConnector + OpenAI Chat Completions encoder/Interpreter
//! against a local scripted SSE server (no live provider).

use axum::body::Body;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use monoloop_connector::{
    AnonymousCredentialResolver, StreamingHttpConfig, StreamingHttpConnectorFactory,
};
use monoloop_contracts::{
    user_text_input, ChannelCapabilities, ChannelDefaults, ChannelId, ChannelKind, ChannelLimits,
    ContinuationPolicy, DialectBinding, DialectDescriptor, ExchangeMode, FnCompletionCallback,
    FnEventSink, InvocationConfig, JsonSchema, McpConfigurationCapability, McpReachability,
    SessionMode, ToolCancellationPolicy, ToolCompletion, ToolExecutionMode, ToolId, ToolLimits,
    ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionEnd,
    TransactionEndKind, TransactionEvent, TransactionEventPayload, TransactionRequest,
    TransactionRuntime, CanonicalToolOutput,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, DefaultTransactionRuntime, HostToolRegistry,
    ImmediateToolHandler, OpenAiChatCompletionsEncoder, OpenAiEncoderOptions, RegisteredTool,
    RuntimeBootstrap, RuntimeConfig, ToolHandler,
};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

async fn bind_sse_server(
    script: Arc<dyn Fn(usize, String) -> String + Send + Sync>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/v1/chat/completions",
        post(move |req: Request| {
            let script = Arc::clone(&script);
            let hits = Arc::clone(&hits);
            async move {
                let n = hits.fetch_add(1, Ordering::SeqCst);
                let body = axum::body::to_bytes(req.into_body(), 2 * 1024 * 1024)
                    .await
                    .unwrap_or_default();
                let body_str = String::from_utf8_lossy(&body).into_owned();
                let sse = script(n, body_str);
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(sse))
                    .unwrap()
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let join = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    tokio::time::sleep(Duration::from_millis(15)).await;
    (addr, join)
}

fn openai_channel(
    id: &str,
    endpoint: String,
    continuation: BTreeSet<ContinuationPolicy>,
    model: &str,
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
            continuation_policies: continuation,
            supports_distinct_session_concurrency: true,
            input_dialect: d.clone(),
            output_dialect: d,
        },
        limits: ChannelLimits::default(),
    }
}

fn text_sse(content: &str) -> String {
    format!(
        "data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"role\":\"assistant\",\"content\":{content}}}}}]}}\n\n\
         data: {{\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"stop\"}}]}}\n\n\
         data: [DONE]\n\n",
        content = serde_json::to_string(content).unwrap()
    )
}

fn tool_call_sse() -> String {
    let mut s = String::new();
    s.push_str(
        r#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"echo","arguments":"{\"q\":"}}]}}]}"#,
    );
    s.push_str("\n\n");
    s.push_str(
        r#"data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"hi\"}"}}]},"finish_reason":"tool_calls"}]}"#,
    );
    s.push_str("\n\n");
    s.push_str("data: [DONE]\n\n");
    s
}

fn echo_tool() -> RegisteredTool {
    let schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "q": { "type": "string" } },
        "required": ["q"],
        "additionalProperties": false
    }))
    .unwrap();
    let out = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "ok": { "type": "boolean" } },
        "required": ["ok"],
        "additionalProperties": false
    }))
    .unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo tool",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits::default(),
        ToolCancellationPolicy::Abortable,
    )
    .unwrap();
    RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(CanonicalToolOutput::Json(
                serde_json::json!({"ok": true}),
            )))
        })) as Arc<dyn ToolHandler>,
    )
}

async fn run_tx(
    rt: &DefaultTransactionRuntime,
    channel: &str,
    text: &str,
    tools: Vec<ToolId>,
    policy: ContinuationPolicy,
) -> (TransactionEndKind, Vec<TransactionEvent>) {
    let events = Arc::new(Mutex::new(Vec::<TransactionEvent>::new()));
    let done = Arc::new(Notify::new());
    let end_kind = Arc::new(Mutex::new(TransactionEndKind::InvariantFailed));

    let events_s = Arc::clone(&events);
    let sink: Arc<dyn monoloop_contracts::TransactionEventSink> =
        Arc::new(FnEventSink(move |e| {
            let events_s = Arc::clone(&events_s);
            Box::pin(async move {
                events_s.lock().unwrap().push(e);
                Ok(())
            }) as monoloop_contracts::EventDelivery
        }));

    let done_s = Arc::clone(&done);
    let end_s = Arc::clone(&end_kind);
    let completion: Box<dyn monoloop_contracts::CompletionCallback> =
        Box::new(FnCompletionCallback(move |end: TransactionEnd| {
            let done_s = Arc::clone(&done_s);
            let end_s = Arc::clone(&end_s);
            Box::pin(async move {
                *end_s.lock().unwrap() = end.kind;
                done_s.notify_waiters();
                Ok(())
            }) as monoloop_contracts::CompletionDelivery
        }));

    TransactionRuntime::submit(
        rt,
        TransactionRequest {
            channel_id: ChannelId::try_new(channel).unwrap(),
            session_id: None,
            input: user_text_input(text).unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(15)),
                continuation_policy: policy,
                model: None, // use channel defaults
                ..Default::default()
            },
            tools,
            events: sink,
            completion,
        },
    )
    .unwrap();

    tokio::time::timeout(Duration::from_secs(10), done.notified())
        .await
        .expect("transaction completed");
    let kind = *end_kind.lock().unwrap();
    let evs = events.lock().unwrap().clone();
    (kind, evs)
}

#[tokio::test]
async fn text_only_transaction() {
    let script = Arc::new(|_n: usize, _body: String| text_sse("Hello from direct LLM."));
    let (addr, join) = bind_sse_server(script).await;
    let endpoint = format!("http://{addr}/v1/chat/completions");

    let rt = DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![openai_channel(
            "openai-a",
            endpoint,
            BTreeSet::from([ContinuationPolicy::CallerControlled]),
            "gpt-test",
        )])
        .unwrap(),
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap();

    let (kind, evs) = run_tx(
        rt.as_ref(),
        "openai-a",
        "Say hi.",
        vec![],
        ContinuationPolicy::CallerControlled,
    )
    .await;
    assert_eq!(kind, TransactionEndKind::Completed);
    let units = evs
        .iter()
        .filter(|e| matches!(e.payload, TransactionEventPayload::CanonicalUnit(_)))
        .count();
    assert!(units > 0, "expected canonical text units");
    assert!(evs
        .iter()
        .any(|e| matches!(e.payload, TransactionEventPayload::Ended(_))));

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    join.abort();
}

#[tokio::test]
async fn caller_controlled_tool_exchange_is_continuation_required() {
    let script = Arc::new(|n: usize, body: String| {
        if n == 0 {
            assert!(body.contains("\"stream\":true"));
            tool_call_sse()
        } else {
            // Must not open a second provider exchange under CallerControlled.
            panic!("unexpected second provider exchange under CallerControlled");
        }
    });
    let (addr, join) = bind_sse_server(script).await;
    let endpoint = format!("http://{addr}/v1/chat/completions");

    let tools = HostToolRegistry::build(vec![echo_tool()]).unwrap();
    let rt = DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![openai_channel(
            "openai-cc",
            endpoint,
            BTreeSet::from([ContinuationPolicy::CallerControlled]),
            "gpt-test",
        )])
        .unwrap(),
        tools,
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap();

    let (kind, evs) = run_tx(
        rt.as_ref(),
        "openai-cc",
        "Use echo.",
        vec![ToolId::try_new("echo").unwrap()],
        ContinuationPolicy::CallerControlled,
    )
    .await;
    assert_eq!(kind, TransactionEndKind::ContinuationRequired);
    assert!(evs.iter().any(|e| matches!(
        e.payload,
        TransactionEventPayload::ToolLifecycle(_)
    )));

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    join.abort();
}

#[tokio::test]
async fn inline_tool_continuation_second_exchange() {
    let script = Arc::new(|n: usize, body: String| match n {
        0 => tool_call_sse(),
        1 => {
            assert!(
                body.contains("\"role\":\"tool\"") || body.contains("call_1"),
                "continuation should include tool result: {body}"
            );
            text_sse("Tool finished successfully.")
        }
        _ => panic!("too many exchanges"),
    });
    let (addr, join) = bind_sse_server(script).await;
    let endpoint = format!("http://{addr}/v1/chat/completions");

    let tools = HostToolRegistry::build(vec![echo_tool()]).unwrap();
    let rt = DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![openai_channel(
            "openai-inline",
            endpoint,
            BTreeSet::from([
                ContinuationPolicy::CallerControlled,
                ContinuationPolicy::InlineToolContinuation,
            ]),
            "gpt-test",
        )])
        .unwrap(),
        tools,
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap();

    let (kind, _evs) = run_tx(
        rt.as_ref(),
        "openai-inline",
        "Use echo.",
        vec![ToolId::try_new("echo").unwrap()],
        ContinuationPolicy::InlineToolContinuation,
    )
    .await;
    assert_eq!(kind, TransactionEndKind::Completed);

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    join.abort();
}

#[tokio::test]
async fn two_profiles_same_impl_no_name_branch() {
    let script = Arc::new(|_n: usize, _b: String| text_sse("ok."));
    let (addr, join) = bind_sse_server(script).await;
    let endpoint = format!("http://{addr}/v1/chat/completions");

    let rt = DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![
            openai_channel(
                "provider-alpha",
                endpoint.clone(),
                BTreeSet::from([ContinuationPolicy::CallerControlled]),
                "model-alpha",
            ),
            openai_channel(
                "provider-beta",
                endpoint,
                BTreeSet::from([ContinuationPolicy::CallerControlled]),
                "model-beta",
            ),
        ])
        .unwrap(),
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap();

    let (k1, _) = run_tx(
        rt.as_ref(),
        "provider-alpha",
        "a",
        vec![],
        ContinuationPolicy::CallerControlled,
    )
    .await;
    let (k2, _) = run_tx(
        rt.as_ref(),
        "provider-beta",
        "b",
        vec![],
        ContinuationPolicy::CallerControlled,
    )
    .await;
    assert_eq!(k1, TransactionEndKind::Completed);
    assert_eq!(k2, TransactionEndKind::Completed);

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    join.abort();
}

#[tokio::test]
async fn concurrent_direct_llm_isolated() {
    use std::sync::atomic::AtomicBool;

    let script = Arc::new(|_n: usize, body: String| {
        // Echo a marker from the user message if present.
        if body.contains("msg-a") {
            text_sse("reply-a.")
        } else if body.contains("msg-b") {
            text_sse("reply-b.")
        } else {
            text_sse("reply-other.")
        }
    });
    let (addr, join) = bind_sse_server(script).await;
    let endpoint = format!("http://{addr}/v1/chat/completions");

    let rt = DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![
            openai_channel(
                "c-a",
                endpoint.clone(),
                BTreeSet::from([ContinuationPolicy::CallerControlled]),
                "m",
            ),
            openai_channel(
                "c-b",
                endpoint,
                BTreeSet::from([ContinuationPolicy::CallerControlled]),
                "m",
            ),
        ])
        .unwrap(),
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap();

    let mk = |ch: &str, msg: &str| {
        let finished = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());
        let f2 = Arc::clone(&finished);
        let n2 = Arc::clone(&notify);
        let sink: Arc<dyn monoloop_contracts::TransactionEventSink> =
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            }));
        let completion: Box<dyn monoloop_contracts::CompletionCallback> =
            Box::new(FnCompletionCallback(move |end: TransactionEnd| {
                let f2 = Arc::clone(&f2);
                let n2 = Arc::clone(&n2);
                Box::pin(async move {
                    assert_eq!(end.kind, TransactionEndKind::Completed);
                    f2.store(true, Ordering::SeqCst);
                    n2.notify_waiters();
                    Ok(())
                }) as monoloop_contracts::CompletionDelivery
            }));
        let req = TransactionRequest {
            channel_id: ChannelId::try_new(ch).unwrap(),
            session_id: None,
            input: user_text_input(msg).unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(15)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            events: sink,
            completion,
        };
        (req, finished, notify)
    };

    let (r1, f1, n1) = mk("c-a", "msg-a please.");
    let (r2, f2, n2) = mk("c-b", "msg-b please.");
    TransactionRuntime::submit(rt.as_ref(), r1).unwrap();
    TransactionRuntime::submit(rt.as_ref(), r2).unwrap();

    let wait = |f: Arc<AtomicBool>, n: Arc<Notify>| async move {
        while !f.load(Ordering::SeqCst) {
            n.notified().await;
        }
    };
    tokio::time::timeout(Duration::from_secs(15), wait(f1, n1))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), wait(f2, n2))
        .await
        .unwrap();

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    join.abort();
}
