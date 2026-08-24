//! Lifecycle unit tests (composed; see `mod.rs`).

use super::super::{StartedRuntime, TransactionRuntimeHandle};
use super::common::*;
use crate::transaction::bootstrap::{
    ControlHoldGate, FinalizerHoldGate, JoinOnlySpillInject, RuntimeBootstrap, RuntimeConfig,
    StartHoldGate, StoppedGate,
};
use crate::transaction::channel_registry::{ChannelBinding, ChannelRegistry};
use crate::transaction::fake_support::PanicEncoder;
use crate::transaction::fake_support::TestTextEncoder;
use crate::transaction::host_tools::HostToolRegistry;
use crate::transaction::state::RuntimeState;
use monoloop_connector::{FakeConnectorConfig, FakeConnectorFactory, FakeEndpoint};
use monoloop_contracts::{
    transaction_delivery, user_text_input, AdmissionError, AdmissionErrorKind, AdmissionReceipt,
    CancellationReason, CancellationReasonCode, ChannelCapabilities, ChannelDefaults, ChannelId,
    ChannelKind, ChannelLimits, ContinuationPolicy, DeliveryLimits, DialectDescriptor,
    ExchangeMode, InvocationConfig, McpConfigurationCapability, McpReachability, OptionPolicy,
    SessionId, SessionMode, ShutdownWaitOutcome, TerminationDisposition, TerminationMode,
    TerminationReason, TerminationReasonCode, ToolExecutionMode, TransactionEndKind, TransactionId,
    TransactionLimits, TransactionReceiver, TransactionSelector, TransactionSubmitRequest,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

/// D-043 / §17 / §7.1: MCP handle published before start returns; RuntimeService
/// joins before Stopped.
#[test]
fn mcp_listener_owned_shutdown_reaches_stopped() {
    let started = start_runtime_with_mcp(2, 2, true);
    let handle = started.handle.clone();
    // §7.1: start returns only after gateway handle/addr are published.
    let addr = handle
        .mcp_local_addr()
        .expect("MCP loopback addr published before start returns");
    assert!(addr.ip().is_loopback());
    assert!(
        handle.mcp_gateway().is_some(),
        "MCP gateway handle published before start returns"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        // Handle is ready at start return; serve may need one supervisor poll.
        // Retry connect only — not a publication poll (§7.1 already asserted).
        let url = format!(
            "http://{addr}/mcp/deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
        let client = reqwest::Client::new();
        let mut resp = None;
        for _ in 0..50 {
            match client.get(&url).send().await {
                Ok(r) => {
                    resp = Some(r);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        let resp = resp.expect("HTTP to live MCP gateway");
        assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

        owner.begin_shutdown();
        let stopped = owner.wait_stopped(Duration::from_secs(3)).await;
        assert!(
            owner.mcp_local_addr().is_none(),
            "MCP addr cleared after Stopped"
        );
        assert!(
            owner.mcp_gateway().is_none(),
            "MCP handle cleared after Stopped"
        );
        stopped
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped with MCP joined, got {outcome:?}"
    );
}

fn external_agent_binding(id: &str, channel_max: usize) -> ChannelBinding {
    external_agent_binding_with_session(id, channel_max, Default::default())
}

fn hang_external_agent_binding(id: &str, channel_max: usize) -> ChannelBinding {
    let mut binding = external_agent_binding_with_session_and_connector(
        id,
        channel_max,
        Default::default(),
        FakeConnectorConfig {
            default_endpoint: FakeEndpoint::Hang,
            ..FakeConnectorConfig::default()
        },
    );
    binding.limits.max_distinct_sessions = channel_max;
    binding
}

fn external_agent_binding_with_session(
    id: &str,
    channel_max: usize,
    session_config: monoloop_connector::FakeSessionAdapterConfig,
) -> ChannelBinding {
    external_agent_binding_with_session_and_connector(
        id,
        channel_max,
        session_config,
        FakeConnectorConfig::default(),
    )
}

fn external_agent_binding_with_session_and_connector(
    id: &str,
    channel_max: usize,
    session_config: monoloop_connector::FakeSessionAdapterConfig,
    connector_config: FakeConnectorConfig,
) -> ChannelBinding {
    let d = DialectDescriptor::test_raw();
    ChannelBinding {
        id: ChannelId::try_new(id).unwrap(),
        kind: ChannelKind::ExternalAgent,
        tool_mode: ToolExecutionMode::McpGateway,
        connector_factory: Arc::new(FakeConnectorFactory::external_agent_with_connector_config(
            session_config,
            connector_config,
        )),
        encoder: Arc::new(TestTextEncoder),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "default".into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: ChannelCapabilities {
            session_mode: SessionMode::External,
            mcp_configuration: McpConfigurationCapability::CreationOnly,
            mcp_reachability: McpReachability::SameLoopbackNamespace,
            exchange_mode: ExchangeMode::Bidirectional,
            continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
            supports_distinct_session_concurrency: true,
            input_dialect: d.clone(),
            output_dialect: d,
            option_policy: OptionPolicy::external_agent(),
        },
        limits: ChannelLimits {
            max_active_transactions: channel_max,
            ..ChannelLimits::default()
        },
    }
}

/// D-015 claim-time: ExternalAgent `session_id: None` admits, then
/// `bind_session` enforces `max_distinct_sessions` → `LimitExceeded`.
///
/// Distinct from admit-time Hang DirectLlm
/// `max_distinct_sessions_exact_admits_plus_one_rejects`: first two creates
/// claim successfully (Hang-pinned); the third admits without a SessionKey
/// then fails closed at claim with `LimitExceeded` (not `InvariantFailed`).
#[test]
fn external_agent_claim_time_distinct_sessions_plus_one_limit_exceeded() {
    let distinct_max = 2usize;
    let limits = TransactionLimits {
        max_active_transactions: 8,
        max_active_per_channel: 8,
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let mut binding = hang_external_agent_binding("agent", 8);
    binding.limits.max_distinct_sessions = distinct_max;
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            enable_mcp_listener: false,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![binding]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut held = Vec::new();
    for i in 0..distinct_max {
        let (delivery, mut recv) =
            transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
        handle
            .submit(TransactionSubmitRequest {
                channel_id: ChannelId::try_new("agent").unwrap(),
                session_id: None,
                input: user_text_input(format!("hold-{i}")).unwrap(),
                session_config: None,
                invocation_config: InvocationConfig::default(),
                tools: vec![],
                delivery,
            })
            .unwrap_or_else(|e| panic!("create {i} must admit: {e:?}"));
        let established = rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while let Some(ev) = recv.events.recv().await {
                    if matches!(
                        ev.payload,
                        monoloop_contracts::TransactionEventPayload::SessionEstablished { .. }
                    ) {
                        return true;
                    }
                }
                false
            })
            .await
            .unwrap_or(false)
        });
        assert!(
            established,
            "create {i} must claim SessionKey before Hang holds"
        );
        held.push(recv);
    }
    assert_eq!(
        started.owner.ledger_len(),
        distinct_max,
        "claimed creates remain Hang-pinned in ledger"
    );

    // Third create: admit succeeds (no SessionKey yet); claim fails LimitExceeded.
    let (delivery, overflow) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: None,
            input: user_text_input("overflow").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect("third create must admit before claim");

    let overflow_kind = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), overflow.completion.recv())
            .await
            .expect("overflow completion timed out")
            .expect("overflow completion channel")
            .end
            .kind
    });
    assert_eq!(
        overflow_kind,
        TransactionEndKind::LimitExceeded,
        "claim-time distinct overflow must be LimitExceeded, not InvariantFailed"
    );
    // Overflow leaves the ledger after terminal cleanup; held creates remain.
    let drained = rt.block_on(async {
        for _ in 0..100 {
            if started.owner.ledger_len() == distinct_max {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    });
    assert!(
        drained,
        "overflow must leave ledger; held creates stay, got len={}",
        started.owner.ledger_len()
    );

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(10)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
    for recv in held {
        let _ = rt.block_on(recv.completion.recv());
    }
}

/// ExternalAgent empty-tool path: attach → open → EstablishExternal before prompt → Completed.
#[test]
fn external_agent_empty_tools_establishes_session_and_completes() {
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            enable_mcp_listener: true,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_binding("agent", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    assert!(
        started.handle.mcp_gateway().is_some(),
        "§7.1 gateway published at start"
    );
    let handle = started.handle.clone();
    let (delivery, mut recv) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let receipt = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect("admit");
    let _ = receipt;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let kind = rt.block_on(async {
        let mut saw_established = false;
        let mut end_kind = None;
        while let Some(ev) = recv.events.recv().await {
            match &ev.payload {
                monoloop_contracts::TransactionEventPayload::SessionEstablished { .. } => {
                    saw_established = true;
                }
                monoloop_contracts::TransactionEventPayload::EndedEvent(term) => {
                    end_kind = Some(term.kind);
                    break;
                }
                _ => {}
            }
        }
        let completion = recv.completion.recv().await.expect("completion");
        assert!(saw_established, "SessionEstablished before end");
        assert_eq!(completion.end.kind, end_kind.unwrap());
        completion.end.kind
    });
    assert_eq!(kind, TransactionEndKind::Completed);
    let mut owner = started.owner;
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// §17: spawn Rejected (closed mailbox) fail closed with 503 (no ambient inline drive).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_mcp_owner_returns_503_when_spawn_rejected() {
    use super::super::mcp_request_owner::SupervisedMcpRequestOwner;
    use super::super::task_spawner::TransactionTaskSpawner;
    use crate::transaction::mcp::McpRequestOwner;
    use axum::body::Body;
    use axum::http::{Response, StatusCode};
    use monoloop_contracts::TransactionId;

    let (spawner, spawn_rx) = TransactionTaskSpawner::channel(1);
    drop(spawn_rx); // supervisor gone → Rejected
    let owner = SupervisedMcpRequestOwner::new(spawner);
    let resp = owner
        .run_owned(
            TransactionId::generate(),
            Box::pin(async { Response::new(Body::from("unused")) }),
        )
        .await;
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "Rejected spawn must fail closed with 503"
    );
}

/// D-051 reopen: Busy/Rejected unregistered owner work must be dropped, never polled.
///
/// Mirrors `exchange.rs` ConnectorOwner Busy/Rejected arms: terminate path returns
/// the boxed future to the caller, which drops it without `await`/`poll`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d051_busy_rejected_drops_unregistered_owner_without_poll() {
    use super::super::task_spawner::{SpawnReject, TransactionTaskSpawner};
    use super::super::task_supervisor::TaskClass;
    use monoloop_contracts::ExchangeId;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct PanicOnPoll;
    impl Future for PanicOnPoll {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            panic!("D-051: unregistered ConnectorOwner work must not be polled");
        }
    }

    let class = || TaskClass::ConnectorOwner(TransactionId::generate(), ExchangeId::generate());

    // Rejected: closed mailbox returns the future undriven.
    {
        let (spawner, spawn_rx) = TransactionTaskSpawner::channel(1);
        drop(spawn_rx);
        match spawner.spawn(class(), PanicOnPoll).await {
            Err(SpawnReject::Rejected { future }) => drop(future),
            Err(SpawnReject::Busy { .. }) => panic!("expected Rejected on closed mailbox"),
            Err(SpawnReject::Orphaned) => panic!("expected Rejected on closed mailbox"),
            Ok(_) => panic!("expected Rejected on closed mailbox"),
        }
    }

    // Busy: capacity-1 mailbox occupied by an undrained request.
    {
        let (spawner, _spawn_rx) = TransactionTaskSpawner::channel(1);
        let occupy = {
            let s = spawner.clone();
            let occupy_class = class();
            tokio::spawn(async move {
                let _ = s.spawn(occupy_class, std::future::pending::<()>()).await;
            })
        };
        // Let the occupy attempt fill the mailbox (try_send succeeds; reply waits).
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        match spawner.spawn(class(), PanicOnPoll).await {
            Err(SpawnReject::Busy { future }) => drop(future),
            Err(SpawnReject::Rejected { .. }) => panic!("expected Busy after full mailbox"),
            Err(SpawnReject::Orphaned) => panic!("expected Busy after full mailbox"),
            Ok(_) => panic!("expected Busy after full mailbox"),
        }
        occupy.abort();
    }
}

/// RuntimeOwner injects SupervisedMcpRequestOwner onto the live gateway.
///
/// TaskClass::McpRequest observation is proven by
/// `mcp_http_request_registers_task_class_mcp_request` (instrumented pump).
/// This test proves StartedRuntime injection + live supervisor accept (non-503)
/// and that RuntimeService is registered before HTTP.
#[test]
fn runtime_owner_mcp_http_uses_supervised_request_owner() {
    use crate::transaction::dispatcher::TransactionToolDispatcher;
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::resolved_tools::ResolvedToolSet;
    use crate::transaction::tool_capacity::SharedToolCapacity;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        ExchangeId, JsonSchema, SessionKey, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits,
        ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionId,
    };

    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let tools = HostToolRegistry::build(vec![RegisteredTool::new(
        spec.clone(),
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    )])
    .unwrap();

    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: true,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_binding("agent", 2)]).unwrap(),
        tools,
    })
    .expect("start");
    let gw = started.handle.mcp_gateway().expect("injected gateway");
    let resolved = ResolvedToolSet::from_registered(vec![RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    )]);
    let tx = TransactionId::generate();
    let dispatcher = TransactionToolDispatcher::new(
        tx,
        SessionKey {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: SessionId::try_new("s1").unwrap(),
        },
        resolved.clone(),
        SharedToolCapacity::unlimited(),
        8,
        16,
    );
    let pending = gw
        .install_pending(tx, resolved, dispatcher, ExchangeId::generate())
        .unwrap();
    gw.activate(&pending.token).unwrap();

    let mut owner = started.owner;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let status = rt.block_on(async {
        for _ in 0..100 {
            if owner.owned_task_count() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            owner.owned_task_count() >= 1,
            "RuntimeService must be live under StartedRuntime before HTTP"
        );
        let url = format!("{}/mcp/{}", gw.base_url(), pending.token.to_hex());
        let mut last = None;
        for _ in 0..50 {
            match reqwest::Client::new().get(&url).send().await {
                Ok(r) => {
                    last = Some(r.status());
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        last.expect("HTTP through RuntimeOwner MCP path")
    });
    assert_ne!(
        status,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "live supervisor must accept McpRequest spawn (injection present)"
    );
    gw.revoke(&pending.token);
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// §17: SupervisedMcpRequestOwner registers HTTP work as TaskClass::McpRequest.
/// (Pump simulates TaskSupervisor drain; RuntimeOwner injects the same owner type.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_http_request_registers_task_class_mcp_request() {
    use super::super::mcp_request_owner::SupervisedMcpRequestOwner;
    use super::super::task_spawner::TransactionTaskSpawner;
    use super::super::task_supervisor::{TaskClass, TaskSupervisor};
    use crate::transaction::dispatcher::TransactionToolDispatcher;
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::mcp::McpGateway;
    use crate::transaction::resolved_tools::ResolvedToolSet;
    use crate::transaction::tool_capacity::SharedToolCapacity;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        ExchangeId, JsonSchema, SessionKey, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits,
        ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionId,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    let (spawner, mut spawn_rx) = TransactionTaskSpawner::channel(16);
    let saw_mcp_request = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&saw_mcp_request);
    let pump = tokio::spawn(async move {
        let mut tasks = TaskSupervisor::new();
        while let Some(req) = spawn_rx.recv().await {
            if matches!(req.class, TaskClass::McpRequest(_)) {
                flag.store(true, Ordering::SeqCst);
            }
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        let _ = tasks.abort_and_drain().await;
    });

    let owner: Arc<dyn crate::transaction::mcp::McpRequestOwner> =
        Arc::new(SupervisedMcpRequestOwner::new(spawner));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let prepared =
        McpGateway::prepare_from_tokio_listener(listener, 8, Some(Arc::clone(&owner))).unwrap();
    let addr = prepared.local_addr();
    let handle = prepared.handle();
    let serve = tokio::spawn(prepared.serve());

    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let registered = RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    );
    let resolved = ResolvedToolSet::from_registered(vec![registered]);
    let tx = TransactionId::generate();
    let dispatcher = TransactionToolDispatcher::new(
        tx,
        SessionKey::new(
            ChannelId::try_new("agent").unwrap(),
            SessionId::try_new("s1").unwrap(),
        ),
        resolved.clone(),
        SharedToolCapacity::unlimited(),
        8,
        16,
    );
    let pending = handle
        .install_pending(tx, resolved, dispatcher, ExchangeId::generate())
        .unwrap();
    handle.activate(&pending.token).unwrap();

    let url = format!("{}/mcp/{}", handle.base_url(), pending.token.to_hex());
    let resp = reqwest::Client::new().get(&url).send().await.expect("http");
    // Unknown method / MCP protocol may 4xx/2xx; ownership is what we assert.
    let _ = resp.status();
    assert!(
        saw_mcp_request.load(Ordering::SeqCst),
        "HTTP MCP dispatch must register TaskClass::McpRequest"
    );

    handle.revoke(&pending.token);
    // Drop serve by aborting join — prepared.cancel is inside serve task.
    serve.abort();
    let _ = serve.await;
    drop(owner);
    // Close spawner by dropping pump's rx when pump exits — drop pump after abort.
    pump.abort();
    let _ = pump.await;
    let _ = addr;
}

/// Attach failure after install_pending must revoke the MCP route (no leak to shutdown).
#[test]
fn mcp_route_revoked_when_attach_fails_after_install() {
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_connector::FakeSessionAdapterConfig;
    use monoloop_contracts::{
        JsonSchema, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits, ToolName,
        ToolOutputContract, ToolSpec, ToolSuccessContract,
    };

    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let tools = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    )])
    .unwrap();

    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: true,
            transaction_limits: TransactionLimits {
                max_active_transactions: 2,
                max_active_per_channel: 2,
                transaction_deadline: Duration::from_secs(2),
                cleanup_deadline: Duration::from_millis(500),
                ..TransactionLimits::default()
            },
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_binding_with_session(
            "agent",
            2,
            FakeSessionAdapterConfig {
                reject_begin_attach: true,
                ..Default::default()
            },
        )])
        .unwrap(),
        tools,
    })
    .expect("start");
    let gw = started.handle.mcp_gateway().expect("mcp gateway");
    let handle = started.handle.clone();
    let (delivery, mut recv) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let _ = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let kind = rt.block_on(async {
        let completion = recv.completion.recv().await.expect("completion");
        while let Some(_ev) = recv.events.recv().await {}
        completion.end.kind
    });
    assert_eq!(kind, TransactionEndKind::InvariantFailed);
    assert_eq!(
        gw.routes().len(),
        0,
        "MCP route must be revoked when attach fails after install"
    );
    let mut owner = started.owner;
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// D-026 / LAW 7: provisional MCP dispatcher SessionKey is rebound before activate.
#[test]
fn mcp_dispatcher_rebind_session_before_activate() {
    use super::super::session_identity::session_key_for;
    use crate::transaction::dispatcher::TransactionToolDispatcher;
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::mcp::McpGateway;
    use crate::transaction::resolved_tools::ResolvedToolSet;
    use crate::transaction::tool_capacity::SharedToolCapacity;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        ExchangeId, JsonSchema, SessionKey, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits,
        ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionId,
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        let spec = ToolSpec::try_new(
            ToolId::try_new("echo").unwrap(),
            ToolName::try_new("echo").unwrap(),
            "echo",
            schema,
            ToolOutputContract {
                success: ToolSuccessContract::json(out),
                error_data_schema: None,
            },
            ToolLimits {
                max_concurrent: 1,
                max_input_bytes: 256,
                max_output_bytes: 256,
                execution_deadline: Duration::from_secs(1),
            },
            ToolExecutionClass::CooperativeInProcess {
                grace: Duration::from_millis(50),
            },
        )
        .unwrap();
        let registered = RegisteredTool::new(
            spec,
            Arc::new(ImmediateToolHandler::new(|_c, _x| {
                Ok(ToolCompletion::Succeeded(
                    monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
                ))
            })),
        );
        let resolved = ResolvedToolSet::from_registered(vec![registered]);
        let tx = TransactionId::generate();
        let provisional = session_key_for(ChannelId::try_new("agent").unwrap(), None, tx);
        let dispatcher = TransactionToolDispatcher::new(
            tx,
            provisional.clone(),
            resolved.clone(),
            SharedToolCapacity::unlimited(),
            8,
            16,
        );
        assert_eq!(dispatcher.session_key(), provisional);
        assert!(
            provisional.session_id.as_str().starts_with("tx-"),
            "provisional key is transaction-scoped"
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback bind");
        let prepared = McpGateway::prepare_from_tokio_listener(listener, 8, None).unwrap();
        let gw = prepared.handle();
        let cancel = prepared.cancel_token();
        let join = tokio::spawn(prepared.serve());
        let pending = gw
            .install_pending(
                tx,
                resolved,
                Arc::clone(&dispatcher),
                ExchangeId::generate(),
            )
            .unwrap();
        let claimed = SessionKey {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: SessionId::try_new("fake-created-authoritative").unwrap(),
        };
        // Coordinator must rebind before activate (D-026).
        pending.dispatcher.rebind_session(claimed.clone());
        assert_eq!(pending.dispatcher.session_key(), claimed);
        assert_ne!(pending.dispatcher.session_key(), provisional);
        gw.activate(&pending.token).unwrap();
        gw.revoke(&pending.token);
        gw.revoke_all_services();
        cancel.cancel();
        let _ = join.await;
    });
}

/// D-014: CreationOnly rejects tool-enabled existing-session reuse at admission.
#[test]
fn creation_only_tool_reuse_rejected_at_admission() {
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        JsonSchema, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits, ToolName,
        ToolOutputContract, ToolSpec, ToolSuccessContract,
    };

    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let tools = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    )])
    .unwrap();

    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: true,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_binding("agent", 2)]).unwrap(),
        tools,
    })
    .expect("start");
    let handle = started.handle.clone();
    let (delivery, _recv) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: Some(SessionId::try_new("existing-session").unwrap()),
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect_err("CreationOnly tool reuse must fail at admission");
    assert_eq!(err.kind, AdmissionErrorKind::CapabilityMismatch);
    let mut owner = started.owner;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// CreationOnly: non-empty tools install pending MCP, activate before prompt, revoke after.
#[test]
fn creation_only_mcp_install_activate_revoke_round_trip() {
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        JsonSchema, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits, ToolName,
        ToolOutputContract, ToolSpec, ToolSuccessContract,
    };

    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let tools = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    )])
    .unwrap();

    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            enable_mcp_listener: true,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_binding("agent", 2)]).unwrap(),
        tools,
    })
    .expect("start");
    let gw = started.handle.mcp_gateway().expect("mcp gateway");
    let handle = started.handle.clone();
    let (delivery, mut recv) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let receipt = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit with tools");
    let _ = receipt;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let kind = rt.block_on(async {
        // Route may be live briefly during the turn; wait for terminal.
        let mut end_kind = None;
        while let Some(ev) = recv.events.recv().await {
            if let monoloop_contracts::TransactionEventPayload::EndedEvent(term) = &ev.payload {
                end_kind = Some(term.kind);
                break;
            }
        }
        let _ = recv.completion.recv().await;
        end_kind.expect("ended")
    });
    assert_eq!(kind, TransactionEndKind::Completed);
    // After coordinator revoke, route table must be empty.
    assert_eq!(gw.routes().len(), 0, "MCP route revoked after terminal");
    let mut owner = started.owner;
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}
