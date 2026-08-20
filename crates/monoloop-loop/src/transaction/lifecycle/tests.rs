//! M2 admission / ownership tests (v2 §22.1 subset).

use super::{StartedRuntime, TransactionRuntimeHandle};
use crate::transaction::bootstrap::{RuntimeBootstrap, RuntimeConfig, StoppedGate};
use crate::transaction::channel_registry::{ChannelBinding, ChannelRegistry};
use crate::transaction::fake_support::TestTextEncoder;
use crate::transaction::host_tools::HostToolRegistry;
use monoloop_connector::FakeConnectorFactory;
use monoloop_contracts::{
    transaction_delivery, user_text_input, AdmissionErrorKind, ChannelCapabilities,
    ChannelDefaults, ChannelId, ChannelKind, ChannelLimits, ContinuationPolicy, DeliveryLimits,
    DialectDescriptor, ExchangeMode, InvocationConfig, McpConfigurationCapability, McpReachability,
    OptionPolicy, SessionId, SessionMode, ShutdownWaitOutcome, ToolExecutionMode,
    TransactionLimits, TransactionSubmitRequest,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

fn llm_binding(id: &str, channel_max: usize) -> ChannelBinding {
    let d = DialectDescriptor::test_raw();
    ChannelBinding {
        id: ChannelId::try_new(id).unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::ModelToolCalls,
        connector_factory: Arc::new(FakeConnectorFactory::direct_llm()),
        encoder: Arc::new(TestTextEncoder),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "default".into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
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
        limits: ChannelLimits {
            max_active_transactions: channel_max,
            ..ChannelLimits::default()
        },
    }
}

fn start_runtime(max_active: usize, channel_max: usize) -> StartedRuntime {
    start_runtime_with_mcp(max_active, channel_max, false)
}

fn start_runtime_with_mcp(max_active: usize, channel_max: usize, mcp: bool) -> StartedRuntime {
    let limits = TransactionLimits {
        max_active_transactions: max_active,
        max_active_per_channel: channel_max.min(max_active),
        // Keep exchange/shutdown tests bounded (default is 600s).
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            enable_mcp_listener: mcp,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", channel_max)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start")
}

fn submit(
    handle: &TransactionRuntimeHandle,
    session: Option<&str>,
) -> Result<
    (
        monoloop_contracts::AdmissionReceipt,
        monoloop_contracts::TransactionReceiver,
    ),
    monoloop_contracts::AdmissionError,
> {
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let receipt = handle.submit(TransactionSubmitRequest {
        channel_id: ChannelId::try_new("llm").unwrap(),
        session_id: session.map(|s| SessionId::try_new(s).unwrap()),
        input: user_text_input("hi").unwrap(),
        session_config: None,
        invocation_config: InvocationConfig::default(),
        tools: vec![],
        delivery,
    })?;
    Ok((receipt, receiver))
}

#[test]
fn submit_from_plain_os_thread_no_tokio_context() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let join = std::thread::spawn(move || submit(&handle, Some("s1")));
    let (receipt, receiver) = join.join().unwrap().expect("admit");
    assert!(receipt.session_id.is_some());

    // Shutdown from a tokio context for wait_stopped.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
    assert!(matches!(outcome, ShutdownWaitOutcome::Stopped(_)));
    let completion = rt.block_on(receiver.completion.recv()).expect("completion");
    assert_eq!(
        completion.end.kind,
        monoloop_contracts::TransactionEndKind::RuntimeShutdown
    );
    let _ = receiver.events;
}

#[test]
fn duplicate_session_rejects_second() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    submit(&handle, Some("same")).expect("first");
    let err = submit(&handle, Some("same")).expect_err("duplicate");
    assert_eq!(err.kind, AdmissionErrorKind::SessionAlreadyActive);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let _ = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
}

#[test]
fn capacity_plus_one_rejects() {
    let started = start_runtime(1, 1);
    let handle = started.handle.clone();
    let (_r1, _recv1) = submit(&handle, Some("a")).expect("first");
    let err = submit(&handle, Some("b")).expect_err("capacity");
    assert_eq!(err.kind, AdmissionErrorKind::CapacityExceeded);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let _ = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
}

#[test]
fn shutdown_publishes_one_completion_per_admission() {
    let started = start_runtime(8, 8);
    let handle = started.handle.clone();
    let mut receivers = Vec::new();
    for i in 0..5 {
        let (_r, recv) = submit(&handle, Some(&format!("s{i}"))).unwrap();
        receivers.push(recv);
    }
    assert_eq!(started.owner.ledger_len(), 5);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            // Coordinators may finish with Completed before shutdown selects
            // RuntimeShutdown; every admission still gets exactly one publish.
            assert_eq!(report.completions_published, 5);
        }
        ShutdownWaitOutcome::TimedOut(snap) => {
            panic!("expected Stopped, got TimedOut: {snap:?}");
        }
    }
    let mut kinds = Vec::new();
    for recv in receivers {
        let c = rt
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(1), recv.completion.recv()).await
            })
            .expect("completion timed out")
            .expect("completion channel closed");
        kinds.push(c.end.kind);
    }
    assert_eq!(kinds.len(), 5);
    assert!(kinds.iter().all(|k| matches!(
        k,
        monoloop_contracts::TransactionEndKind::RuntimeShutdown
            | monoloop_contracts::TransactionEndKind::Completed
    )));
}

#[test]
fn fake_echo_exchange_emits_canonical_text_unit() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let (receipt, receiver) = submit(&handle, Some("echo")).unwrap();
    assert!(receipt.session_id.is_some());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (completion, saw_text) = rt.block_on(async {
        let mut events = receiver.events;
        let completion = tokio::time::timeout(Duration::from_secs(3), receiver.completion.recv())
            .await
            .expect("completion timed out")
            .expect("completion channel closed");
        let mut saw_text = false;
        while let Ok(ev) = events.try_recv() {
            if let monoloop_contracts::TransactionEventPayload::CanonicalUnit(unit) = &ev.payload {
                if let monoloop_contracts::CanonicalUnit::Text(t) = &unit.snapshot().unit {
                    // TestTextEncoder appends ". " to "hi"
                    assert!(t.content.contains("hi"), "unexpected text: {}", t.content);
                    saw_text = true;
                }
            }
        }
        (completion, saw_text)
    });
    assert!(saw_text, "expected Text canonical unit from Fake echo");
    assert_eq!(
        completion.end.kind,
        monoloop_contracts::TransactionEndKind::Completed
    );

    let mut owner = started.owner;
    let _ = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
}

#[test]
fn coordinator_publishes_sequenced_unit_and_completed() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let (receipt, receiver) = submit(&handle, Some("seq")).unwrap();
    assert!(receipt.session_id.is_some());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(async {
        let mut events = receiver.events;
        let completion_rx = receiver.completion;
        // Allow coordinator + publisher to run.
        let completion = tokio::time::timeout(Duration::from_secs(2), completion_rx.recv())
            .await
            .expect("completion timed out")
            .expect("completion channel closed");
        let mut seqs = Vec::new();
        let mut saw_unit = false;
        while let Ok(ev) = events.try_recv() {
            seqs.push(ev.sequence);
            if matches!(
                ev.payload,
                monoloop_contracts::TransactionEventPayload::CanonicalUnit(_)
            ) {
                saw_unit = true;
            }
        }
        assert!(saw_unit, "expected at least one CanonicalUnit event");
        assert!(!seqs.is_empty());
        seqs.sort_unstable();
        for w in seqs.windows(2) {
            assert_eq!(w[1], w[0] + 1, "sequences must be contiguous");
        }
        completion
    });
    assert_eq!(
        completion.end.kind,
        monoloop_contracts::TransactionEndKind::Completed
    );

    let mut owner = started.owner;
    let _ = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
}

#[test]
fn reservation_pool_rejects_zero_capacity() {
    use super::ReservationPool;
    use monoloop_contracts::ChannelId;
    assert!(ReservationPool::try_new(0, vec![]).is_err());
    assert!(ReservationPool::try_new(1, vec![(ChannelId::try_new("c").unwrap(), 0)]).is_err());
}

#[test]
fn shutdown_control_not_starved_when_start_queue_full() {
    // max_active=2: admit two (fills start queue processing), then shutdown via
    // control path must still reach Stopped with both completions.
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let mut receivers = Vec::new();
    for i in 0..2 {
        let (_r, recv) = submit(&handle, Some(&format!("full{i}"))).unwrap();
        receivers.push(recv);
    }
    // Third admit must fail on capacity (reservations), not hang.
    let err = submit(&handle, Some("overflow")).expect_err("capacity");
    assert_eq!(err.kind, AdmissionErrorKind::CapacityExceeded);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == 2),
        "expected Stopped with 2 completions, got {outcome:?}"
    );
    for recv in receivers {
        let c = rt.block_on(recv.completion.recv()).unwrap();
        assert_eq!(
            c.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
        );
    }
}

#[test]
fn short_wait_may_timeout_while_quiescing_then_complete() {
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let (_r, _recv) = submit(&handle, Some("q")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
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

/// M6 §22.5: short wait may TimedOut while Quiescing; later wait reaches Stopped
/// with empty ledger (same shutdown ticket generation on TimedOut snapshot).
#[test]
fn m6_short_timeout_then_repeat_wait_same_generation_stopped() {
    use crate::transaction::state::RuntimeState;

    // Gate Stopped so ZERO-deadline TimedOut cannot race a fast FakeConnector drain.
    let gate = Arc::new(StoppedGate::new());
    let limits = TransactionLimits {
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            block_stopped: Some(Arc::clone(&gate)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r1, _recv1) = submit(&handle, Some("m6a")).unwrap();
    let (_r2, _recv2) = submit(&handle, Some("m6b")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let ticket = owner.begin_shutdown();
    // Idempotent: second begin_shutdown must not bump generation (§22.5).
    let ticket2 = owner.begin_shutdown();
    assert_eq!(ticket.generation(), ticket2.generation());
    assert_eq!(ticket.generation(), 1);

    // Zero deadline under block_stopped: must observe TimedOut while Quiescing.
    let first = rt.block_on(owner.wait_stopped(Duration::ZERO));
    let ShutdownWaitOutcome::TimedOut(snap) = first else {
        panic!("§22.5 requires short wait → TimedOut while work drains, got {first:?}");
    };
    assert_eq!(snap.generation, ticket.generation());
    assert_eq!(owner.state(), RuntimeState::Quiescing);
    // §18.2: TimedOut snapshot reports remaining work while gate holds Stopped.
    assert!(
        snap.ledger_entries > 0 || snap.owned_tasks > 0,
        "TimedOut under block_stopped must report residual ledger/tasks, got {snap:?}"
    );

    gate.release();
    let second = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    match second {
        ShutdownWaitOutcome::Stopped(_) => {
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(owner.global_reservations(), 0);
            assert_eq!(owner.state(), RuntimeState::Stopped);
        }
        other => panic!("expected Stopped after long wait, got {other:?}"),
    }
}

/// §22.5: concurrent begin_shutdown callers share one generation (CAS 0→1).
#[test]
fn m6_concurrent_begin_shutdown_same_generation() {
    let started = start_runtime(2, 2);
    let owner = Arc::new(started.owner);
    let mut handles = Vec::new();
    for _ in 0..8 {
        let o = Arc::clone(&owner);
        handles.push(std::thread::spawn(move || o.begin_shutdown().generation()));
    }
    let mut gens: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    gens.sort_unstable();
    assert!(gens.iter().all(|g| *g == 1), "all tickets must share gen 1, got {gens:?}");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Arc::into_inner fails if clones remain; we moved all thread clones out.
    let mut owner = Arc::try_unwrap(owner).unwrap_or_else(|_| panic!("owner still shared"));
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(outcome, ShutdownWaitOutcome::Stopped(_)));
}

/// M6 / D-004: Seal with authoritative session id replaces synthetic key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_publisher_prefers_authoritative_session_on_seal() {
    use super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TerminalEventDelivery,
        TransactionDiagnostic, TransactionEndEvent, TransactionEndKind, TransactionEventPayload,
        TransactionId, TransactionUsage,
    };
    use tokio::sync::{mpsc, oneshot};

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(16, 64 * 1024).unwrap()).unwrap();
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
    ));

    // First ordinary event invents tx-{id}.
    cmd_tx
        .send(EventPublisherCommand::Publish(Box::new(
            TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("noop", Some("x"), 64).unwrap(),
            }),
        )))
        .await
        .unwrap();
    let first = receiver.events.recv().await.expect("first event");
    let synthetic = first.session_id.clone();
    assert!(
        synthetic.as_str().starts_with("tx-") || synthetic.as_str() == "direct",
        "expected synthetic, got {}",
        synthetic.as_str()
    );

    let authoritative = SessionId::try_new("grok-session-auth").unwrap();
    let (reply_tx, reply_rx) = oneshot::channel();
    cmd_tx
        .send(EventPublisherCommand::Seal {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: Some(authoritative.clone()),
                channel_id: channel.clone(),
                kind: TransactionEndKind::Completed,
                emitted_events: 0,
                usage: TransactionUsage::default(),
                diagnostics: vec![],
            },
            reply: reply_tx,
        })
        .await
        .unwrap();
    let pub_result = reply_rx.await.unwrap();
    assert_eq!(pub_result.delivery, TerminalEventDelivery::Published);
    let ended = receiver.events.recv().await.expect("ended");
    // Envelope SessionId must match authoritative.
    assert_eq!(ended.session_id, authoritative);
    assert_ne!(ended.session_id, synthetic);
    // EndedEvent payload SessionId must match the envelope (Seal sync).
    match &ended.payload {
        TransactionEventPayload::EndedEvent(term) => {
            assert_eq!(
                term.session_id.as_ref(),
                Some(&authoritative),
                "payload session_id must match envelope"
            );
        }
        other => panic!("expected EndedEvent, got {other:?}"),
    }
    let _ = pub_task.await;
}

/// D-043: MCP loopback listener is TaskSupervisor-owned and joins before Stopped.
#[test]
fn mcp_listener_owned_shutdown_reaches_stopped() {
    let started = start_runtime_with_mcp(2, 2, true);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        // Give the RuntimeService listener a moment to bind.
        tokio::time::sleep(Duration::from_millis(50)).await;
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped with MCP joined, got {outcome:?}"
    );
}

#[test]
fn needs_loop_dispatch_ready_only() {
    use super::loop_dispatch::needs_loop_dispatch;
    use monoloop_contracts::{
        CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, FlowId,
        InterpretationId, LaneId, ToolActionEvent, ToolActionId, ToolExecutionState,
        ToolRequestState, ToolResultState, UnitId, UnitState,
    };

    let ready = CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
        unit_id: UnitId::new("tool-1"),
        unit_generation: 1,
        unit_state: UnitState::Waiting,
        interpretation_id: InterpretationId::generate(),
        connection_id: ConnectionId::generate(),
        external_session_id: None,
        flow_id: FlowId::main(),
        lane_id: LaneId::tool(),
        lane_ordinal: 1,
        causal_parent_id: None,
        source_time: None,
        source_step: None,
        unit: CanonicalUnit::Tool(ToolActionEvent {
            tool_action_id: ToolActionId::new("a1"),
            tool_name: Some("bash".into()),
            request_state: ToolRequestState::Ready,
            execution_state: ToolExecutionState::Waiting,
            result_state: ToolResultState::Absent,
            request_payload: Some("{}".into()),
            result_payload: None,
            terminal_outcome: None,
            waiting_for: None,
        }),
    });
    let waiting = CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
        unit_id: UnitId::new("tool-2"),
        unit_generation: 1,
        unit_state: UnitState::Waiting,
        interpretation_id: InterpretationId::generate(),
        connection_id: ConnectionId::generate(),
        external_session_id: None,
        flow_id: FlowId::main(),
        lane_id: LaneId::tool(),
        lane_ordinal: 2,
        causal_parent_id: None,
        source_time: None,
        source_step: None,
        unit: CanonicalUnit::Tool(ToolActionEvent {
            tool_action_id: ToolActionId::new("a2"),
            tool_name: Some("bash".into()),
            request_state: ToolRequestState::Assembling,
            execution_state: ToolExecutionState::Waiting,
            result_state: ToolResultState::Absent,
            request_payload: None,
            result_payload: None,
            terminal_outcome: None,
            waiting_for: Some("args".into()),
        }),
    });
    assert!(needs_loop_dispatch(std::slice::from_ref(&ready)));
    assert!(!needs_loop_dispatch(std::slice::from_ref(&waiting)));
    assert!(needs_loop_dispatch(&[waiting, ready]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_loop_no_ambient_spawn_unavailable() {
    use crate::registry::EmptyToolRegistry;
    use crate::runtime::{DefaultLoopRuntime, StartLoop};
    use crate::subscription::SubscriptionPublisher;
    use crate::tools::NoToolRuntime;
    use monoloop_contracts::{
        CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, FlowId,
        InterpretationId, InterpreterOutputEvent, LaneId, LoopEndKind, LoopId, LoopLimits,
        LoopOutputEvent, LoopScope, MonoloopRunId, OutboundToolOutcome, ToolActionEvent,
        ToolActionId, ToolExecutionState, ToolRequestState, ToolResultState, UnitId, UnitState,
    };

    let (pub_, sub) = SubscriptionPublisher::channel("prep", 16);
    let run = MonoloopRunId::new("r-prep");
    let loop_id = LoopId::new("l-prep");
    let scope = LoopScope {
        monoloop_run_id: run.clone(),
        loop_id: loop_id.clone(),
        accepted_interpretation_ids: vec![],
        accepted_connection_ids: vec![],
        accepted_external_session_ids: vec![],
        accept_all_in_run: true,
    };
    let limits = LoopLimits::default();
    let (handle, fut) = DefaultLoopRuntime::new()
        .prepare(StartLoop {
            monoloop_run_id: run,
            loop_id,
            scope,
            subscription: sub,
            tool_registry: Arc::new(EmptyToolRegistry::new()),
            tool_runtime: Arc::new(NoToolRuntime::new()),
            output_capacity: 16,
            limits,
        })
        .expect("prepare");

    let ready = InterpreterOutputEvent::Unit(Box::new(CanonicalUnitEvent::Created(
        CanonicalUnitSnapshot {
            unit_id: UnitId::new("tool-1"),
            unit_generation: 1,
            unit_state: UnitState::Waiting,
            interpretation_id: InterpretationId::new("i1"),
            connection_id: ConnectionId::new("c1"),
            external_session_id: None,
            flow_id: FlowId::main(),
            lane_id: LaneId::tool(),
            lane_ordinal: 1,
            causal_parent_id: None,
            source_time: None,
            source_step: None,
            unit: CanonicalUnit::Tool(ToolActionEvent {
                tool_action_id: ToolActionId::new("a1"),
                tool_name: Some("bash".into()),
                request_state: ToolRequestState::Ready,
                execution_state: ToolExecutionState::Waiting,
                result_state: ToolResultState::Absent,
                request_payload: Some(r#"{"cmd":"ls"}"#.into()),
                result_payload: None,
                terminal_outcome: None,
                waiting_for: Some("external".into()),
            }),
        },
    )));

    let feed = async {
        pub_.publish(ready).await.unwrap();
        drop(pub_);
    };
    tokio::join!(fut, feed);

    let mut unavailable = 0;
    let mut outbound = 0;
    {
        let mut rx = handle.take_output().await;
        while let Some(ev) = rx.recv().await {
            match &ev {
                LoopOutputEvent::ToolUnavailable { .. } => unavailable += 1,
                LoopOutputEvent::OutboundToolResult(r) => {
                    assert_eq!(r.outcome, OutboundToolOutcome::ToolUnavailable);
                    assert!(r.tool_execution_id.is_none());
                    outbound += 1;
                }
                LoopOutputEvent::LoopEnded(_) => break,
                _ => {}
            }
        }
    }
    assert_eq!(unavailable, 1);
    assert_eq!(outbound, 1);
    let end = handle.completion.wait().await;
    assert_eq!(end.kind, LoopEndKind::Drained);
    assert_eq!(end.tools_unavailable, 1);
}

fn ready_tool_unit() -> monoloop_contracts::CanonicalUnitEvent {
    use monoloop_contracts::{
        CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, FlowId,
        InterpretationId, LaneId, ToolActionEvent, ToolActionId, ToolExecutionState,
        ToolRequestState, ToolResultState, UnitId, UnitState,
    };
    CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
        unit_id: UnitId::new("tool-1"),
        unit_generation: 1,
        unit_state: UnitState::Waiting,
        interpretation_id: InterpretationId::new("i1"),
        connection_id: ConnectionId::new("c1"),
        external_session_id: None,
        flow_id: FlowId::main(),
        lane_id: LaneId::tool(),
        lane_ordinal: 1,
        causal_parent_id: None,
        source_time: None,
        source_step: None,
        unit: CanonicalUnit::Tool(ToolActionEvent {
            tool_action_id: ToolActionId::new("a1"),
            tool_name: Some("bash".into()),
            request_state: ToolRequestState::Ready,
            execution_state: ToolExecutionState::Waiting,
            result_state: ToolResultState::Absent,
            request_payload: Some(r#"{"cmd":"ls"}"#.into()),
            result_payload: None,
            terminal_outcome: None,
            waiting_for: None,
        }),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_empty_loop_ready_under_task_spawner() {
    use super::event_publisher::EventPublisherCommand;
    use super::loop_dispatch::run_supervised_empty_loop;
    use super::task_spawner::TransactionTaskSpawner;
    use super::task_supervisor::TaskSupervisor;
    use crate::transaction::sticky_cancel::StickyCancel;
    use monoloop_contracts::{ChannelId, ExchangeId, TransactionEventPayload, TransactionId};
    use tokio::sync::mpsc;

    let (spawner, mut spawn_rx) = TransactionTaskSpawner::channel(8);
    let pump = tokio::spawn(async move {
        let mut tasks = TaskSupervisor::new();
        while let Some(req) = spawn_rx.recv().await {
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        let _ = tasks.abort_and_drain().await;
    });

    let (publish_tx, mut publish_rx) = mpsc::channel::<EventPublisherCommand>(16);
    let collector = tokio::spawn(async move {
        let mut tool_lifecycle = 0u32;
        while let Some(cmd) = publish_rx.recv().await {
            if let EventPublisherCommand::Publish(payload) = cmd {
                if matches!(payload.as_ref(), TransactionEventPayload::ToolLifecycle(_)) {
                    tool_lifecycle = tool_lifecycle.saturating_add(1);
                }
            }
        }
        tool_lifecycle
    });

    let cancel = Arc::new(StickyCancel::new());
    let report = run_supervised_empty_loop(
        &spawner,
        TransactionId::generate(),
        ChannelId::try_new("llm").unwrap(),
        None,
        ExchangeId::generate(),
        vec![ready_tool_unit()],
        publish_tx,
        cancel,
    )
    .await
    .expect("supervised empty loop");

    assert_eq!(report.tools_unavailable, 1);
    assert_eq!(report.outbound_results, 1);

    drop(spawner);
    let lifecycle_count = collector.await.expect("collector");
    assert_eq!(lifecycle_count, 1);
    pump.await.expect("spawn pump");
}

/// LAW 25: cancel before any Loop waiter must not report success/Drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_empty_loop_cancel_before_waiter_is_cancelled() {
    use super::event_publisher::EventPublisherCommand;
    use super::loop_dispatch::{run_supervised_empty_loop, LoopDispatchError};
    use super::task_spawner::TransactionTaskSpawner;
    use super::task_supervisor::TaskSupervisor;
    use crate::transaction::sticky_cancel::StickyCancel;
    use monoloop_contracts::{ChannelId, ExchangeId, TransactionId};
    use tokio::sync::mpsc;

    let (spawner, mut spawn_rx) = TransactionTaskSpawner::channel(8);
    let pump = tokio::spawn(async move {
        let mut tasks = TaskSupervisor::new();
        while let Some(req) = spawn_rx.recv().await {
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        let _ = tasks.abort_and_drain().await;
    });

    let (publish_tx, mut publish_rx) = mpsc::channel::<EventPublisherCommand>(8);
    let drain_pub = tokio::spawn(async move { while publish_rx.recv().await.is_some() {} });

    let cancel = Arc::new(StickyCancel::new());
    // Fire before run_supervised_empty_loop installs waiters.
    cancel.cancel();

    let err = run_supervised_empty_loop(
        &spawner,
        TransactionId::generate(),
        ChannelId::try_new("llm").unwrap(),
        None,
        ExchangeId::generate(),
        vec![ready_tool_unit()],
        publish_tx,
        Arc::clone(&cancel),
    )
    .await
    .expect_err("pre-cancel must not succeed");

    assert_eq!(err, LoopDispatchError::Cancelled);

    drop(spawner);
    let _ = drain_pub.await;
    pump.await.expect("spawn pump");
}
