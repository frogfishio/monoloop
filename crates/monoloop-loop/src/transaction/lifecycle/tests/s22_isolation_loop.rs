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

/// §22.6: SessionEstablished is sequence 1 for new external sessions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_6_session_established_is_sequence_one() {
    use super::super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, ExternalSessionId, SafeDiagnostic,
        TransactionDiagnostic, TransactionEventPayload, TransactionId,
    };
    use tokio::sync::mpsc;

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(16, 64 * 1024).unwrap()).unwrap();
    let (admit, cmd_rx) = super::super::event_publisher::OrdinaryCmdAdmit::channel(8);
    let (_seal_tx, seal_rx) = mpsc::channel(1);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        admit.clone(),
        seal_rx,
        Arc::new(crate::transaction::sticky_cancel::StickyCancel::new()),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    let external = ExternalSessionId::try_new("grok-ext-1").unwrap();
    admit
        .send(EventPublisherCommand::EstablishExternal(external.clone()))
        .await
        .unwrap();
    let first = receiver.events.recv().await.expect("session established");
    assert_eq!(first.sequence, 1);
    match &first.payload {
        TransactionEventPayload::SessionEstablished {
            external_session_id,
        } => {
            assert_eq!(external_session_id.as_str(), external.as_str());
        }
        other => panic!("expected SessionEstablished, got {other:?}"),
    }

    admit
        .send(EventPublisherCommand::Publish(Box::new(
            TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("noop", Some("x"), 64).unwrap(),
            }),
        )))
        .await
        .unwrap();
    let second = receiver
        .events
        .recv()
        .await
        .expect("ordinary after establish");
    assert_eq!(second.sequence, 2);
    assert!(matches!(
        second.payload,
        TransactionEventPayload::Diagnostic(_)
    ));
    drop(admit);
    drop(_seal_tx);
    let _ = pub_task.await;
}

/// §22.6: concurrent producers through one publisher stay contiguous 1..N.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s22_6_concurrent_producers_contiguous_sequence() {
    use super::super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TransactionDiagnostic,
        TransactionEventPayload, TransactionId,
    };
    use tokio::sync::mpsc;

    let n = 32usize;
    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(n + 8, 1024 * 1024).unwrap()).unwrap();
    let (admit, cmd_rx) = super::super::event_publisher::OrdinaryCmdAdmit::channel(n);
    let (_seal_tx, seal_rx) = mpsc::channel(1);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel,
        None,
        delivery.event_tx,
        cmd_rx,
        admit.clone(),
        seal_rx,
        Arc::new(crate::transaction::sticky_cancel::StickyCancel::new()),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    let mut joins = Vec::new();
    for i in 0..n {
        let tx = admit.clone();
        joins.push(tokio::spawn(async move {
            tx.send(EventPublisherCommand::Publish(Box::new(
                TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                    diagnostic: SafeDiagnostic::try_new("noop", Some(&format!("p{i}")), 64)
                        .unwrap(),
                }),
            )))
            .await
            .unwrap();
        }));
    }
    for j in joins {
        j.await.unwrap();
    }
    drop(admit);
    drop(_seal_tx); // close seal channel so publisher can exit without Seal

    let mut seqs = Vec::new();
    while let Some(ev) = receiver.events.recv().await {
        seqs.push(ev.sequence);
    }
    let _ = pub_task.await;
    assert_eq!(seqs.len(), n, "all publishes delivered, got {seqs:?}");
    let expected: Vec<u64> = (1..=n as u64).collect();
    let mut got = seqs.clone();
    got.sort_unstable();
    assert_eq!(got, expected, "contiguous 1..N allocated");
    // Delivery order matches allocation order (single publisher serializes).
    assert_eq!(seqs, expected, "delivery order must match sequence order");
}

/// §22.6: same session string on different Channels remains isolated.
#[test]
fn s22_6_same_session_string_different_channels_isolated() {
    let limits = TransactionLimits {
        max_active_transactions: 4,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm-a", 2), llm_binding("llm-b", 2)])
            .unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let (r_a, recv_a) = submit_on(&handle, "llm-a", Some("shared-sid")).expect("a");
    let (r_b, recv_b) = submit_on(&handle, "llm-b", Some("shared-sid")).expect("b");
    assert_ne!(r_a.transaction_id, r_b.transaction_id);
    // Same session string is isolated by ChannelId inside SessionKey.
    assert_eq!(
        r_a.session_id.as_ref().map(|s| s.as_str()),
        Some("shared-sid")
    );
    assert_eq!(
        r_b.session_id.as_ref().map(|s| s.as_str()),
        Some("shared-sid")
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == 2),
        "both channel-isolated admissions complete, got {outcome:?}"
    );
    let _ = rt.block_on(recv_a.completion.recv());
    let _ = rt.block_on(recv_b.completion.recv());
}

/// §22.6: reused provider tool-call ids across exchanges stay distinct via helper.
#[test]
fn s22_6_reused_provider_tool_call_ids_across_exchanges_distinct() {
    use super::super::session_identity::tool_action_id_for_exchange;

    // Provider may reuse the same tool_call id string across exchanges; Monoloop
    // correlates with exchange-scoped ToolActionId (production helper).
    let provider_reuse = "call_abc";
    let exchange_a = monoloop_contracts::ExchangeId::generate();
    let exchange_b = monoloop_contracts::ExchangeId::generate();
    let action_a = tool_action_id_for_exchange(exchange_a, provider_reuse);
    let action_b = tool_action_id_for_exchange(exchange_b, provider_reuse);
    assert_ne!(
        action_a.as_str(),
        action_b.as_str(),
        "same provider id on different exchanges must remain distinct"
    );
    assert!(action_a.as_str().contains(provider_reuse));
    assert!(action_b.as_str().contains(provider_reuse));
}

/// §22.6: failed EstablishExternal must not mutate identity or lose seq 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_6_establish_external_capacity_fail_does_not_steal_seq1() {
    use super::super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, ExternalSessionId, SafeDiagnostic,
        TransactionDiagnostic, TransactionEvent, TransactionEventPayload, TransactionId,
    };
    use tokio::sync::mpsc;

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(1, 64 * 1024).unwrap()).unwrap();
    // Pre-fill mailbox so EstablishExternal's try_send fails while next_seq is still 1.
    let filler = delivery.event_tx.clone();
    filler
        .try_send(TransactionEvent {
            transaction_id: tx_id,
            channel_id: channel.clone(),
            session_id: SessionId::try_new("prefill").unwrap(),
            sequence: 0,
            payload: TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("noop", Some("prefill"), 64).unwrap(),
            }),
        })
        .unwrap();

    let (admit, cmd_rx) = super::super::event_publisher::OrdinaryCmdAdmit::channel(8);
    let (_seal_tx, seal_rx) = mpsc::channel(1);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel,
        None,
        delivery.event_tx,
        cmd_rx,
        admit.clone(),
        seal_rx,
        Arc::new(crate::transaction::sticky_cancel::StickyCancel::new()),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    let external = ExternalSessionId::try_new("grok-retry").unwrap();
    admit
        .send(EventPublisherCommand::EstablishExternal(external.clone()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Drain prefill; Establish must still be able to claim seq 1 on retry.
    let pre = receiver.events.recv().await.expect("prefill");
    assert_eq!(pre.sequence, 0);

    admit
        .send(EventPublisherCommand::EstablishExternal(external.clone()))
        .await
        .unwrap();
    let first = receiver.events.recv().await.expect("session established");
    assert_eq!(first.sequence, 1);
    assert!(matches!(
        first.payload,
        TransactionEventPayload::SessionEstablished { .. }
    ));

    admit
        .send(EventPublisherCommand::Publish(Box::new(
            TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("noop", Some("after"), 64).unwrap(),
            }),
        )))
        .await
        .unwrap();
    let second = receiver.events.recv().await.expect("ordinary");
    assert_eq!(second.sequence, 2);
    drop(admit);
    drop(_seal_tx);
    let _ = pub_task.await;
}

/// §22.6: event item plus-one fails closed.
#[test]
fn s22_6_event_item_plus_one_fails_closed() {
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TransactionDiagnostic,
        TransactionEvent, TransactionEventPayload, TransactionId,
    };

    let (delivery, _recv) =
        transaction_delivery(DeliveryLimits::try_new(1, 1024 * 1024).unwrap()).unwrap();
    let tx_id = TransactionId::generate();
    let ev = || TransactionEvent {
        transaction_id: tx_id,
        channel_id: ChannelId::try_new("ch").unwrap(),
        session_id: SessionId::try_new("s").unwrap(),
        sequence: 1,
        payload: TransactionEventPayload::Diagnostic(TransactionDiagnostic {
            diagnostic: SafeDiagnostic::try_new("noop", Some("x"), 64).unwrap(),
        }),
    };
    delivery.event_tx.try_send(ev()).unwrap();
    let err = delivery.event_tx.try_send(ev()).unwrap_err();
    assert_eq!(
        err,
        monoloop_contracts::EventEnqueueError::ItemCapacityExceeded
    );
}

/// §22.6: event byte plus-one fails closed.
#[test]
fn s22_6_event_byte_plus_one_fails_closed() {
    use monoloop_contracts::{
        estimate_event_bytes, transaction_delivery, DeliveryLimits, SafeDiagnostic,
        TransactionDiagnostic, TransactionEvent, TransactionEventPayload, TransactionId,
    };

    let tx_id = TransactionId::generate();
    let sample = TransactionEvent {
        transaction_id: tx_id,
        channel_id: ChannelId::try_new("ch").unwrap(),
        session_id: SessionId::try_new("s").unwrap(),
        sequence: 1,
        payload: TransactionEventPayload::Diagnostic(TransactionDiagnostic {
            diagnostic: SafeDiagnostic::try_new("noop", Some("payload-bytes"), 256).unwrap(),
        }),
    };
    let nbytes = estimate_event_bytes(&sample);
    // Budget exactly one event's bytes — second enqueue must fail closed.
    let (delivery, _recv) =
        transaction_delivery(DeliveryLimits::try_new(8, nbytes).unwrap()).unwrap();
    delivery.event_tx.try_send(sample.clone()).unwrap();
    let err = delivery.event_tx.try_send(sample).unwrap_err();
    assert_eq!(
        err,
        monoloop_contracts::EventEnqueueError::ByteCapacityExceeded
    );
}

#[test]
fn needs_loop_dispatch_ready_only() {
    use super::super::loop_dispatch::needs_loop_dispatch;
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
    use super::super::event_publisher::{EventPublisherCommand, OrdinaryCmdAdmit};
    use super::super::loop_dispatch::run_supervised_empty_loop;
    use super::super::task_spawner::TransactionTaskSpawner;
    use super::super::task_supervisor::TaskSupervisor;
    use crate::transaction::sticky_cancel::StickyCancel;
    use monoloop_contracts::{ChannelId, ExchangeId, TransactionEventPayload, TransactionId};

    let (spawner, mut spawn_rx) = TransactionTaskSpawner::channel(8);
    let pump = tokio::spawn(async move {
        let mut tasks = TaskSupervisor::new();
        while let Some(req) = spawn_rx.recv().await {
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        let _ = tasks.abort_and_drain().await;
    });

    let (publish_tx, mut publish_rx) = OrdinaryCmdAdmit::channel(16);
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

/// Non-empty HostToolRegistry: Ready tool completes via supervised HostToolRuntime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_non_empty_loop_dispatches_registered_tool() {
    use super::super::event_publisher::{EventPublisherCommand, OrdinaryCmdAdmit};
    use super::super::loop_dispatch::run_supervised_tool_loop;
    use super::super::task_spawner::TransactionTaskSpawner;
    use super::super::task_supervisor::TaskSupervisor;
    use crate::transaction::dispatcher::TransactionToolDispatcher;
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::loop_adapters::{HostToolRuntime, ResolvedToolRegistry};
    use crate::transaction::resolved_tools::ResolvedToolSet;
    use crate::transaction::sticky_cancel::StickyCancel;
    use crate::transaction::tool_capacity::SharedToolCapacity;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        ChannelId, ExchangeId, JsonSchema, SessionKey, ToolCompletion, ToolExecutionClass, ToolId,
        ToolLimits, ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract,
        TransactionEventPayload, TransactionId,
    };

    let schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "cmd": { "type": "string" } },
        "required": ["cmd"],
        "additionalProperties": false
    }))
    .unwrap();
    let out_schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "ok": { "type": "boolean" } },
        "required": ["ok"],
        "additionalProperties": false
    }))
    .unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("bash").unwrap(),
        ToolName::try_new("bash").unwrap(),
        "bash tool",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out_schema),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 2,
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            execution_deadline: Duration::from_secs(2),
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
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({"ok": true})),
            ))
        })),
    );
    let resolved = ResolvedToolSet::from_registered(vec![registered]);
    let tx_id = TransactionId::generate();
    let exchange_id = ExchangeId::generate();
    let dispatcher = TransactionToolDispatcher::new(
        tx_id,
        SessionKey::new(
            ChannelId::try_new("llm").unwrap(),
            SessionId::try_new("s1").unwrap(),
        ),
        resolved.clone(),
        SharedToolCapacity::unlimited(),
        8,
        16,
    );

    let (spawner, mut spawn_rx) = TransactionTaskSpawner::channel(16);
    let pump = tokio::spawn(async move {
        let mut tasks = TaskSupervisor::new();
        while let Some(req) = spawn_rx.recv().await {
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        let _ = tasks.abort_and_drain().await;
    });

    let (publish_tx, mut publish_rx) = OrdinaryCmdAdmit::channel(16);
    let collector = tokio::spawn(async move {
        let mut completed_ok = 0u32;
        while let Some(cmd) = publish_rx.recv().await {
            if let EventPublisherCommand::Publish(payload) = cmd {
                if let TransactionEventPayload::ToolLifecycle(
                    monoloop_contracts::ToolLifecycleEvent::Completed { result },
                ) = payload.as_ref()
                {
                    if matches!(
                        result.outcome,
                        monoloop_contracts::CanonicalToolResultOutcome::Succeeded(_)
                    ) {
                        completed_ok = completed_ok.saturating_add(1);
                    }
                }
            }
        }
        completed_ok
    });

    let runtime =
        HostToolRuntime::with_spawner(Arc::clone(&dispatcher), exchange_id, tx_id, spawner.clone());
    let cancel = Arc::new(StickyCancel::new());
    let report = run_supervised_tool_loop(
        &spawner,
        tx_id,
        ChannelId::try_new("llm").unwrap(),
        Some(SessionId::try_new("s1").unwrap()),
        exchange_id,
        vec![ready_tool_unit()],
        publish_tx,
        cancel,
        Arc::new(ResolvedToolRegistry::new(resolved)),
        Arc::new(runtime),
    )
    .await
    .expect("supervised non-empty loop");

    assert_eq!(report.tools_unavailable, 0, "registered tool must resolve");
    assert!(report.outbound_results >= 1);
    assert_eq!(report.tools_completed, 1);

    drop(spawner);
    let ok_count = collector.await.expect("collector");
    assert_eq!(ok_count, 1);
    pump.await.expect("spawn pump");
}

/// LAW 25: cancel before any Loop waiter must not report success/Drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_empty_loop_cancel_before_waiter_is_cancelled() {
    use super::super::event_publisher::OrdinaryCmdAdmit;
    use super::super::loop_dispatch::{run_supervised_empty_loop, LoopDispatchError};
    use super::super::task_spawner::TransactionTaskSpawner;
    use super::super::task_supervisor::TaskSupervisor;
    use crate::transaction::sticky_cancel::StickyCancel;
    use monoloop_contracts::{ChannelId, ExchangeId, TransactionId};

    let (spawner, mut spawn_rx) = TransactionTaskSpawner::channel(8);
    let pump = tokio::spawn(async move {
        let mut tasks = TaskSupervisor::new();
        while let Some(req) = spawn_rx.recv().await {
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        let _ = tasks.abort_and_drain().await;
    });

    let (publish_tx, mut publish_rx) = OrdinaryCmdAdmit::channel(8);
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
