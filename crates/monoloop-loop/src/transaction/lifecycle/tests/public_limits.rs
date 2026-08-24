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

/// D-039: unknown transaction id is NotFound — never AlreadyTerminal from a missed send.
#[test]
fn terminate_unknown_transaction_is_not_found() {
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let disp = handle.terminate(
        TransactionSelector::Transaction(TransactionId::generate()),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    assert_eq!(disp, TerminationDisposition::NotFound);

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

/// §23: `TransactionLimits.terminal_event_delivery_deadline` sizes Seal budget
/// on `StartedRuntime` — full host mailbox + short Seal budget →
/// `TerminalEventDelivery::DeadlineExceeded` (D-047 path through the field).
#[test]
fn transaction_limits_terminal_event_delivery_deadline_seal_fails_closed() {
    use monoloop_contracts::TerminalEventDelivery;

    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        // Long tx deadline so Fake echo can finish; Seal budget is the cell.
        transaction_deadline: Duration::from_secs(5),
        cleanup_deadline: Duration::from_millis(500),
        terminal_event_delivery_deadline: Duration::from_millis(1),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    // Capacity 1: first ordinary event occupies the host mailbox so Seal waits.
    let (delivery, mut recv) =
        transaction_delivery(DeliveryLimits::try_new(1, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("seal-deadline").unwrap()),
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect("admit");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt
        .block_on(async {
            // Do not drain events — keep the host mailbox full for Seal.
            tokio::time::timeout(Duration::from_secs(3), recv.completion.recv()).await
        })
        .expect("completion within 3s")
        .expect("completion channel closed");
    assert_eq!(
        completion.terminal_event_delivery,
        TerminalEventDelivery::DeadlineExceeded,
        "expected Seal DeadlineExceeded from TransactionLimits.terminal_event_delivery_deadline=1ms with full host mailbox, got {:?}",
        completion.terminal_event_delivery
    );
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::EventDeliveryFailed,
        "sticky Seal DeadlineExceeded must remap Completed → EventDeliveryFailed, got {:?}",
        completion.end.kind
    );
    // Drain after assertion so Drop does not strand the publisher.
    while recv.events.try_recv().is_ok() {}
    shutdown_owner(started);
}

/// §23: `TransactionLimits.transaction_deadline` — Hang exchange fails closed
/// with `DeadlineExceeded` when the configured deadline elapses.
#[test]
fn transaction_limits_transaction_deadline_hang_ends_deadline_exceeded() {
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        // Short but above open/spawn noise; Hang never completes until terminate.
        transaction_deadline: Duration::from_millis(80),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_receipt, receiver) = submit(&handle, Some("tx-deadline")).expect("admit Hang");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(2), receiver.completion.recv()).await
        })
        .expect("Hang must terminalize under transaction_deadline within 2s")
        .expect("completion channel closed");
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::DeadlineExceeded,
        "expected DeadlineExceeded from TransactionLimits.transaction_deadline on Hang, got {:?}",
        completion.end.kind
    );

    shutdown_owner(started);
}

/// InvocationConfig.deadline shortens the absolute transaction deadline (may not
/// exceed TransactionLimits.transaction_deadline).
#[test]
fn invocation_deadline_override_hang_ends_deadline_exceeded() {
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        // Long runtime ceiling — invocation override must win by shortening.
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("inv-deadline").unwrap()),
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_millis(80)),
                ..InvocationConfig::default()
            },
            tools: vec![],
            delivery,
        })
        .expect("admit Hang with short invocation deadline");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(2), receiver.completion.recv()).await
        })
        .expect("Hang must terminalize under invocation deadline within 2s")
        .expect("completion channel closed");
    assert_eq!(
        completion.end.kind,
        TransactionEndKind::DeadlineExceeded,
        "expected DeadlineExceeded from InvocationConfig.deadline on Hang, got {:?}",
        completion.end.kind
    );
    shutdown_owner(started);
}

/// §23: `TransactionLimits.max_tool_schema_bytes` enforced at `StartedRuntime::start`
/// — exact schema size admits; one byte under rejects (D-056).
#[test]
fn transaction_limits_max_tool_schema_bytes_exact_admits_plus_one_rejects() {
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use crate::StartupError;
    use monoloop_contracts::{
        JsonSchema, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits, ToolName,
        ToolOutputContract, ToolSpec, ToolSuccessContract,
    };

    let schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "q": { "type": "string" } },
        "required": ["q"],
        "additionalProperties": false
    }))
    .unwrap();
    let schema_bytes = serde_json::to_vec(schema.as_value()).unwrap().len();
    assert!(
        schema_bytes >= 2,
        "fixture schema must be at least 2 bytes for exact/plus-one"
    );
    let out = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "ok": { "type": "boolean" } },
        "required": ["ok"],
        "additionalProperties": false
    }))
    .unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("schema_probe").unwrap(),
        ToolName::try_new("schema_probe").unwrap(),
        "schema byte ceiling probe",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits::default(),
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let tools = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_, _| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({"ok": true})),
            ))
        })),
    )])
    .expect("registry build under default schema ceiling");

    let exact_limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        max_tool_schema_bytes: schema_bytes,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: exact_limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: tools.clone(),
    })
    .expect("exact-admit: schema_bytes under max_tool_schema_bytes must start");
    shutdown_owner(started);

    let plus_limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        max_tool_schema_bytes: schema_bytes - 1,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    match StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: plus_limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools,
    }) {
        Ok(_) => panic!("plus-one: schema_bytes-1 must reject at start"),
        Err(err) => assert_eq!(
            err,
            StartupError::InvalidConfig("tool schema exceeds max_tool_schema_bytes")
        ),
    }
}

/// §23: `TransactionLimits.max_event_queue` is the runtime ceiling over caller
/// `DeliveryLimits.max_event_items` — exact admits, plus-one rejects at admit.
#[test]
fn transaction_limits_max_event_queue_exact_admits_plus_one_rejects() {
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        max_event_queue: 1,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let (delivery_ok, recv_ok) =
        transaction_delivery(DeliveryLimits::try_new(1, 64 * 1024).unwrap()).unwrap();
    let ok = handle.submit(TransactionSubmitRequest {
        channel_id: ChannelId::try_new("llm").unwrap(),
        session_id: Some(SessionId::try_new("eq-ok").unwrap()),
        input: user_text_input("hi").unwrap(),
        session_config: None,
        invocation_config: InvocationConfig::default(),
        tools: vec![],
        delivery: delivery_ok,
    });
    assert!(
        ok.is_ok(),
        "exact-admit: DeliveryLimits items=1 under max_event_queue=1 must admit, got {ok:?}"
    );
    drop(recv_ok);

    let (delivery_plus, recv_plus) =
        transaction_delivery(DeliveryLimits::try_new(2, 64 * 1024).unwrap()).unwrap();
    let (err, _) = (
        handle.submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("eq-plus").unwrap()),
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery: delivery_plus,
        }),
        recv_plus,
    );
    let err = err.expect_err("plus-one: DeliveryLimits items=2 must exceed max_event_queue=1");
    assert_eq!(err.kind, AdmissionErrorKind::InvalidConfiguration);
    assert!(
        err.message.contains("max_event_queue"),
        "expected max_event_queue message, got {:?}",
        err.message
    );

    shutdown_owner(started);
}

/// §23: `TransactionLimits.max_event_queue_bytes` ceiling over caller
/// `DeliveryLimits.max_event_bytes` — exact admits, plus-one rejects at admit.
#[test]
fn transaction_limits_max_event_queue_bytes_exact_admits_plus_one_rejects() {
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        max_event_queue_bytes: 1024,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let (delivery_ok, recv_ok) =
        transaction_delivery(DeliveryLimits::try_new(8, 1024).unwrap()).unwrap();
    let ok = handle.submit(TransactionSubmitRequest {
        channel_id: ChannelId::try_new("llm").unwrap(),
        session_id: Some(SessionId::try_new("eqb-ok").unwrap()),
        input: user_text_input("hi").unwrap(),
        session_config: None,
        invocation_config: InvocationConfig::default(),
        tools: vec![],
        delivery: delivery_ok,
    });
    assert!(
        ok.is_ok(),
        "exact-admit: DeliveryLimits bytes=1024 under max_event_queue_bytes=1024 must admit, got {ok:?}"
    );
    drop(recv_ok);

    let (delivery_plus, recv_plus) =
        transaction_delivery(DeliveryLimits::try_new(8, 1025).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("eqb-plus").unwrap()),
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery: delivery_plus,
        })
        .expect_err("plus-one: DeliveryLimits bytes=1025 must exceed max_event_queue_bytes=1024");
    drop(recv_plus);
    assert_eq!(err.kind, AdmissionErrorKind::InvalidConfiguration);
    assert!(
        err.message.contains("max_event_queue_bytes"),
        "expected max_event_queue_bytes message, got {:?}",
        err.message
    );

    shutdown_owner(started);
}

/// §23: `TransactionLimits.max_actor_commands` sizes the control `mpsc`;
/// exact-admit one Cancel while drain is held, plus-one → `ControlCapacityExceeded`.
#[test]
fn transaction_limits_max_actor_commands_plus_one_rejects() {
    let hold = Arc::new(ControlHoldGate::new());
    hold.hold();
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        max_actor_commands: 1,
        transaction_deadline: Duration::from_secs(5),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            hold_control: Some(Arc::clone(&hold)),
            ..RuntimeConfig::default()
        },
        // Hang keeps the admit non-terminal so a second terminate is not
        // AlreadyTerminal before the control queue fills.
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (receipt, _recv) = submit(&handle, Some("actor-cmd")).expect("admit Hang");

    let cancel = TerminationMode::Cancel {
        reason: CancellationReason {
            code: CancellationReasonCode::CallerRequested,
            detail: None,
        },
    };
    let first = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        cancel.clone(),
    );
    assert_eq!(
        first,
        TerminationDisposition::Accepted,
        "exact-admit: max_actor_commands=1 must accept the first control command"
    );

    let second = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        cancel,
    );
    assert_eq!(
        second,
        TerminationDisposition::ControlCapacityExceeded,
        "plus-one: control mpsc sized from TransactionLimits.max_actor_commands=1 must fail closed"
    );

    hold.release();
    shutdown_owner(started);
}

/// D-039: terminate dispositions are ledger-honest — never Full→AlreadyTerminal.
#[test]
fn terminate_after_cancel_is_ledger_honest() {
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let (receipt, recv) = submit(&handle, Some("term")).unwrap();
    let cancel = TerminationMode::Cancel {
        reason: CancellationReason {
            code: CancellationReasonCode::CallerRequested,
            detail: None,
        },
    };
    let first = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        cancel.clone(),
    );
    assert_eq!(first, TerminationDisposition::Accepted);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Wait until completion (tombstone may clear) or a short settle.
    let _ = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), recv.completion.recv()).await
    });
    let second = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        cancel,
    );
    // After terminal: AlreadyTerminal while the row remains, or NotFound once
    // the tombstone cleared. Never ControlCapacityExceeded→AlreadyTerminal lie.
    assert!(
        matches!(
            second,
            TerminationDisposition::AlreadyTerminal | TerminationDisposition::NotFound
        ),
        "expected ledger-honest AlreadyTerminal|NotFound, got {second:?}"
    );

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped after cancel+shutdown, got {outcome:?}"
    );
}

/// D-039: wait_stopped while Quiescing re-announces; TimedOut then later Stopped.
#[test]
fn wait_stopped_reannounce_while_quiescing_then_stopped() {
    let gate = Arc::new(StoppedGate::new());
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
            block_stopped: Some(Arc::clone(&gate)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, _recv) = submit(&handle, Some("reannounce")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    owner.begin_shutdown();
    let first = rt.block_on(owner.wait_stopped(Duration::from_millis(20)));
    assert!(
        matches!(first, ShutdownWaitOutcome::TimedOut(_)),
        "expected TimedOut under block_stopped, got {first:?}"
    );
    gate.release();
    let second = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(
        matches!(second, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped after re-announce + release, got {second:?}"
    );
}

/// §22.4 / Law 23 / M5.4: TaskSupervisor-owned JoinOnly-style work blocks Stopped.
#[test]
fn join_only_owned_task_blocks_stopped_until_released() {
    use crate::transaction::state::RuntimeState;

    let inject = Arc::new(JoinOnlySpillInject::new());
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
            inject_join_only_spill: Some(Arc::clone(&inject)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Wait until supervised JoinOnly-style task has entered park.
    let entered = rt.block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !inject.is_entered() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        inject.is_entered()
    });
    assert!(
        entered,
        "JoinOnly owned-task inject must enter before shutdown proof"
    );

    let mut owner = started.owner;
    assert!(
        owner.owned_task_count() >= 1,
        "TaskSupervisor must own JoinOnly work before begin_shutdown, owned={}",
        owner.owned_task_count()
    );
    owner.begin_shutdown();
    let mid = rt.block_on(owner.wait_stopped(Duration::from_millis(80)));
    assert!(
        matches!(mid, ShutdownWaitOutcome::TimedOut(_)),
        "JoinOnly owned task must keep Quiescing (not false Stopped), got {mid:?}"
    );
    assert_eq!(owner.state(), RuntimeState::Quiescing);
    assert!(
        owner.owned_task_count() >= 1,
        "JoinOnly must remain registered while Quiescing, owned={}",
        owner.owned_task_count()
    );

    inject.release();
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    match outcome {
        ShutdownWaitOutcome::Stopped(_) => {
            assert_eq!(owner.state(), RuntimeState::Stopped);
            assert_eq!(owner.owned_task_count(), 0);
        }
        other => panic!("expected Stopped after JoinOnly release, got {other:?}"),
    }
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
    assert!(
        gens.iter().all(|g| *g == 1),
        "all tickets must share gen 1, got {gens:?}"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Arc::into_inner fails if clones remain; we moved all thread clones out.
    let mut owner = Arc::try_unwrap(owner).unwrap_or_else(|_| panic!("owner still shared"));
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(outcome, ShutdownWaitOutcome::Stopped(_)));
}

/// §22.5: repeated TimedOut waiters observe compatible snapshots (same generation).
///
/// `wait_stopped` takes `&mut self` (thread join on Stopped), so true concurrent
/// `&mut` waiters are not an API surface; compatible Quiescing snapshots are.
#[test]
fn m6_wait_stopped_timed_out_snapshots_compatible() {
    use crate::transaction::state::RuntimeState;

    let gate = Arc::new(StoppedGate::new());
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            block_stopped: Some(Arc::clone(&gate)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let mut owner = started.owner;
    let ticket = owner.begin_shutdown();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let first = rt.block_on(owner.wait_stopped(Duration::ZERO));
    let ShutdownWaitOutcome::TimedOut(snap_a) = first else {
        panic!("expected TimedOut under block_stopped, got {first:?}");
    };
    assert_eq!(snap_a.generation, ticket.generation());
    assert_eq!(owner.state(), RuntimeState::Quiescing);

    let second = rt.block_on(owner.wait_stopped(Duration::ZERO));
    let ShutdownWaitOutcome::TimedOut(snap_b) = second else {
        panic!("second wait must TimedOut while gated, got {second:?}");
    };
    assert_eq!(
        snap_a.generation, snap_b.generation,
        "compatible TimedOut snapshots share shutdown generation"
    );

    gate.release();
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// M6 / D-004: Seal with authoritative session id replaces synthetic key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_publisher_prefers_authoritative_session_on_seal() {
    use super::super::event_publisher::{run_event_publisher, EventPublisherCommand, SealCommand};
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
    let (admit, cmd_rx) = super::super::event_publisher::OrdinaryCmdAdmit::channel(8);
    let (seal_tx, seal_rx) = mpsc::channel(1);
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

    // First ordinary event invents tx-{id}.
    admit
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
    seal_tx
        .send(SealCommand {
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
            deadline: std::time::Instant::now() + Duration::from_secs(5),
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
