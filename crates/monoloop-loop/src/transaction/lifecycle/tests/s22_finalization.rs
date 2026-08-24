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

/// §22.2: shutdown between Seal and completion send cannot lose ledger/completion.
#[test]
fn s22_2_shutdown_between_seal_and_completion_keeps_completion() {
    use crate::transaction::state::RuntimeState;

    let hold = Arc::new(FinalizerHoldGate::new());
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(5),
        cleanup_deadline: Duration::from_millis(200),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            hold_finalizer_after_seal: Some(Arc::clone(&hold)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, mut recv) = submit(&handle, Some("seal-hold")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Wait until Seal publishes EndedEvent while Finalizer is held before completion.
    let saw_ended = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(ev) = recv.events.recv().await {
                if matches!(
                    ev.payload,
                    monoloop_contracts::TransactionEventPayload::EndedEvent(_)
                ) {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false)
    });
    assert!(saw_ended, "Seal must publish Ended before completion hold");

    // Shutdown while Finalizer is still between Seal and completion send.
    let mut owner = started.owner;
    owner.begin_shutdown();
    let mid = rt.block_on(owner.wait_stopped(Duration::from_millis(100)));
    assert!(
        matches!(mid, ShutdownWaitOutcome::TimedOut(_)),
        "held Finalizer must keep Quiescing (not false Stopped), got {mid:?}"
    );
    assert_eq!(owner.state(), RuntimeState::Quiescing);

    // Release completion publish; hard-grace must not have dropped the attempt.
    hold.release();
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(5)));
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => {
            assert_eq!(
                r.completions_published, 1,
                "Seal→completion must not lose the one completion attempt"
            );
            assert_eq!(r.completions_invariant_failed, 0);
        }
        other => panic!("expected Stopped, got {other:?}"),
    }
    let completion = rt
        .block_on(recv.completion.recv())
        .expect("completion must arrive after Finalizer release");
    assert!(matches!(
        completion.end.kind,
        TransactionEndKind::Completed | TransactionEndKind::RuntimeShutdown
    ));
}

/// §22.3: spawn registers in the supervisor before the user future can first-poll.
#[tokio::test(flavor = "current_thread")]
async fn s22_3_spawn_registers_before_first_poll() {
    use super::super::task_supervisor::{TaskClass, TaskSupervisor};
    use std::sync::atomic::{AtomicBool, Ordering};

    let tx = TransactionId::generate();
    let entered = Arc::new(AtomicBool::new(false));
    let (block_tx, block_rx) = tokio::sync::oneshot::channel::<()>();
    let entered_task = Arc::clone(&entered);

    let mut tasks = TaskSupervisor::new();
    let id = tasks.spawn(TaskClass::TransactionCoordinator(tx), async move {
        // First action of the user future — must run only after registration.
        entered_task.store(true, Ordering::SeqCst);
        let _ = block_rx.await;
    });

    // Registration is synchronous in `spawn` before the start-gate release.
    assert_eq!(tasks.registered_count(), 1);
    assert_eq!(tasks.tasks_for(&tx), vec![id]);

    // Allow one poll; user future may now set `entered`.
    tokio::task::yield_now().await;
    assert!(
        entered.load(Ordering::SeqCst),
        "start-gate must release so the registered future can poll"
    );
    assert_eq!(
        tasks.registered_count(),
        1,
        "registration must outlive first poll"
    );

    let _ = block_tx.send(());
    let finished = tasks.join_next().await.expect("join");
    assert_eq!(finished.0, id);
    assert_eq!(tasks.registered_count(), 0);
}

/// §22.3: abort is followed by an observed join before stopped proof.
#[tokio::test(flavor = "current_thread")]
async fn s22_3_abort_then_observed_join() {
    use super::super::task_supervisor::{TaskClass, TaskExit, TaskSupervisor};

    let tx = TransactionId::generate();
    let mut tasks = TaskSupervisor::new();
    let id = tasks.spawn(TaskClass::EventPublisher(tx), async {
        std::future::pending::<()>().await;
    });
    assert_eq!(tasks.registered_count(), 1);

    tasks.abort(id);
    let (joined_id, class, exit) = tasks.join_next().await.expect("join after abort");
    assert_eq!(joined_id, id);
    assert!(matches!(class, TaskClass::EventPublisher(_)));
    assert_eq!(exit, TaskExit::Cancelled);
    assert!(tasks.is_empty());
}

/// §22.3: a yielding abortable future is aborted and joined.
#[tokio::test(flavor = "current_thread")]
async fn s22_3_yielding_abortable_aborted_and_joined() {
    use super::super::task_supervisor::{TaskClass, TaskExit, TaskSupervisor};

    let tx = TransactionId::generate();
    let mut tasks = TaskSupervisor::new();
    let id = tasks.spawn(TaskClass::LoopRuntime(tx), async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    // Let it yield at least once.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    tasks.abort(id);
    let (joined_id, _, exit) = tasks.join_next().await.expect("join yielding abort");
    assert_eq!(joined_id, id);
    assert_eq!(exit, TaskExit::Cancelled);
    assert_eq!(tasks.registered_count(), 0);
}

/// §22.3: abort_and_drain observes joins; counts return to zero (non-kill path).
#[tokio::test(flavor = "current_thread")]
async fn s22_3_abort_and_drain_counts_to_zero() {
    use super::super::task_supervisor::{TaskClass, TaskSupervisor};

    let tx = TransactionId::generate();
    let mut tasks = TaskSupervisor::new();
    for _ in 0..3 {
        tasks.spawn(
            TaskClass::ConnectorOwner(tx, monoloop_contracts::ExchangeId::generate()),
            async {
                std::future::pending::<()>().await;
            },
        );
    }
    assert_eq!(tasks.registered_count(), 3);
    assert!(tasks.abort_and_drain().await);
    assert!(tasks.is_empty());
    assert_eq!(tasks.registered_count(), 0);
    assert!(tasks.tasks_for(&tx).is_empty());
}

/// §22.3: runtime shutdown (normal path) leaves owned task count at zero.
#[test]
fn s22_3_runtime_normal_path_counts_to_zero() {
    use crate::transaction::state::RuntimeState;

    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let (_r, recv) = submit(&handle, Some("own-ok")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(3), recv.completion.recv()).await
        })
        .expect("completion timeout")
        .expect("completion");
    assert!(matches!(
        completion.end.kind,
        TransactionEndKind::Completed | TransactionEndKind::RuntimeShutdown
    ));

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => {
            assert_eq!(r.completions_published, 1);
            assert_eq!(owner.state(), RuntimeState::Stopped);
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(owner.owned_task_count(), 0);
        }
        other => panic!("expected Stopped with zero owned tasks, got {other:?}"),
    }
}

/// §22.3: cancel path still drains owned tasks to zero before Stopped.
#[test]
fn s22_3_runtime_cancel_path_counts_to_zero() {
    use crate::transaction::state::RuntimeState;

    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let (receipt, recv) = submit(&handle, Some("own-cancel")).unwrap();
    let disp = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    assert!(matches!(
        disp,
        TerminationDisposition::Accepted | TerminationDisposition::AlreadyTerminal
    ));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _ = rt.block_on(recv.completion.recv());

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(_) => {
            assert_eq!(owner.state(), RuntimeState::Stopped);
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(owner.owned_task_count(), 0);
        }
        other => panic!("expected Stopped after cancel drain, got {other:?}"),
    }
}

/// §22.3: coordinator panic / failure path still reaches zero owned tasks.
#[test]
fn s22_3_runtime_failure_path_counts_to_zero() {
    use crate::transaction::state::RuntimeState;

    // PanicEncoder forces coordinator failure → InvariantFailed completion.
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let mut binding = llm_binding("llm", 2);
    binding.encoder = Arc::new(PanicEncoder);
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![binding]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, recv) = submit(&handle, Some("own-fail")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(recv.completion.recv()).expect("completion");
    assert_eq!(completion.end.kind, TransactionEndKind::InvariantFailed);

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(_) => {
            assert_eq!(owner.state(), RuntimeState::Stopped);
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(owner.owned_task_count(), 0);
        }
        other => panic!("expected Stopped after failure drain, got {other:?}"),
    }
}

/// §22.3: Connector/Interpreter pumps stay supervisor-owned (joined) — shutdown
/// after a live exchange does not leave detached pump tasks.
#[test]
fn s22_3_exchange_pumps_joined_not_detached() {
    // Hang keeps ConnectorOwner parked under TaskSupervisor while we shut down.
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
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, _recv) = submit(&handle, Some("pump-own")).unwrap();
    // Give Hang exchange time to register ConnectorOwner under the supervisor.
    std::thread::sleep(Duration::from_millis(50));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(_) => {
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(
                owner.owned_task_count(),
                0,
                "Connector/Interpreter pumps must be joined, not detached"
            );
        }
        other => panic!("expected Stopped with joined pumps, got {other:?}"),
    }
    let _ = handle;
}

/// §22.2: every admission → exactly one completion send attempt.
#[test]
fn s22_2_one_completion_per_admission() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let mut receivers = Vec::new();
    for i in 0..3 {
        let (_r, recv) = submit(&handle, Some(&format!("c{i}"))).unwrap();
        receivers.push(recv);
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => {
            assert_eq!(r.completions_published, 3);
            assert_eq!(r.completions_invariant_failed, 0);
        }
        other => panic!("expected Stopped, got {other:?}"),
    }
    for recv in receivers {
        let _ = rt.block_on(recv.completion.recv()).expect("completion");
    }
}

/// §22.2: receiver dropped before completion is accounted without a task leak.
#[test]
fn s22_2_dropped_completion_receiver_accounted() {
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let (_r, recv) = submit(&handle, Some("drop-c")).unwrap();
    drop(recv.completion); // host drops before completion
    let mut events = recv.events;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => {
            assert_eq!(r.completions_published, 1);
            assert_eq!(r.completions_receiver_dropped, 1);
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(owner.global_reservations(), 0);
        }
        other => panic!("expected Stopped after dropped receiver, got {other:?}"),
    }
    let _ = events.try_recv();
}

/// §22.2: coordinator panic → one InvariantFailed completion.
#[test]
fn s22_2_coordinator_panic_one_invariant_failed() {
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(5),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let mut binding = llm_binding("llm", 2);
    binding.encoder = Arc::new(PanicEncoder);
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![binding]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, recv) = submit(&handle, Some("panic")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), recv.completion.recv())
            .await
            .expect("completion timeout")
            .expect("completion")
    });
    assert_eq!(completion.end.kind, TransactionEndKind::InvariantFailed);
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => {
            assert_eq!(r.completions_published, 1);
        }
        other => panic!("expected Stopped, got {other:?}"),
    }
}

/// §22.2: cancel then force-terminate → one Terminated completion.
#[test]
fn s22_2_cancel_upgraded_to_force_terminate() {
    // Hang so cancel/force race before natural Completed.
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
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
    let (receipt, recv) = submit(&handle, Some("force")).unwrap();
    std::thread::sleep(Duration::from_millis(30));
    let _ = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    let _ = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::ForceTerminate {
            reason: TerminationReason {
                code: TerminationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), recv.completion.recv())
            .await
            .expect("completion timeout")
            .expect("completion")
    });
    assert_eq!(completion.end.kind, TransactionEndKind::Terminated);
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => assert_eq!(r.completions_published, 1),
        other => panic!("expected Stopped, got {other:?}"),
    }
}

/// §22.2: coordinator completion racing cancel → exactly one documented cause.
#[test]
fn s22_2_completion_racing_cancel_one_cause() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let (receipt, recv) = submit(&handle, Some("race-c")).unwrap();
    // Immediate cancel while echo may already be finishing.
    let _ = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), recv.completion.recv())
            .await
            .expect("completion timeout")
            .expect("completion")
    });
    assert!(
        matches!(
            completion.end.kind,
            TransactionEndKind::Cancelled | TransactionEndKind::Completed
        ),
        "one documented cause, got {:?}",
        completion.end.kind
    );
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => assert_eq!(r.completions_published, 1),
        other => panic!("expected Stopped, got {other:?}"),
    }
}

/// §22.2: no ordinary event is published after Seal / terminal attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_2_no_event_after_terminal_attempt() {
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

    let (reply_tx, reply_rx) = oneshot::channel();
    seal_tx
        .send(SealCommand {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: Some(SessionId::try_new("s").unwrap()),
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
    let seal = reply_rx.await.unwrap();
    assert_eq!(seal.delivery, TerminalEventDelivery::Published);
    let ended = receiver.events.recv().await.expect("ended");
    assert!(matches!(
        ended.payload,
        TransactionEventPayload::EndedEvent(_)
    ));

    // Post-Seal Publish must be ignored (no further events).
    let _ = admit
        .send(EventPublisherCommand::Publish(Box::new(
            TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("late", Some("x"), 64).unwrap(),
            }),
        )))
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        receiver.events.try_recv().is_err(),
        "no event after terminal attempt"
    );
    drop(admit);
    drop(seal_tx);
    let _ = pub_task.await;
}

/// D-047 / §22.2: full mailbox waits; drain preserves contiguous sequences and
/// payload count (no silent drop).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_2_failed_enqueue_consumes_no_sequence() {
    use super::super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TransactionDiagnostic,
        TransactionEventPayload, TransactionId,
    };
    use tokio::sync::mpsc;

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(1, 64 * 1024).unwrap()).unwrap();
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

    let diag = |tag: &str| {
        TransactionEventPayload::Diagnostic(TransactionDiagnostic {
            diagnostic: SafeDiagnostic::try_new("noop", Some(tag), 64).unwrap(),
        })
    };
    admit
        .send(EventPublisherCommand::Publish(Box::new(diag("a"))))
        .await
        .unwrap();
    // Fill capacity 1 without draining — second publish waits (D-047), does not drop.
    admit
        .send(EventPublisherCommand::Publish(Box::new(diag("b"))))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let first = receiver.events.recv().await.expect("first");
    assert_eq!(first.sequence, 1);
    let second = receiver.events.recv().await.expect("second after wait");
    assert_eq!(second.sequence, 2);
    admit
        .send(EventPublisherCommand::Publish(Box::new(diag("c"))))
        .await
        .unwrap();
    let third = receiver.events.recv().await.expect("third");
    assert_eq!(third.sequence, 3, "contiguous after waited enqueue");
    drop(admit);
    drop(_seal_tx);
    let _ = pub_task.await;
}

/// D-047: permanently full host queue → sticky DeadlineExceeded on Seal (not
/// Published / Completed).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d047_full_queue_seal_reports_deadline_not_published() {
    use super::super::event_publisher::{
        run_event_publisher, EventPublisherCommand, SealCommand, TerminalPublicationResult,
    };
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TerminalEventDelivery,
        TransactionDiagnostic, TransactionEndEvent, TransactionEndKind, TransactionEventPayload,
        TransactionId, TransactionUsage,
    };
    use tokio::sync::{mpsc, oneshot};

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, _receiver) =
        transaction_delivery(DeliveryLimits::try_new(1, 64 * 1024).unwrap()).unwrap();
    let (admit, cmd_rx) = super::super::event_publisher::OrdinaryCmdAdmit::channel(8);
    let (seal_tx, seal_rx) = mpsc::channel(1);
    let cancel = Arc::new(crate::transaction::sticky_cancel::StickyCancel::new());
    // Short deadline so the waiting second publish fails closed.
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        admit.clone(),
        seal_rx,
        Arc::clone(&cancel),
        std::time::Instant::now() + Duration::from_millis(80),
    ));

    let diag = || {
        TransactionEventPayload::Diagnostic(TransactionDiagnostic {
            diagnostic: SafeDiagnostic::try_new("noop", Some("x"), 64).unwrap(),
        })
    };
    admit
        .send(EventPublisherCommand::Publish(Box::new(diag())))
        .await
        .unwrap();
    // Do not drain — second publish waits until deadline.
    admit
        .send(EventPublisherCommand::Publish(Box::new(diag())))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    let (reply_tx, reply_rx) = oneshot::channel();
    seal_tx
        .send(SealCommand {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: None,
                channel_id: channel,
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
    let res: TerminalPublicationResult = reply_rx.await.expect("seal reply");
    assert_eq!(
        res.delivery,
        TerminalEventDelivery::DeadlineExceeded,
        "sticky wait failure must surface on Seal, got {:?}",
        res.delivery
    );
    assert_eq!(res.last_sequence, 1, "only the first event committed");
    drop(admit);
    drop(seal_tx);
    let _ = pub_task.await;
}

/// D-047: Seal on the dedicated channel succeeds even when the ordinary
/// command mpsc is Full — and pre-fence ordinary Publish is not lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d047_seal_priority_when_ordinary_cmd_queue_full() {
    use super::super::event_publisher::{
        run_event_publisher, EventPublisherCommand, SealCommand, TerminalPublicationResult,
    };
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TerminalEventDelivery,
        TransactionDiagnostic, TransactionEndEvent, TransactionEndKind, TransactionEventPayload,
        TransactionId, TransactionUsage,
    };
    use tokio::sync::{mpsc, oneshot};

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    // Roomy host mailbox so ordinary publishes can wait without sticky fail.
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 1024 * 1024).unwrap()).unwrap();
    let (admit, cmd_rx) = super::super::event_publisher::OrdinaryCmdAdmit::channel(1);
    let (seal_tx, seal_rx) = mpsc::channel(1);
    let cancel = Arc::new(crate::transaction::sticky_cancel::StickyCancel::new());
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        admit.clone(),
        seal_rx,
        Arc::clone(&cancel),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    let diag = || {
        TransactionEventPayload::Diagnostic(TransactionDiagnostic {
            diagnostic: SafeDiagnostic::try_new("noop", Some("x"), 64).unwrap(),
        })
    };
    // Fill ordinary capacity-1 queue and park a second send.
    admit
        .send(EventPublisherCommand::Publish(Box::new(diag())))
        .await
        .unwrap();
    let blocked = admit.try_send(EventPublisherCommand::Publish(Box::new(diag())));
    assert!(
        matches!(
            blocked,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
        ),
        "ordinary queue must be full"
    );

    let (reply_tx, reply_rx) = oneshot::channel();
    seal_tx
        .try_send(SealCommand {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: None,
                channel_id: channel,
                kind: TransactionEndKind::Completed,
                emitted_events: 0,
                usage: TransactionUsage::default(),
                diagnostics: vec![],
            },
            reply: reply_tx,
            deadline: std::time::Instant::now() + Duration::from_secs(5),
        })
        .expect("Seal must enqueue on dedicated channel while ordinary is Full");

    // Collect the full stream: accepted ordinary must appear before Ended.
    let mut evs = Vec::new();
    let res: TerminalPublicationResult = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::pin!(reply_rx);
        loop {
            tokio::select! {
                biased;
                reply = &mut reply_rx => break reply.expect("seal reply"),
                ev = receiver.events.recv() => {
                    evs.push(ev.expect("host event"));
                }
            }
        }
    })
    .await
    .expect("seal reply timeout");
    // Drain any events that raced after the reply.
    while let Ok(ev) = receiver.events.try_recv() {
        evs.push(ev);
    }

    assert_eq!(
        res.delivery,
        TerminalEventDelivery::Published,
        "Seal via priority channel must publish, got {:?}",
        res.delivery
    );
    assert!(
        evs.len() >= 2,
        "need ordinary + Ended, got {} events: {:?}",
        evs.len(),
        evs.iter().map(|e| e.sequence).collect::<Vec<_>>()
    );
    assert!(
        matches!(
            evs.first().map(|e| &e.payload),
            Some(TransactionEventPayload::Diagnostic(_))
        ),
        "first committed event must be the accepted ordinary diagnostic"
    );
    assert!(
        matches!(
            evs.last().map(|e| &e.payload),
            Some(TransactionEventPayload::EndedEvent(_))
        ),
        "last event must be EndedEvent"
    );
    for (i, ev) in evs.iter().enumerate() {
        assert_eq!(
            ev.sequence,
            (i as u64).saturating_add(1),
            "sequences must be contiguous starting at 1"
        );
    }
    assert_eq!(
        res.last_sequence,
        evs.len() as u64,
        "Seal last_sequence must match delivered count"
    );
    drop(admit);
    drop(seal_tx);
    let _ = pub_task.await;
}

/// D-047: a `send` that holds a pre-fence Sender clone and is blocked on capacity
/// when Seal closes admission MUST complete `Ok` and appear before `Ended`.
///
/// Schedule (forced): fill ordinary queue with publisher stopped → park second
/// send (sync after Sender clone) → queue Seal → start publisher. Biased Seal
/// closes admit, drain frees capacity, parked send completes into the drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d047_seal_fence_parked_send_delivered_before_ended() {
    use super::super::event_publisher::{
        run_event_publisher, EventPublisherCommand, SealCommand, TerminalPublicationResult,
    };
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TerminalEventDelivery,
        TransactionDiagnostic, TransactionEndEvent, TransactionEndKind, TransactionEventPayload,
        TransactionId, TransactionUsage,
    };
    use tokio::sync::{mpsc, oneshot};

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 1024 * 1024).unwrap()).unwrap();
    // Cap 1; publisher is NOT started until the second send is proven parked.
    let (admit, cmd_rx) = super::super::event_publisher::OrdinaryCmdAdmit::channel(1);
    let (seal_tx, seal_rx) = mpsc::channel(1);
    let cancel = Arc::new(crate::transaction::sticky_cancel::StickyCancel::new());

    let diag = |label: &str| {
        TransactionEventPayload::Diagnostic(TransactionDiagnostic {
            diagnostic: SafeDiagnostic::try_new(label, Some(label), 64).unwrap(),
        })
    };

    admit
        .try_send(EventPublisherCommand::Publish(Box::new(diag("queued"))))
        .expect("fill ordinary capacity while publisher is stopped");
    assert!(
        matches!(
            admit.try_send(EventPublisherCommand::Publish(Box::new(diag("extra")))),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
        ),
        "ordinary queue must be full before parking"
    );

    let (holding_tx, holding_rx) = oneshot::channel();
    let parked_admit = admit.clone();
    let parked = tokio::spawn(async move {
        parked_admit
            .send_after_pre_fence_hold(
                EventPublisherCommand::Publish(Box::new(diag("parked"))),
                holding_tx,
            )
            .await
    });
    // Critical sync: Sender cloned under open admit before any Seal/close.
    tokio::time::timeout(Duration::from_secs(1), holding_rx)
        .await
        .expect("parked send must signal pre-fence hold")
        .expect("holding oneshot");
    // Still Full — parked future is waiting on capacity, not finished.
    assert!(
        matches!(
            admit.try_send(EventPublisherCommand::Publish(Box::new(diag("probe")))),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
        ),
        "parked send must still be blocked on a full queue at Seal time"
    );
    assert!(admit.is_open(), "admit must still be open when parked");

    let (reply_tx, reply_rx) = oneshot::channel();
    seal_tx
        .try_send(SealCommand {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: None,
                channel_id: channel.clone(),
                kind: TransactionEndKind::Completed,
                emitted_events: 0,
                usage: TransactionUsage::default(),
                diagnostics: vec![],
            },
            reply: reply_tx,
            deadline: std::time::Instant::now() + Duration::from_secs(2),
        })
        .expect("queue Seal before starting publisher");

    // Start publisher with Seal + backlog already pending (biased Seal wins).
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel,
        None,
        delivery.event_tx,
        cmd_rx,
        admit.clone(),
        seal_rx,
        Arc::clone(&cancel),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    let mut evs = Vec::new();
    let res: TerminalPublicationResult = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::pin!(reply_rx);
        loop {
            tokio::select! {
                biased;
                reply = &mut reply_rx => break reply.expect("seal reply"),
                ev = receiver.events.recv() => evs.push(ev.expect("event")),
            }
        }
    })
    .await
    .expect("timeout");
    while let Ok(ev) = receiver.events.try_recv() {
        evs.push(ev);
    }

    let parked_result = tokio::time::timeout(Duration::from_secs(1), parked)
        .await
        .expect("parked join")
        .expect("parked task");
    assert_eq!(
        parked_result,
        Ok(()),
        "pre-fence parked send must complete Ok after drain frees capacity"
    );

    assert_eq!(res.delivery, TerminalEventDelivery::Published);
    assert_eq!(evs.len(), 3, "queued + parked + Ended, got {}", evs.len());
    let code0 = match &evs[0].payload {
        TransactionEventPayload::Diagnostic(d) => d.diagnostic.code.as_str().to_string(),
        other => panic!("expected queued diagnostic, got {other:?}"),
    };
    let code1 = match &evs[1].payload {
        TransactionEventPayload::Diagnostic(d) => d.diagnostic.code.as_str().to_string(),
        other => panic!("expected parked diagnostic, got {other:?}"),
    };
    assert_eq!(code0, "queued", "first ordinary must be pre-queued");
    assert_eq!(code1, "parked", "parked ordinary must appear before Ended");
    assert!(matches!(
        &evs[2].payload,
        TransactionEventPayload::EndedEvent(_)
    ));
    assert_eq!(evs[0].sequence, 1);
    assert_eq!(evs[1].sequence, 2);
    assert_eq!(evs[2].sequence, 3);
    assert!(receiver.events.try_recv().is_err(), "nothing after Ended");
    drop(admit);
    drop(seal_tx);
    let _ = pub_task.await;
}

/// D-047: Seal fence drains a queued ordinary Publish before Ended (no silent loss).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d047_seal_fence_drains_queued_ordinary_before_ended() {
    use super::super::event_publisher::{
        run_event_publisher, EventPublisherCommand, SealCommand, TerminalPublicationResult,
    };
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TerminalEventDelivery,
        TransactionDiagnostic, TransactionEndEvent, TransactionEndKind, TransactionEventPayload,
        TransactionId, TransactionUsage,
    };
    use tokio::sync::{mpsc, oneshot};

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 1024 * 1024).unwrap()).unwrap();
    // Cap 8 so Seal + ordinary can both sit ready; biased Seal must still drain.
    let (admit, cmd_rx) = super::super::event_publisher::OrdinaryCmdAdmit::channel(8);
    let (seal_tx, seal_rx) = mpsc::channel(1);
    let cancel = Arc::new(crate::transaction::sticky_cancel::StickyCancel::new());
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        admit.clone(),
        seal_rx,
        Arc::clone(&cancel),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    // Pause the publisher briefly by not yielding work until both are queued.
    let diag = TransactionEventPayload::Diagnostic(TransactionDiagnostic {
        diagnostic: SafeDiagnostic::try_new("pre-fence", Some("keep"), 64).unwrap(),
    });
    admit
        .try_send(EventPublisherCommand::Publish(Box::new(diag)))
        .expect("ordinary");
    let (reply_tx, reply_rx) = oneshot::channel();
    seal_tx
        .try_send(SealCommand {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: None,
                channel_id: channel,
                kind: TransactionEndKind::Completed,
                emitted_events: 0,
                usage: TransactionUsage::default(),
                diagnostics: vec![],
            },
            reply: reply_tx,
            deadline: std::time::Instant::now() + Duration::from_secs(2),
        })
        .expect("seal");

    let mut evs = Vec::new();
    let res: TerminalPublicationResult = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::pin!(reply_rx);
        loop {
            tokio::select! {
                biased;
                reply = &mut reply_rx => break reply.expect("seal reply"),
                ev = receiver.events.recv() => evs.push(ev.expect("event")),
            }
        }
    })
    .await
    .expect("timeout");
    while let Ok(ev) = receiver.events.try_recv() {
        evs.push(ev);
    }

    assert_eq!(res.delivery, TerminalEventDelivery::Published);
    assert_eq!(evs.len(), 2, "ordinary + Ended only");
    assert!(matches!(
        evs[0].payload,
        TransactionEventPayload::Diagnostic(_)
    ));
    assert!(matches!(
        evs[1].payload,
        TransactionEventPayload::EndedEvent(_)
    ));
    assert_eq!(evs[0].sequence, 1);
    assert_eq!(evs[1].sequence, 2);
    drop(admit);
    drop(seal_tx);
    let _ = pub_task.await;
}

/// P2: configured `terminal_event_delivery_deadline` is honored exactly (no silent floor).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d047_terminal_deadline_uses_configured_value_exactly() {
    use super::super::event_publisher::{
        run_event_publisher, SealCommand, TerminalPublicationResult,
    };
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, TerminalEventDelivery, TransactionEndEvent,
        TransactionEndKind, TransactionId, TransactionUsage,
    };
    use tokio::sync::{mpsc, oneshot};

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    // Full host mailbox — Seal enqueue must wait.
    let (delivery, _receiver) =
        transaction_delivery(DeliveryLimits::try_new(1, 64 * 1024).unwrap()).unwrap();
    {
        use monoloop_contracts::{
            SafeDiagnostic, TransactionDiagnostic, TransactionEvent, TransactionEventPayload,
        };
        delivery
            .event_tx
            .try_send(TransactionEvent {
                transaction_id: tx_id,
                channel_id: channel.clone(),
                session_id: SessionId::try_new("s").unwrap(),
                sequence: 1,
                payload: TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                    diagnostic: SafeDiagnostic::try_new("occupy", Some("x"), 64).unwrap(),
                }),
            })
            .expect("occupy");
    }
    let (admit, cmd_rx) = super::super::event_publisher::OrdinaryCmdAdmit::channel(8);
    let (seal_tx, seal_rx) = mpsc::channel(1);
    let cancel = Arc::new(crate::transaction::sticky_cancel::StickyCancel::new());
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        admit.clone(),
        seal_rx,
        Arc::clone(&cancel),
        std::time::Instant::now() + Duration::from_secs(600),
    ));

    // Caller-configured 1ms — must not be silently raised to 50ms.
    let configured = Duration::from_millis(1);
    let seal_deadline = std::time::Instant::now() + configured;
    let started = std::time::Instant::now();
    let (reply_tx, reply_rx) = oneshot::channel();
    seal_tx
        .try_send(SealCommand {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: None,
                channel_id: channel,
                kind: TransactionEndKind::Completed,
                emitted_events: 0,
                usage: TransactionUsage::default(),
                diagnostics: vec![],
            },
            reply: reply_tx,
            deadline: seal_deadline,
        })
        .expect("seal");

    let res: TerminalPublicationResult = tokio::time::timeout(Duration::from_millis(200), reply_rx)
        .await
        .expect("must conclude near the 1ms budget, not a 50ms floor")
        .expect("reply");
    let elapsed = started.elapsed();
    assert_eq!(res.delivery, TerminalEventDelivery::DeadlineExceeded);
    assert!(
        elapsed < Duration::from_millis(40),
        "exact 1ms budget must not be clamped to 50ms; elapsed={elapsed:?}"
    );
    drop(seal_tx);
    let _ = pub_task.await;
}

/// D-047 reopen: Seal enqueue uses SealCommand.deadline (terminal budget), not
/// the long ordinary transaction deadline — so Finalizer and publisher share
/// one authoritative Instant and Ended cannot publish after completion timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d047_seal_uses_terminal_deadline_not_transaction_deadline() {
    use super::super::event_publisher::{
        run_event_publisher, SealCommand, TerminalPublicationResult,
    };
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, TerminalEventDelivery, TransactionEndEvent,
        TransactionEndKind, TransactionId, TransactionUsage,
    };
    use tokio::sync::{mpsc, oneshot};

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    // Capacity 1, never drained → Seal enqueue waits.
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(1, 64 * 1024).unwrap()).unwrap();
    // Occupy the single host slot so Seal cannot publish immediately.
    {
        use monoloop_contracts::{
            SafeDiagnostic, TransactionDiagnostic, TransactionEvent, TransactionEventPayload,
        };
        let occupy = TransactionEvent {
            transaction_id: tx_id,
            channel_id: channel.clone(),
            session_id: SessionId::try_new("s").unwrap(),
            sequence: 1,
            payload: TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("occupy", Some("x"), 64).unwrap(),
            }),
        };
        delivery
            .event_tx
            .try_send(occupy)
            .expect("occupy host slot");
    }
    let (admit, cmd_rx) = super::super::event_publisher::OrdinaryCmdAdmit::channel(8);
    let (seal_tx, seal_rx) = mpsc::channel(1);
    let cancel = Arc::new(crate::transaction::sticky_cancel::StickyCancel::new());
    // Ordinary transaction deadline is long — must NOT govern Seal.
    let long_tx_deadline = std::time::Instant::now() + Duration::from_secs(600);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        admit.clone(),
        seal_rx,
        Arc::clone(&cancel),
        long_tx_deadline,
    ));

    let seal_deadline = std::time::Instant::now() + Duration::from_millis(80);
    let (reply_tx, reply_rx) = oneshot::channel();
    seal_tx
        .try_send(SealCommand {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: None,
                channel_id: channel,
                kind: TransactionEndKind::Completed,
                emitted_events: 0,
                usage: TransactionUsage::default(),
                diagnostics: vec![],
            },
            reply: reply_tx,
            deadline: seal_deadline,
        })
        .expect("seal enqueue");

    let res: TerminalPublicationResult = tokio::time::timeout(Duration::from_millis(500), reply_rx)
        .await
        .expect("Seal must conclude under terminal deadline, not 600s tx deadline")
        .expect("seal reply");
    assert_eq!(
        res.delivery,
        TerminalEventDelivery::DeadlineExceeded,
        "Seal must fail on terminal deadline while host is full, got {:?}",
        res.delivery
    );

    // Drain later must not reveal a late Ended (publisher already replied).
    while receiver.events.try_recv().is_ok() {}
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        receiver.events.try_recv().is_err(),
        "no late Ended after Seal DeadlineExceeded reply"
    );
    drop(seal_tx);
    let _ = pub_task.await;
}
