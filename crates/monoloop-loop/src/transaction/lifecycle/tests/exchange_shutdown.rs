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
    // Drain events concurrently with completion so a late publisher cannot
    // leave Text unread when completion wins the race (D-053 advisor residual).
    let (completion, saw_text) = rt.block_on(async {
        let mut events = receiver.events;
        let completion_fut = receiver.completion.recv();
        tokio::pin!(completion_fut);
        let mut saw_text = false;
        let completion = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                tokio::select! {
                    biased;
                    c = &mut completion_fut => {
                        break c.expect("completion channel closed");
                    }
                    ev = events.recv() => {
                        if let Some(ev) = ev {
                            if let monoloop_contracts::TransactionEventPayload::CanonicalUnit(unit) =
                                &ev.payload
                            {
                                if let monoloop_contracts::CanonicalUnit::Text(t) =
                                    &unit.snapshot().unit
                                {
                                    // TestTextEncoder appends ". " to "hi"
                                    assert!(
                                        t.content.contains("hi"),
                                        "unexpected text: {}",
                                        t.content
                                    );
                                    saw_text = true;
                                }
                            }
                        }
                    }
                }
            }
        })
        .await
        .expect("completion timed out");
        while let Ok(ev) = events.try_recv() {
            if let monoloop_contracts::TransactionEventPayload::CanonicalUnit(unit) = &ev.payload {
                if let monoloop_contracts::CanonicalUnit::Text(t) = &unit.snapshot().unit {
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
        monoloop_contracts::TransactionEndKind::Completed,
        "Fake DirectLlm echo must Complete; got {:?}",
        completion.end.kind
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
    use super::super::ReservationPool;
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

/// D-049: wait_stopped budget covers executor teardown join; Stopped only after join.
#[test]
fn wait_stopped_times_out_during_executor_teardown_then_completes() {
    use crate::transaction::state::RuntimeState;

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
            hold_executor_teardown: Some(Arc::clone(&gate)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, _recv) = submit(&handle, Some("teardown")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    owner.begin_shutdown();
    // Drain can complete quickly; join waits on teardown gate — short wait TimedOut.
    let first = rt.block_on(owner.wait_stopped(Duration::from_millis(50)));
    assert!(
        matches!(first, ShutdownWaitOutcome::TimedOut(_)),
        "D-049: short wait during executor teardown must TimedOut, got {first:?}"
    );
    assert_eq!(
        owner.state(),
        RuntimeState::Quiescing,
        "public state must stay Quiescing until OS thread join"
    );
    gate.release();
    let second = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(
        matches!(second, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped after teardown release + join, got {second:?}"
    );
    assert_eq!(owner.state(), RuntimeState::Stopped);
}

/// D-040 / §22.1: short wait MUST TimedOut while Quiescing (not conditional Stopped).
#[test]
fn short_wait_may_timeout_while_quiescing_then_complete() {
    use crate::transaction::state::RuntimeState;

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
    let (_r, _recv) = submit(&handle, Some("q")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    owner.begin_shutdown();
    let first = rt.block_on(owner.wait_stopped(Duration::ZERO));
    assert!(
        matches!(first, ShutdownWaitOutcome::TimedOut(_)),
        "§22.1 / D-040: short wait must TimedOut under block_stopped, got {first:?}"
    );
    assert_eq!(owner.state(), RuntimeState::Quiescing);
    gate.release();
    let second = rt.block_on(owner.wait_stopped(Duration::from_secs(2)));
    assert!(
        matches!(second, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped after release, got {second:?}"
    );
}

/// D-040 / §22.1: parked Hang worker cannot delay synchronous admission.
#[test]
fn parked_worker_cannot_delay_synchronous_admission() {
    let limits = TransactionLimits {
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r1, _recv1) = submit(&handle, Some("hang1")).expect("first admit");
    // Give Hang exchange a moment to park a runtime worker.
    std::thread::sleep(Duration::from_millis(20));
    let h2 = handle.clone();
    let t0 = Instant::now();
    let join = std::thread::spawn(move || submit(&h2, Some("hang2")));
    let (_r2, _recv2) = join.join().unwrap().expect("second admit");
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(250),
        "synchronous admit must not wait on parked Hang worker, elapsed={elapsed:?}"
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
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
}

/// D-040 / §22.1: start-queue full rolls back ledger, session, delivery, permits.
#[test]
fn start_queue_full_rolls_back_all_permits() {
    let hold = Arc::new(StartHoldGate::new());
    hold.hold();
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
            hold_start: Some(Arc::clone(&hold)),
            // Queue of 1 fills while reservation pool still has headroom (2).
            start_queue_capacity: Some(1),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let channel = ChannelId::try_new("llm").unwrap();

    let (_r1, _recv1) = submit(&handle, Some("q1")).expect("first fills start queue");
    // Brief settle so the Start is enqueued while drain is held.
    std::thread::sleep(Duration::from_millis(10));

    let (err, rejected_rx) = submit_ports(&handle, Some("q2"));
    let err = err.expect_err("start queue full");
    assert_eq!(err.kind, AdmissionErrorKind::SpawnFailed);

    // Rollback: only the first admission's reservation remains.
    assert_eq!(started.owner.global_reservations(), 1);
    assert_eq!(started.owner.channel_reservations(&channel), 1);
    assert_eq!(started.owner.ledger_len(), 1);
    assert_rejected_silent(rejected_rx);

    // Rolled-back session is free: retry fails on the still-full queue, not
    // SessionAlreadyActive.
    let (err2, recv2) = submit_ports(&handle, Some("q2"));
    assert_eq!(
        err2.expect_err("queue still full").kind,
        AdmissionErrorKind::SpawnFailed
    );
    assert_rejected_silent(recv2);

    hold.release();
    shutdown_owner(started);
}

/// D-040 / §22.1: parked unprocessed Starts still complete on shutdown.
#[test]
fn parked_starts_reach_stopped_on_shutdown() {
    let hold = Arc::new(StartHoldGate::new());
    hold.hold();
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
            hold_start: Some(Arc::clone(&hold)),
            start_queue_capacity: Some(2),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let mut receivers = Vec::new();
    for i in 0..2 {
        let (_r, recv) = submit(&handle, Some(&format!("park{i}"))).unwrap();
        receivers.push(recv);
    }
    assert_eq!(started.owner.ledger_len(), 2);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    // Keep start drain held: unprocessed Starts stay in the queue while control
    // shutdown still reaches Stopped (D-039 parked-Start proof).
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    let _ = hold;
    assert!(
        matches!(
            outcome,
            ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == 2
        ),
        "parked Starts must still get completions, got {outcome:?}"
    );
    for recv in receivers {
        let c = rt.block_on(recv.completion.recv()).unwrap();
        assert_eq!(
            c.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
        );
        // D-041: shutdown-before-Start never Sealed — not Published.
        assert_eq!(
            c.terminal_event_delivery,
            monoloop_contracts::TerminalEventDelivery::NotAttempted
        );
    }
}
