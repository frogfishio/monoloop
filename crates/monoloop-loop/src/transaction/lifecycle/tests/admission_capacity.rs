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

/// §18.4: Dropping RuntimeOwner initiates shutdown and joins the executor thread
/// (no abandon-after-grace detach). Empty cooperative runtime reaches Stopped.
#[test]
fn runtime_owner_drop_joins_executor_thread_reaches_stopped() {
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    // Contract violation path: drop without wait_stopped — ownership still joins.
    drop(started.owner);
    assert_eq!(
        handle.state(),
        RuntimeState::Stopped,
        "§18.4 Drop must join through Stopped, not abandon Quiescing"
    );
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
    let (err, recv) = submit_ports(&handle, Some("same"));
    assert_eq!(
        err.expect_err("duplicate").kind,
        AdmissionErrorKind::SessionAlreadyActive
    );
    assert_rejected_silent(recv);

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
    let (err, recv) = submit_ports(&handle, Some("b"));
    assert_eq!(
        err.expect_err("capacity").kind,
        AdmissionErrorKind::CapacityExceeded
    );
    assert_rejected_silent(recv);

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

/// D-015 / §23: `ChannelLimits.max_distinct_sessions` exact admits; plus-one
/// rejects on v2 ledger admission (Hang-pinned so slots stay occupied).
///
/// Distinct from global/per-channel `max_active` capacity — channel max_active
/// and global headroom remain above the distinct-session bound.
#[test]
fn max_distinct_sessions_exact_admits_plus_one_rejects() {
    let distinct_max = 2usize;
    let limits = TransactionLimits {
        max_active_transactions: 8,
        max_active_per_channel: 8,
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let mut binding = hang_llm_binding("llm", 8);
    binding.limits.max_distinct_sessions = distinct_max;
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

    let mut receivers = Vec::new();
    for i in 0..distinct_max {
        let (receipt, recv) =
            submit(&handle, Some(&format!("ds-{i}"))).expect("exact distinct session");
        let _ = receipt;
        receivers.push(recv);
    }
    assert_eq!(started.owner.ledger_len(), distinct_max);

    let (err, overflow_recv) = submit_ports(&handle, Some("ds-overflow"));
    assert_eq!(
        err.expect_err("distinct sessions plus-one").kind,
        AdmissionErrorKind::CapacityExceeded
    );
    assert_rejected_silent(overflow_recv);
    assert_eq!(started.owner.ledger_len(), distinct_max);

    // Session-less admit does not consume a distinct-session slot at admit
    // (no SessionKey until external claim).
    let (none_ok, none_recv) = submit_ports(&handle, None);
    assert!(
        none_ok.is_ok(),
        "session-less admit must not hit max_distinct_sessions, got {none_ok:?}"
    );
    receivers.push(none_recv);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(10)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(
                report.completions_published as usize,
                distinct_max + 1,
                "exact distinct + session-less must each complete once"
            );
        }
        other => panic!("expected Stopped after distinct-sessions proof, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0);

    for recv in receivers {
        let _ = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
    }
}

/// Broader stress: N+1 barrier submits at exact `max_active` (global).
///
/// Distinct from sequential `capacity_plus_one_rejects`. Hang pins capacity so
/// completions cannot free slots mid-race; exactly `max` admit and one
/// `CapacityExceeded` reject are required.
#[test]
fn concurrent_global_capacity_exhaustion_admits_exactly_max() {
    let max = 4usize;
    let limits = TransactionLimits {
        max_active_transactions: max,
        max_active_per_channel: max,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", max)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let n = max + 1;
    let barrier = Arc::new(Barrier::new(n));
    let mut joins = Vec::new();
    for i in 0..n {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        let session = format!("cap-race-{i}");
        joins.push(std::thread::spawn(move || {
            b.wait();
            submit_ports(&h, Some(&session))
        }));
    }

    let mut admitted = 0usize;
    let mut rejected = 0usize;
    let mut receivers = Vec::new();
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(receipt) => {
                admitted += 1;
                let _ = receipt;
                receivers.push(recv);
            }
            Err(e) => {
                assert_eq!(e.kind, AdmissionErrorKind::CapacityExceeded);
                rejected += 1;
                assert_rejected_silent(recv);
            }
        }
    }
    assert_eq!(
        admitted, max,
        "exactly max_active must admit under barrier race"
    );
    assert_eq!(rejected, 1, "exactly one CapacityExceeded overflow");
    assert_eq!(started.owner.ledger_len(), max);
    assert_eq!(started.owner.global_reservations(), max);

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
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(
                report.completions_published as usize, max,
                "every admitted Hang tx must complete once"
            );
        }
        other => panic!("expected Stopped after capacity race, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0);
    assert_eq!(owner.global_reservations(), 0);

    for recv in receivers {
        let completion = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
        assert!(matches!(
            completion.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
                | monoloop_contracts::TransactionEndKind::Cancelled
                | monoloop_contracts::TransactionEndKind::Terminated
        ));
    }
}

/// Broader stress: per-channel max tighter than global — N+1 barrier submits
/// on one channel admit exactly `max_active_per_channel`.
#[test]
fn concurrent_per_channel_capacity_exhaustion_admits_exactly_channel_max() {
    let global = 8usize;
    let channel_max = 2usize;
    let limits = TransactionLimits {
        max_active_transactions: global,
        max_active_per_channel: channel_max,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", channel_max)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let n = channel_max + 1;
    let barrier = Arc::new(Barrier::new(n));
    let mut joins = Vec::new();
    for i in 0..n {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        let session = format!("ch-cap-race-{i}");
        joins.push(std::thread::spawn(move || {
            b.wait();
            submit_ports(&h, Some(&session))
        }));
    }

    let mut admitted = 0usize;
    let mut rejected = 0usize;
    let mut receivers = Vec::new();
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(receipt) => {
                admitted += 1;
                let _ = receipt;
                receivers.push(recv);
            }
            Err(e) => {
                assert_eq!(e.kind, AdmissionErrorKind::CapacityExceeded);
                rejected += 1;
                assert_rejected_silent(recv);
            }
        }
    }
    assert_eq!(
        admitted, channel_max,
        "exactly max_active_per_channel must admit"
    );
    assert_eq!(rejected, 1, "exactly one per-channel CapacityExceeded");
    assert_eq!(started.owner.ledger_len(), channel_max);
    let llm = ChannelId::try_new("llm").unwrap();
    assert_eq!(
        started.owner.global_reservations(),
        channel_max,
        "provisional global permits held for admitted only"
    );
    assert_eq!(
        started.owner.channel_reservations(&llm),
        channel_max,
        "channel pool must be at exact max"
    );
    assert!(
        channel_max < global,
        "topology must leave global headroom so overflow is channel-scoped"
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
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(report.completions_published as usize, channel_max);
        }
        other => panic!("expected Stopped after per-channel capacity race, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0);
    assert_eq!(owner.global_reservations(), 0);
    assert_eq!(owner.channel_reservations(&llm), 0);

    for recv in receivers {
        let completion = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
        assert!(matches!(
            completion.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
                | monoloop_contracts::TransactionEndKind::Cancelled
                | monoloop_contracts::TransactionEndKind::Terminated
        ));
    }
}

/// WP-12 Golden progress: concurrent multi-Channel / multi-session load with
/// SessionKey isolation (Fake Hang path; not live Grok).
///
/// Distinct from single-channel capacity races and from
/// `s22_6_same_session_string_different_channels_isolated` (two-submit smoke):
/// barrier-raced admits across ≥3 Channels, shared external session strings
/// across Channels (independent SessionKeys), same-Channel duplicate reject
/// with capacity headroom, then fill-to-capacity `CapacityExceeded`, then one
/// shutdown completing every admission.
#[test]
fn multi_channel_multi_session_concurrent_load() {
    let channel_ids = ["ch-a", "ch-b", "ch-c"];
    let per_channel = 4usize;
    // Leave one free slot per channel so SessionAlreadyActive is observable
    // (capacity check must not mask the session reject).
    let occupied_per_channel = per_channel - 1;
    let global = channel_ids.len() * per_channel;
    let wave = channel_ids.len() * occupied_per_channel;
    let limits = TransactionLimits {
        max_active_transactions: global,
        max_active_per_channel: per_channel,
        // Wide pin window so Hang deadline cannot free slots mid-probe (Expert).
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(
            channel_ids
                .iter()
                .map(|id| hang_llm_binding(id, per_channel))
                .collect(),
        )
        .unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let barrier = Arc::new(Barrier::new(wave));
    let mut joins = Vec::new();
    for (ci, ch) in channel_ids.iter().enumerate() {
        for j in 0..occupied_per_channel {
            let h = handle.clone();
            let b = Arc::clone(&barrier);
            let channel = (*ch).to_string();
            // Even slots: same external string on every Channel (SessionKey
            // isolation). Odd slots: channel-unique sessions.
            let session = if j % 2 == 0 {
                format!("shared-slot-{j}")
            } else {
                format!("uniq-{channel}-j{j}-c{ci}")
            };
            joins.push(std::thread::spawn(move || {
                b.wait();
                submit_ports_on(&h, &channel, Some(&session))
            }));
        }
    }

    let mut admitted = 0usize;
    let mut receivers = Vec::new();
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(receipt) => {
                admitted += 1;
                let _ = receipt;
                receivers.push(recv);
            }
            Err(e) => panic!("multi-channel load admit must succeed, got {e:?}"),
        }
    }
    assert_eq!(admitted, wave, "partial-fill multi-channel wave must admit");
    assert_eq!(started.owner.ledger_len(), wave);
    assert_eq!(started.owner.global_reservations(), wave);
    for ch in channel_ids {
        let id = ChannelId::try_new(ch).unwrap();
        assert_eq!(
            started.owner.channel_reservations(&id),
            occupied_per_channel,
            "channel {ch} occupancy"
        );
    }

    // Headroom remains: duplicate SessionKey on ch-a is SessionAlreadyActive,
    // not CapacityExceeded.
    let (dup_err, dup_recv) = submit_ports_on(&handle, "ch-a", Some("shared-slot-0"));
    assert_eq!(
        dup_err.expect_err("duplicate SessionKey").kind,
        AdmissionErrorKind::SessionAlreadyActive
    );
    assert_rejected_silent(dup_recv);

    // Fill each channel's remaining slot with a unique session.
    for ch in channel_ids {
        let (res, recv) = submit_ports_on(&handle, ch, Some(&format!("fill-{ch}")));
        let receipt = res.expect("fill slot must admit");
        let _ = receipt;
        receivers.push(recv);
        admitted += 1;
    }
    assert_eq!(admitted, global);
    assert_eq!(started.owner.ledger_len(), global);
    for ch in channel_ids {
        assert_eq!(
            started
                .owner
                .channel_reservations(&ChannelId::try_new(ch).unwrap()),
            per_channel
        );
    }

    let (cap_err, cap_recv) = submit_ports_on(&handle, "ch-a", Some("overflow-unique"));
    assert_eq!(
        cap_err.expect_err("channel full").kind,
        AdmissionErrorKind::CapacityExceeded
    );
    assert_rejected_silent(cap_recv);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(10)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(
                report.completions_published as usize, global,
                "every multi-channel admission completes once"
            );
        }
        other => panic!("expected Stopped after multi-channel load, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0);
    assert_eq!(owner.global_reservations(), 0);
    for ch in channel_ids {
        assert_eq!(
            owner.channel_reservations(&ChannelId::try_new(ch).unwrap()),
            0
        );
    }

    for recv in receivers {
        let completion = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
        assert!(matches!(
            completion.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
                | monoloop_contracts::TransactionEndKind::Cancelled
                | monoloop_contracts::TransactionEndKind::Terminated
        ));
    }
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
        owner.wait_stopped(Duration::from_secs(5)).await
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
