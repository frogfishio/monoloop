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

/// D-010 / §22.1: barrier-controlled concurrent submit vs begin_shutdown.
///
/// Each racer ends as either silent reject (`RuntimeShuttingDown`) or fully
/// admitted into the shutdown ledger (completion observed). Stopped implies
/// zero ledger / capacity.
#[test]
fn submit_versus_shutdown_barrier_race_two_outcomes() {
    let started = start_runtime(16, 16);
    let handle = started.handle.clone();
    let n_submitters = 8usize;
    // +1 for the shutdown thread.
    let barrier = Arc::new(Barrier::new(n_submitters + 1));
    let mut joins = Vec::new();
    for i in 0..n_submitters {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        let session = format!("race-shut-{i}");
        joins.push(std::thread::spawn(move || {
            b.wait();
            submit_ports(&h, Some(&session))
        }));
    }

    let owner = started.owner;
    let shut_barrier = Arc::clone(&barrier);
    let shut_join = std::thread::spawn(move || {
        shut_barrier.wait();
        owner.begin_shutdown();
        owner
    });

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
                assert_eq!(
                    e.kind,
                    AdmissionErrorKind::RuntimeShuttingDown,
                    "only RuntimeShuttingDown (or Ok) is legal, got {e:?}"
                );
                rejected += 1;
                assert_rejected_silent(recv);
            }
        }
    }
    assert_eq!(
        admitted + rejected,
        n_submitters,
        "every racer must resolve as Ok(admit) or RuntimeShuttingDown"
    );

    let mut owner = shut_join.join().unwrap();
    // Do not assert mid-Quiescing ledger_len == admitted: Echo may drain before
    // this thread rejoins. The durable proof is Stopped completions == admitted.

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(5)));
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(
                report.completions_published as usize, admitted,
                "every admitted transaction must complete once (no force-remove ghosts)"
            );
        }
        other => panic!("expected Stopped after race, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0, "Stopped ⇒ empty ledger");
    assert_eq!(owner.global_reservations(), 0, "Stopped ⇒ zero capacity");

    for recv in receivers {
        let completion = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
        assert!(matches!(
            completion.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
                | monoloop_contracts::TransactionEndKind::Completed
                | monoloop_contracts::TransactionEndKind::Cancelled
        ));
    }
}

/// D-010: Hang harness pins both outcomes in one interleaving.
///
/// Pre-admit a Hang worker (stays live through Quiescing), barrier-race more
/// submits against `begin_shutdown`, then one post-Quiescing submit that MUST
/// reject — so `admitted >= 1` and `rejected >= 1` are deterministic.
#[test]
fn submit_versus_shutdown_hang_barrier_both_outcomes() {
    let limits = TransactionLimits {
        max_active_transactions: 16,
        max_active_per_channel: 16,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", 16)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let (pre_receipt, pre_recv) = submit(&handle, Some("pre-hang")).expect("pre-admit Hang");
    let _ = pre_receipt;
    // Let Hang register ConnectorOwner under the supervisor.
    std::thread::sleep(Duration::from_millis(30));

    let n_submitters = 6usize;
    let barrier = Arc::new(Barrier::new(n_submitters + 1));
    let mut joins = Vec::new();
    for i in 0..n_submitters {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        let session = format!("hang-race-{i}");
        joins.push(std::thread::spawn(move || {
            b.wait();
            submit_ports(&h, Some(&session))
        }));
    }

    let owner = started.owner;
    let shut_barrier = Arc::clone(&barrier);
    let shut_join = std::thread::spawn(move || {
        shut_barrier.wait();
        owner.begin_shutdown();
        owner
    });

    let mut race_admitted = 0usize;
    let mut race_rejected = 0usize;
    let mut receivers = vec![pre_recv];
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(receipt) => {
                race_admitted += 1;
                let _ = receipt;
                receivers.push(recv);
            }
            Err(e) => {
                assert_eq!(e.kind, AdmissionErrorKind::RuntimeShuttingDown);
                race_rejected += 1;
                assert_rejected_silent(recv);
            }
        }
    }

    let mut owner = shut_join.join().unwrap();
    // Deterministic reject after Quiescing (does not rely on barrier timing).
    let (post_err, post_recv) = submit_ports(&handle, Some("post-quiesce"));
    assert_eq!(
        post_err.expect_err("must reject after Quiescing").kind,
        AdmissionErrorKind::RuntimeShuttingDown
    );
    assert_rejected_silent(post_recv);

    let admitted = race_admitted + 1; // pre-Hang
    let rejected = race_rejected + 1; // post-Quiescing
    assert!(admitted >= 1, "Hang pre-admit guarantees an admit");
    assert!(rejected >= 1, "post-Quiescing submit guarantees a reject");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(5)));
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(
                report.completions_published as usize, admitted,
                "every admitted Hang/race tx must complete once"
            );
        }
        other => panic!("expected Stopped after Hang race, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0);
    assert_eq!(owner.global_reservations(), 0);

    for recv in receivers {
        let completion = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
        assert!(
            matches!(
                completion.end.kind,
                monoloop_contracts::TransactionEndKind::RuntimeShutdown
                    | monoloop_contracts::TransactionEndKind::Cancelled
                    | monoloop_contracts::TransactionEndKind::Terminated
            ),
            "Hang shutdown path should not invent Completed, got {:?}",
            completion.end.kind
        );
    }
}

/// D-040 / §22.1: submit vs begin_shutdown — only reject or fully admit into ledger.
#[test]
fn submit_versus_begin_shutdown_two_outcomes() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let (receipt, recv_ok) = submit(&handle, Some("before")).expect("admit before shutdown");
    assert_eq!(started.owner.ledger_len(), 1);
    assert_eq!(started.owner.global_reservations(), 1);

    let mut owner = started.owner;
    owner.begin_shutdown();
    // Fully admitted: still present in the shutdown ledger until completion.
    assert_eq!(owner.ledger_len(), 1);
    let _ = receipt;

    let (err, recv_rej) = submit_ports(&handle, Some("after"));
    assert_eq!(
        err.expect_err("must reject after Quiescing").kind,
        AdmissionErrorKind::RuntimeShuttingDown
    );
    assert_rejected_silent(recv_rej);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(report.completions_published, 1);
        }
        other => panic!("expected Stopped with the admitted transaction, got {other:?}"),
    }
    let completion = rt
        .block_on(recv_ok.completion.recv())
        .expect("admitted must complete");
    assert!(matches!(
        completion.end.kind,
        monoloop_contracts::TransactionEndKind::RuntimeShutdown
            | monoloop_contracts::TransactionEndKind::Completed
    ));
}

/// D-040 / §22.1: concurrent duplicate SessionKey admits exactly one.
#[test]
fn duplicate_session_race_admits_exactly_one() {
    let started = start_runtime(8, 8);
    let handle = started.handle.clone();
    let n = 8;
    let barrier = Arc::new(Barrier::new(n));
    let mut joins = Vec::new();
    for _ in 0..n {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        joins.push(std::thread::spawn(move || {
            b.wait();
            submit_ports(&h, Some("race-same"))
        }));
    }
    let mut winners = 0usize;
    let mut rejected = 0usize;
    let mut winner_recv = None;
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(_) => {
                winners += 1;
                winner_recv = Some(recv);
            }
            Err(e) => {
                assert_eq!(e.kind, AdmissionErrorKind::SessionAlreadyActive);
                rejected += 1;
                assert_rejected_silent(recv);
            }
        }
    }
    assert_eq!(winners, 1, "exactly one duplicate-session admit");
    assert_eq!(rejected, n - 1);
    assert_eq!(started.owner.ledger_len(), 1);

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
        matches!(outcome, ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == 1),
        "one admission → one completion, got {outcome:?}"
    );
    let _ = rt.block_on(winner_recv.expect("winner").completion.recv());
}

/// Race/load expansion: barrier-concurrent Cancel on N distinct Hang sessions.
/// Every terminate is Accepted (ledger non-terminal); every admission completes
/// once as Cancelled; shutdown publishes exactly N completions.
#[test]
fn concurrent_hang_terminate_storm_all_cancelled() {
    let n = 8usize;
    let limits = TransactionLimits {
        max_active_transactions: n,
        max_active_per_channel: n,
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", n)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let mut receivers = Vec::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let (receipt, recv) =
            submit(&handle, Some(&format!("term-storm-{i}"))).expect("admit Hang");
        ids.push(receipt.transaction_id);
        receivers.push(recv);
    }
    assert_eq!(started.owner.ledger_len(), n);
    // Per-class Hang-ready: wait for N live ConnectorOwners (D-051
    // register-before-I/O). Aggregate owned_task_count is not per-admit.
    let ready = Instant::now();
    while started.owner.live_connector_owners() < n as u32 {
        assert!(
            ready.elapsed() < Duration::from_secs(2),
            "Hang Cancel storm: expected live_connector_owners>={n} before barrier, got {}",
            started.owner.live_connector_owners()
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let barrier = Arc::new(Barrier::new(n));
    let mut joins = Vec::new();
    for id in ids {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        joins.push(std::thread::spawn(move || {
            b.wait();
            h.terminate(
                TransactionSelector::Transaction(id),
                TerminationMode::Cancel {
                    reason: CancellationReason {
                        code: CancellationReasonCode::CallerRequested,
                        detail: None,
                    },
                },
            )
        }));
    }
    let mut accepted = 0usize;
    for j in joins {
        let disp = j.join().unwrap();
        assert_eq!(
            disp,
            TerminationDisposition::Accepted,
            "distinct Hang cancel under headroom must Accepted, got {disp:?}"
        );
        accepted += 1;
    }
    assert_eq!(accepted, n);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for recv in receivers {
        let completion = rt
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(3), recv.completion.recv()).await
            })
            .expect("Hang cancel must complete within 3s")
            .expect("completion channel closed");
        assert_eq!(
            completion.end.kind,
            TransactionEndKind::Cancelled,
            "expected Cancelled after concurrent Hang terminate storm"
        );
    }

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(
            outcome,
            ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == n as u64
        ),
        "exactly one completion per Hang admission, got {outcome:?}"
    );
}

/// Race/load expansion: barrier-concurrent ForceTerminate on N distinct Hang
/// sessions — twin of the Cancel storm (Accepted → Terminated → N completions).
#[test]
fn concurrent_hang_force_terminate_storm_all_terminated() {
    let n = 8usize;
    let limits = TransactionLimits {
        max_active_transactions: n,
        max_active_per_channel: n,
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", n)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let mut receivers = Vec::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let (receipt, recv) =
            submit(&handle, Some(&format!("force-storm-{i}"))).expect("admit Hang");
        ids.push(receipt.transaction_id);
        receivers.push(recv);
    }
    assert_eq!(started.owner.ledger_len(), n);
    let ready = Instant::now();
    while started.owner.live_connector_owners() < n as u32 {
        assert!(
            ready.elapsed() < Duration::from_secs(2),
            "Hang ForceTerminate storm: expected live_connector_owners>={n} before barrier, got {}",
            started.owner.live_connector_owners()
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let barrier = Arc::new(Barrier::new(n));
    let mut joins = Vec::new();
    for id in ids {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        joins.push(std::thread::spawn(move || {
            b.wait();
            h.terminate(
                TransactionSelector::Transaction(id),
                TerminationMode::ForceTerminate {
                    reason: TerminationReason {
                        code: TerminationReasonCode::CallerRequested,
                        detail: None,
                    },
                },
            )
        }));
    }
    let mut accepted = 0usize;
    for j in joins {
        let disp = j.join().unwrap();
        assert_eq!(
            disp,
            TerminationDisposition::Accepted,
            "distinct Hang ForceTerminate under headroom must Accepted, got {disp:?}"
        );
        accepted += 1;
    }
    assert_eq!(accepted, n);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for recv in receivers {
        let completion = rt
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(3), recv.completion.recv()).await
            })
            .expect("Hang ForceTerminate must complete within 3s")
            .expect("completion channel closed");
        assert_eq!(
            completion.end.kind,
            TransactionEndKind::Terminated,
            "expected Terminated after concurrent Hang ForceTerminate storm"
        );
    }

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(
            outcome,
            ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == n as u64
        ),
        "exactly one completion per Hang admission, got {outcome:?}"
    );
}

/// Race/load expansion: barrier-concurrent Cancel vs ForceTerminate on **one**
/// Hang admission — §22.2 terminal selection under true concurrency.
/// Exactly one completion in `{Cancelled, Terminated}` (Force may upgrade Cancel
/// before Seal, or Cancel may Seal first). Dispositions only
/// `{Accepted, AlreadyTerminal}` with ≥1 Accepted.
#[test]
fn concurrent_hang_cancel_versus_force_terminate_one_terminal() {
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

    let (receipt, recv) = submit(&handle, Some("cancel-force-race")).expect("admit Hang");
    let id = receipt.transaction_id;
    assert_eq!(started.owner.ledger_len(), 1);
    let ready = Instant::now();
    while started.owner.live_connector_owners() < 1 {
        assert!(
            ready.elapsed() < Duration::from_secs(2),
            "Hang Cancel×Force race: expected live_connector_owners>=1 before barrier, got {}",
            started.owner.live_connector_owners()
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let barrier = Arc::new(Barrier::new(2));
    let h_cancel = handle.clone();
    let b_cancel = Arc::clone(&barrier);
    let cancel_join = std::thread::spawn(move || {
        b_cancel.wait();
        h_cancel.terminate(
            TransactionSelector::Transaction(id),
            TerminationMode::Cancel {
                reason: CancellationReason {
                    code: CancellationReasonCode::CallerRequested,
                    detail: None,
                },
            },
        )
    });
    let h_force = handle.clone();
    let b_force = Arc::clone(&barrier);
    let force_join = std::thread::spawn(move || {
        b_force.wait();
        h_force.terminate(
            TransactionSelector::Transaction(id),
            TerminationMode::ForceTerminate {
                reason: TerminationReason {
                    code: TerminationReasonCode::CallerRequested,
                    detail: None,
                },
            },
        )
    });

    let cancel_disp = cancel_join.join().unwrap();
    let force_disp = force_join.join().unwrap();
    for (label, disp) in [("Cancel", cancel_disp), ("ForceTerminate", force_disp)] {
        assert!(
            matches!(
                disp,
                TerminationDisposition::Accepted | TerminationDisposition::AlreadyTerminal
            ),
            "{label} disposition must be Accepted|AlreadyTerminal, got {disp:?}"
        );
    }
    assert!(
        matches!(cancel_disp, TerminationDisposition::Accepted)
            || matches!(force_disp, TerminationDisposition::Accepted),
        "at least one terminate must Accepted (got Cancel={cancel_disp:?}, Force={force_disp:?})"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(3), recv.completion.recv()).await
        })
        .expect("Hang Cancel×Force must complete within 3s")
        .expect("completion channel closed");
    assert!(
        matches!(
            completion.end.kind,
            TransactionEndKind::Cancelled | TransactionEndKind::Terminated
        ),
        "exactly one documented terminal in {{Cancelled, Terminated}}, got {:?}",
        completion.end.kind
    );

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(
            outcome,
            ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == 1
        ),
        "exactly one completion for the Hang admission, got {outcome:?}"
    );
}
