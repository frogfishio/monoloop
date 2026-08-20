//! WP-12: full-system hardening — capacity, load, races, shutdown, isolation.
//!
//! Deterministic FakeConnector path only (no live providers).

use monoloop_connector::FakeConnectorFactory;
use monoloop_contracts::{
    user_text_input, AdmissionErrorKind, CancellationReason, CancellationReasonCode,
    ChannelCapabilities, ChannelDefaults, ChannelId, ChannelKind, ChannelLimits,
    ContinuationPolicy, DialectDescriptor, ExchangeMode, FnCompletionCallback, FnEventSink,
    InvocationConfig, McpConfigurationCapability, McpReachability, SessionId, SessionMode,
    TerminationMode, ToolExecutionMode, TransactionEnd, TransactionEndKind, TransactionEvent,
    TransactionEventPayload, TransactionLimits, TransactionRequest, TransactionRuntime,
    TransactionSelector,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, DefaultTransactionRuntime, HostToolRegistry, RuntimeBootstrap,
    RuntimeConfig, TestTextEncoder,
};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

fn caps() -> ChannelCapabilities {
    let d = DialectDescriptor::test_raw();
    ChannelCapabilities {
        session_mode: SessionMode::Stateless,
        mcp_configuration: McpConfigurationCapability::None,
        mcp_reachability: McpReachability::None,
        exchange_mode: ExchangeMode::RequestResponse,
        continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
        supports_distinct_session_concurrency: true,
        input_dialect: d.clone(),
        output_dialect: d,
        option_policy: monoloop_contracts::OptionPolicy::direct_llm(),
    }
}

fn llm_binding(id: &str, channel_max: usize) -> ChannelBinding {
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
        capabilities: caps(),
        limits: ChannelLimits {
            max_active_transactions: channel_max,
            max_distinct_sessions: channel_max,
            max_encoded_exchange_bytes: 4 * 1024 * 1024,
        },
    }
}

async fn start_with_limits(
    channels: Vec<ChannelBinding>,
    limits: TransactionLimits,
) -> Arc<DefaultTransactionRuntime> {
    DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            transaction_limits: limits,
            ..Default::default()
        },
        channels: ChannelRegistry::build(channels).unwrap(),
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap()
}

fn limits_cap(global: usize, per_channel: usize) -> TransactionLimits {
    TransactionLimits {
        max_active_transactions: global,
        max_active_per_channel: per_channel,
        ..Default::default()
    }
}

fn limits_cap_events(
    global: usize,
    per_channel: usize,
    max_event_queue: usize,
) -> TransactionLimits {
    TransactionLimits {
        max_active_transactions: global,
        max_active_per_channel: per_channel,
        max_event_queue,
        ..Default::default()
    }
}

fn blocked_sink(gate: Arc<Notify>) -> Arc<dyn monoloop_contracts::TransactionEventSink> {
    Arc::new(FnEventSink(move |_e| {
        let gate = Arc::clone(&gate);
        Box::pin(async move {
            gate.notified().await;
            Ok(())
        }) as monoloop_contracts::EventDelivery
    }))
}

fn counting_completion(
    ends: Arc<AtomicUsize>,
    done: Arc<Notify>,
) -> Box<dyn monoloop_contracts::CompletionCallback> {
    Box::new(FnCompletionCallback(move |_end: TransactionEnd| {
        let ends = Arc::clone(&ends);
        let done = Arc::clone(&done);
        Box::pin(async move {
            ends.fetch_add(1, Ordering::SeqCst);
            done.notify_waiters();
            Ok(())
        }) as monoloop_contracts::CompletionDelivery
    }))
}

fn free_request(
    channel: &str,
    session: Option<SessionId>,
    events: Arc<dyn monoloop_contracts::TransactionEventSink>,
    completion: Box<dyn monoloop_contracts::CompletionCallback>,
) -> TransactionRequest {
    TransactionRequest {
        channel_id: ChannelId::try_new(channel).unwrap(),
        session_id: session,
        input: user_text_input("harden").unwrap(),
        session_config: None,
        invocation_config: InvocationConfig {
            deadline: Some(Duration::from_secs(30)),
            continuation_policy: ContinuationPolicy::CallerControlled,
            ..Default::default()
        },
        tools: vec![],
        events,
        completion,
    }
}

/// Global max N admits N blocked; N+1 fails; release drains capacity for a later admit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_plus_one_global() {
    let max = 3usize;
    let rt = start_with_limits(vec![llm_binding("llm", max)], limits_cap(max, max)).await;

    let gate = Arc::new(Notify::new());
    let ends = Arc::new(AtomicUsize::new(0));
    let mut dones = Vec::new();

    for i in 0..max {
        let done = Arc::new(Notify::new());
        dones.push(Arc::clone(&done));
        let receipt = TransactionRuntime::submit(
            rt.as_ref(),
            free_request(
                "llm",
                Some(SessionId::try_new(format!("g-{i}")).unwrap()),
                blocked_sink(Arc::clone(&gate)),
                counting_completion(Arc::clone(&ends), done),
            ),
        )
        .unwrap();
        assert!(receipt.session_id.is_some());
    }
    assert_eq!(rt.capacity().global_active(), max);

    let overflow = TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            Some(SessionId::try_new("g-overflow").unwrap()),
            blocked_sink(Arc::clone(&gate)),
            counting_completion(Arc::clone(&ends), Arc::new(Notify::new())),
        ),
    )
    .unwrap_err();
    assert_eq!(overflow.kind, AdmissionErrorKind::CapacityExceeded);

    gate.notify_waiters();
    for d in &dones {
        let _ = tokio::time::timeout(Duration::from_secs(5), d.notified()).await;
    }
    // Wait for capacity release after actors finish.
    for _ in 0..50 {
        if rt.capacity().global_active() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(rt.capacity().global_active(), 0);
    assert_eq!(rt.active_count(), 0);

    let done = Arc::new(Notify::new());
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            Some(SessionId::try_new("g-after").unwrap()),
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            counting_completion(Arc::clone(&ends), Arc::clone(&done)),
        ),
    )
    .expect("capacity must free after completion");
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("post-release transaction");

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(2)).await;
    assert_eq!(rt.active_count(), 0);
    assert_eq!(rt.capacity().global_active(), 0);
}

/// Per-channel max is tighter than global: channel overflow rejects while other channel works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_plus_one_per_channel() {
    let rt = start_with_limits(
        vec![llm_binding("a", 2), llm_binding("b", 2)],
        limits_cap(8, 2),
    )
    .await;

    let gate = Arc::new(Notify::new());
    let ends = Arc::new(AtomicUsize::new(0));

    for i in 0..2 {
        TransactionRuntime::submit(
            rt.as_ref(),
            free_request(
                "a",
                Some(SessionId::try_new(format!("a-{i}")).unwrap()),
                blocked_sink(Arc::clone(&gate)),
                counting_completion(Arc::clone(&ends), Arc::new(Notify::new())),
            ),
        )
        .unwrap();
    }
    let err = TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "a",
            Some(SessionId::try_new("a-overflow").unwrap()),
            blocked_sink(Arc::clone(&gate)),
            counting_completion(Arc::clone(&ends), Arc::new(Notify::new())),
        ),
    )
    .unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::CapacityExceeded);

    // Channel b still admits.
    let done_b = Arc::new(Notify::new());
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "b",
            Some(SessionId::try_new("b-0").unwrap()),
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            counting_completion(Arc::clone(&ends), Arc::clone(&done_b)),
        ),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), done_b.notified())
        .await
        .expect("channel b free path");

    gate.notify_waiters();
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(2)).await;
    assert_eq!(rt.active_count(), 0);
}

/// Thousands of completed fake transactions within configured limits (wave concurrency).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thousands_of_fake_transactions_within_limits() {
    let wave = 32usize;
    let total = 2_048usize;
    let rt = start_with_limits(vec![llm_binding("llm", wave)], limits_cap(wave, wave)).await;

    let completed = Arc::new(AtomicUsize::new(0));
    let mut i = 0usize;
    while i < total {
        let batch = (total - i).min(wave);
        let barrier = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        for j in 0..batch {
            let completed = Arc::clone(&completed);
            let barrier = Arc::clone(&barrier);
            let notify = Arc::clone(&notify);
            let idx = i + j;
            let sink: Arc<dyn monoloop_contracts::TransactionEventSink> =
                Arc::new(FnEventSink(|_| {
                    Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
                }));
            let completion: Box<dyn monoloop_contracts::CompletionCallback> =
                Box::new(FnCompletionCallback(move |_e| {
                    let completed = Arc::clone(&completed);
                    let barrier = Arc::clone(&barrier);
                    let notify = Arc::clone(&notify);
                    Box::pin(async move {
                        completed.fetch_add(1, Ordering::SeqCst);
                        if barrier.fetch_add(1, Ordering::SeqCst) + 1 == batch {
                            notify.notify_waiters();
                        }
                        Ok(())
                    }) as monoloop_contracts::CompletionDelivery
                }));
            TransactionRuntime::submit(
                rt.as_ref(),
                free_request(
                    "llm",
                    Some(SessionId::try_new(format!("load-{idx}")).unwrap()),
                    sink,
                    completion,
                ),
            )
            .unwrap_or_else(|e| panic!("admit {idx}: {e:?}"));
        }
        tokio::time::timeout(Duration::from_secs(30), notify.notified())
            .await
            .unwrap_or_else(|_| panic!("wave starting at {i} timed out"));
        // Ensure transaction and callback capacity released before next wave.
        for _ in 0..200 {
            if rt.capacity().global_active() == 0 && rt.callback_available_permits() >= wave {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        i += batch;
    }

    assert_eq!(completed.load(Ordering::SeqCst), total);
    assert_eq!(rt.active_count(), 0);
    assert_eq!(rt.capacity().global_active(), 0);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(2)).await;
}

/// Identical session strings on different Channels are independent SessionKeys.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_session_strings_different_channels() {
    let rt = start_with_limits(
        vec![llm_binding("ch-a", 8), llm_binding("ch-b", 8)],
        limits_cap(16, 8),
    )
    .await;
    let sid = SessionId::try_new("shared-external-string").unwrap();
    let ends = Arc::new(AtomicUsize::new(0));
    let both = Arc::new(Notify::new());
    let free_sink: Arc<dyn monoloop_contracts::TransactionEventSink> =
        Arc::new(FnEventSink(|_| {
            Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
        }));

    let mk_cb = |ends: Arc<AtomicUsize>, both: Arc<Notify>| {
        Box::new(FnCompletionCallback(move |_e: TransactionEnd| {
            let ends = Arc::clone(&ends);
            let both = Arc::clone(&both);
            Box::pin(async move {
                if ends.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                    both.notify_waiters();
                }
                Ok(())
            }) as monoloop_contracts::CompletionDelivery
        })) as Box<dyn monoloop_contracts::CompletionCallback>
    };

    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "ch-a",
            Some(sid.clone()),
            Arc::clone(&free_sink),
            mk_cb(Arc::clone(&ends), Arc::clone(&both)),
        ),
    )
    .unwrap();
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "ch-b",
            Some(sid),
            free_sink,
            mk_cb(Arc::clone(&ends), Arc::clone(&both)),
        ),
    )
    .unwrap();

    // Poll so a lost Notify cannot hang if both finish before we wait.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if ends.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::select! {
                _ = both.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
    })
    .await
    .expect("both channels");
    assert_eq!(ends.load(Ordering::SeqCst), 2);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// Slow sink on one transaction does not block completion of an independent peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriber_backpressure_isolated() {
    // Keep event queue small so backpressure is visible on the slow txn only.
    let rt = start_with_limits(vec![llm_binding("llm", 8)], limits_cap_events(8, 8, 4)).await;

    let slow_gate = Arc::new(Notify::new());
    let slow_ends = Arc::new(AtomicUsize::new(0));
    let slow_done = Arc::new(Notify::new());
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            Some(SessionId::try_new("slow").unwrap()),
            blocked_sink(Arc::clone(&slow_gate)),
            counting_completion(Arc::clone(&slow_ends), Arc::clone(&slow_done)),
        ),
    )
    .unwrap();

    // Give the slow sink a chance to block delivery before admitting the peer.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let fast_ends = Arc::new(AtomicUsize::new(0));
    let fast_done = Arc::new(Notify::new());
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            Some(SessionId::try_new("fast").unwrap()),
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            counting_completion(Arc::clone(&fast_ends), Arc::clone(&fast_done)),
        ),
    )
    .unwrap();

    tokio::time::timeout(Duration::from_secs(5), fast_done.notified())
        .await
        .expect("fast transaction must complete despite slow peer sink");
    assert_eq!(fast_ends.load(Ordering::SeqCst), 1);
    assert_eq!(slow_ends.load(Ordering::SeqCst), 0);

    slow_gate.notify_waiters();
    // Finalize waits up to 5s for terminal delivery ack when the sink was stuck.
    tokio::time::timeout(Duration::from_secs(15), slow_done.notified())
        .await
        .expect("slow transaction completes after sink unblocks");
    assert_eq!(slow_ends.load(Ordering::SeqCst), 1);

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(2)).await;
}

/// Zero events after Ended; callback exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_events_after_ended_and_single_callback() {
    let rt = start_with_limits(vec![llm_binding("llm", 4)], limits_cap(4, 4)).await;

    let events = Arc::new(Mutex::new(Vec::<TransactionEvent>::new()));
    let ends = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(Notify::new());
    let events_s = Arc::clone(&events);
    let sink: Arc<dyn monoloop_contracts::TransactionEventSink> = Arc::new(FnEventSink(move |e| {
        let events_s = Arc::clone(&events_s);
        Box::pin(async move {
            events_s.lock().unwrap().push(e);
            Ok(())
        }) as monoloop_contracts::EventDelivery
    }));

    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            None,
            sink,
            counting_completion(Arc::clone(&ends), Arc::clone(&done)),
        ),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .unwrap();
    // Allow stray late deliveries (must not happen).
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(ends.load(Ordering::SeqCst), 1);

    {
        let evs = events.lock().unwrap();
        let ended_idx = evs
            .iter()
            .position(|e| matches!(e.payload, TransactionEventPayload::Ended(_)))
            .expect("Ended event");
        assert!(
            evs[ended_idx + 1..]
                .iter()
                .all(|e| !matches!(e.payload, TransactionEventPayload::Ended(_))),
            "no duplicate Ended"
        );
        assert!(
            evs[ended_idx + 1..].is_empty(),
            "no events after Ended: {:?}",
            &evs[ended_idx + 1..]
        );
        // Monotonic sequences.
        let seqs: Vec<_> = evs.iter().map(|e| e.sequence).collect();
        let mut sorted = seqs.clone();
        sorted.sort_unstable();
        assert_eq!(seqs, sorted);
        assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    }

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// Completion vs cancel race: exactly one terminal callback.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_versus_cancel_single_terminal() {
    let rt = start_with_limits(vec![llm_binding("llm", 4)], limits_cap(4, 4)).await;

    let ends = Arc::new(Mutex::new(Vec::<TransactionEndKind>::new()));
    let done = Arc::new(Notify::new());
    let ends_s = Arc::clone(&ends);
    let done_s = Arc::clone(&done);

    // Free sink: cancel races natural completion; either terminal is valid, once only.
    let receipt = TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            Some(SessionId::try_new("race-cancel").unwrap()),
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            Box::new(FnCompletionCallback(move |end: TransactionEnd| {
                let ends_s = Arc::clone(&ends_s);
                let done_s = Arc::clone(&done_s);
                Box::pin(async move {
                    ends_s.lock().unwrap().push(end.kind);
                    done_s.notify_waiters();
                    Ok(())
                }) as monoloop_contracts::CompletionDelivery
            })),
        ),
    )
    .unwrap();

    let _ = TransactionRuntime::terminate(
        rt.as_ref(),
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    tokio::time::timeout(Duration::from_secs(10), done.notified())
        .await
        .expect("one callback");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let kinds = ends.lock().unwrap().clone();
    assert_eq!(kinds.len(), 1, "exactly one terminal callback: {kinds:?}");
    assert!(
        matches!(
            kinds[0],
            TransactionEndKind::Completed
                | TransactionEndKind::Cancelled
                | TransactionEndKind::Terminated
                | TransactionEndKind::EventDeliveryFailed
        ),
        "terminal kind: {:?}",
        kinds[0]
    );

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// Short deadline produces a terminal outcome while the sink is blocked (outer race).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn short_deadline_terminal() {
    let rt = start_with_limits(vec![llm_binding("llm", 4)], limits_cap(4, 4)).await;

    let ends = Arc::new(Mutex::new(Vec::<TransactionEndKind>::new()));
    let done = Arc::new(Notify::new());
    let ends_s = Arc::clone(&ends);
    let done_s = Arc::clone(&done);
    // Block events so exchange emits stall; actor outer deadline still fires.
    let gate = Arc::new(Notify::new());

    let mut req = free_request(
        "llm",
        Some(SessionId::try_new("deadline").unwrap()),
        blocked_sink(Arc::clone(&gate)),
        Box::new(FnCompletionCallback(move |end: TransactionEnd| {
            let ends_s = Arc::clone(&ends_s);
            let done_s = Arc::clone(&done_s);
            Box::pin(async move {
                ends_s.lock().unwrap().push(end.kind);
                done_s.notify_waiters();
                Ok(())
            }) as monoloop_contracts::CompletionDelivery
        })),
    );
    req.invocation_config.deadline = Some(Duration::from_millis(80));

    TransactionRuntime::submit(rt.as_ref(), req).unwrap();
    // Outer deadline (~80ms) + terminal delivery ack budget (~5s) when sink blocked.
    tokio::time::timeout(Duration::from_secs(15), done.notified())
        .await
        .expect("deadline path terminal");
    gate.notify_waiters();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let kinds = ends.lock().unwrap().clone();
    assert_eq!(kinds.len(), 1);
    assert!(
        matches!(
            kinds[0],
            TransactionEndKind::DeadlineExceeded
                | TransactionEndKind::Cancelled
                | TransactionEndKind::Completed
                | TransactionEndKind::Terminated
                | TransactionEndKind::EventDeliveryFailed
                | TransactionEndKind::LimitExceeded
        ),
        "got {:?}",
        kinds[0]
    );

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(2)).await;
    assert_eq!(rt.capacity().global_active(), 0);
}

/// Shutdown with active blocked transactions finalizes and leaves zero owned state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_with_active_zero_owned() {
    let rt = start_with_limits(vec![llm_binding("llm", 8)], limits_cap(8, 8)).await;

    let gate = Arc::new(Notify::new());
    let callbacks = Arc::new(AtomicU64::new(0));
    for i in 0..4 {
        let callbacks = Arc::clone(&callbacks);
        TransactionRuntime::submit(
            rt.as_ref(),
            free_request(
                "llm",
                Some(SessionId::try_new(format!("sd-{i}")).unwrap()),
                blocked_sink(Arc::clone(&gate)),
                Box::new(FnCompletionCallback(move |_e| {
                    let callbacks = Arc::clone(&callbacks);
                    Box::pin(async move {
                        callbacks.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }) as monoloop_contracts::CompletionDelivery
                })),
            ),
        )
        .unwrap();
    }
    // Ensure actors are in-flight and holding capacity before shutdown.
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(rt.active_count() >= 1 || rt.capacity().global_active() >= 1);

    let disp = TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(3)).await;
    gate.notify_waiters();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let finalized = disp.normally_finalized + disp.supervisor_finalized;
    let cb = callbacks.load(Ordering::SeqCst);
    assert!(
        finalized >= 1 || cb >= 1,
        "shutdown disposition={disp:?} callbacks={cb}"
    );
    assert_eq!(rt.active_count(), 0);
    assert_eq!(
        rt.capacity().global_active(),
        0,
        "shutdown must release all capacity permits"
    );

    // No further admits after stop.
    let err = TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            None,
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            counting_completion(Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new())),
        ),
    )
    .unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::RuntimeShuttingDown);
}

/// Tool capacity queue + concurrent limits reject excess (unit of capacity managers).
#[test]
fn tool_capacity_plus_one() {
    use monoloop_contracts::ToolId;
    use monoloop_loop::{SharedToolCapacity, TransactionToolCapacity};

    let shared = SharedToolCapacity::new(1);
    let cap = TransactionToolCapacity::new(Arc::clone(&shared), 1, 1);
    let tool = ToolId::try_new("t").unwrap();
    cap.configure_tool(tool.clone(), 1);

    assert!(cap.try_enqueue());
    assert!(!cap.try_enqueue(), "queued capacity plus one must fail");
    let permit = cap.try_acquire(&tool).expect("one concurrent");
    // Queue was consumed by acquire; second concurrent start needs another enqueue.
    assert!(cap.try_enqueue());
    assert!(
        cap.try_acquire(&tool).is_none(),
        "concurrent capacity plus one must fail"
    );
    cap.dequeue(); // release failed start's queue reservation
    drop(permit);
    assert!(cap.try_enqueue());
    let p2 = cap.try_acquire(&tool).expect("slot free after release");
    drop(p2);
}

/// D-009: actor does not run (no callback) when start gate is dropped before install start signal.
/// Covered indirectly: after shutdown, capacity is zero and submits reject.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submit_after_shutdown_rejects_without_active_leak() {
    let rt = start_with_limits(vec![llm_binding("llm", 4)], limits_cap(4, 4)).await;
    let done = Arc::new(Notify::new());
    let ends = Arc::new(AtomicUsize::new(0));
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            None,
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            counting_completion(Arc::clone(&ends), Arc::clone(&done)),
        ),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .ok();
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(2)).await;
    assert_eq!(rt.active_count(), 0);
    assert_eq!(rt.capacity().global_active(), 0);
    let err = TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            None,
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            counting_completion(Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new())),
        ),
    )
    .unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::RuntimeShuttingDown);
}

/// D-023: unknown invocation extension rejected when Channel allowlist is empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_extension_rejected_at_admission() {
    use monoloop_contracts::{ExtensionKey, VersionedExtension};

    let rt = start_with_limits(vec![llm_binding("llm", 4)], limits_cap(4, 4)).await;
    let mut req = free_request(
        "llm",
        None,
        Arc::new(FnEventSink(|_| {
            Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
        })),
        counting_completion(Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new())),
    );
    let key = ExtensionKey::try_new("ns.not-allowed", 64).unwrap();
    req.invocation_config.extensions.insert(
        key,
        VersionedExtension {
            version: 1,
            value: serde_json::json!({"v": true}),
        },
    );
    let err = TransactionRuntime::submit(rt.as_ref(), req).unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::InvalidConfiguration);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// D-023: DirectLlm and ExternalAgent declare distinct option matrices.
#[test]
fn channel_option_policy_matrices_differ() {
    use monoloop_contracts::ConfigOption;
    let direct = monoloop_contracts::OptionPolicy::direct_llm();
    let external = monoloop_contracts::OptionPolicy::external_agent();
    assert!(direct
        .supported_invocation
        .contains(&ConfigOption::Temperature));
    assert!(!external
        .supported_invocation
        .contains(&ConfigOption::Temperature));
    assert!(external
        .supported_invocation
        .contains(&ConfigOption::ContinuationPolicy));
}

/// D-023: allowed openai.seed extension admits; temperature rejected on external-agent policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allowed_extension_admits_and_external_rejects_temperature() {
    use monoloop_contracts::{ExtensionKey, VersionedExtension};

    let mut binding = llm_binding("llm", 4);
    let key = ExtensionKey::try_new("openai.seed", 64).unwrap();
    binding.capabilities.option_policy = binding
        .capabilities
        .option_policy
        .with_extension_keys([key.clone()]);
    let rt = start_with_limits(vec![binding], limits_cap(4, 4)).await;
    let mut req = free_request(
        "llm",
        None,
        Arc::new(FnEventSink(|_| {
            Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
        })),
        counting_completion(Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new())),
    );
    req.invocation_config.extensions.insert(
        key,
        VersionedExtension {
            version: 1,
            value: serde_json::json!(7),
        },
    );
    TransactionRuntime::submit(rt.as_ref(), req).expect("allowed extension admits");
    for _ in 0..50 {
        if rt.active_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // External-agent option matrix rejects Temperature (policy-only check on a
    // DirectLlm binding so we do not need a SessionAdapter for this admission gate).
    let mut strict = llm_binding("strict", 4);
    strict.capabilities.option_policy = monoloop_contracts::OptionPolicy::external_agent();
    let rt2 = start_with_limits(vec![strict], limits_cap(4, 4)).await;
    let mut bad = free_request(
        "strict",
        None,
        Arc::new(FnEventSink(|_| {
            Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
        })),
        counting_completion(Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new())),
    );
    bad.invocation_config.temperature = Some(0.9);
    let err = TransactionRuntime::submit(rt2.as_ref(), bad).unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::InvalidConfiguration);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    TransactionRuntime::shutdown(rt2.as_ref(), Duration::from_secs(1)).await;
}

/// D-024: Abortable tool cannot register a handler that refuses abort support.
#[test]
fn abortable_requires_supports_abort_handler() {
    use monoloop_contracts::{
        JsonSchema, ToolCall, ToolCallContext, ToolCompletion, ToolExecutionClass, ToolId,
        ToolLimits, ToolName, ToolOutputContract, ToolSpec, ToolStartError, ToolSuccessContract,
    };
    use monoloop_loop::{RegisteredTool, ToolHandler};

    struct Unstoppable;
    impl ToolHandler for Unstoppable {
        fn start(
            &self,
            _call: ToolCall,
            _ctx: ToolCallContext,
        ) -> Result<monoloop_loop::LinkedToolExecutionHandle, ToolStartError> {
            Err(ToolStartError::Rejected("unstoppable"))
        }
        fn supports_abort(&self) -> bool {
            false
        }
    }

    let schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "additionalProperties": false
    }))
    .unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("u").unwrap(),
        ToolName::try_new("u").unwrap(),
        "unstoppable",
        schema.clone(),
        ToolOutputContract {
            success: ToolSuccessContract::json(schema),
            error_data_schema: None,
        },
        ToolLimits::default(),
        ToolExecutionClass::AbortableAtYield {
            grace: std::time::Duration::from_secs(1),
        },
    )
    .unwrap();
    let err = RegisteredTool::try_new(spec, Arc::new(Unstoppable)).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("supports_abort") || msg.contains("Abortable"),
        "expected policy mismatch, got {msg}"
    );
    let _ = ToolCompletion::Succeeded(monoloop_contracts::CanonicalToolOutput::Json(
        serde_json::json!({}),
    ));
}

/// D-015: max_tools_per_transaction plus-one rejects at admission.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tools_per_transaction_plus_one_rejected() {
    use monoloop_contracts::{
        JsonSchema, ToolExecutionClass, ToolId, ToolLimits, ToolName, ToolOutputContract, ToolSpec,
        ToolSuccessContract,
    };
    use monoloop_loop::{HostToolRegistry, ImmediateToolHandler, RegisteredTool};

    let schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    }))
    .unwrap();
    let make = |id: &str| {
        RegisteredTool::new(
            ToolSpec::try_new(
                ToolId::try_new(id).unwrap(),
                ToolName::try_new(id).unwrap(),
                "t",
                schema.clone(),
                ToolOutputContract {
                    success: ToolSuccessContract::json(schema.clone()),
                    error_data_schema: None,
                },
                ToolLimits::default(),
                ToolExecutionClass::CooperativeInProcess {
                    grace: std::time::Duration::from_millis(50),
                },
            )
            .unwrap(),
            Arc::new(ImmediateToolHandler::new(|_, _| {
                Ok(monoloop_contracts::ToolCompletion::Succeeded(
                    monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
                ))
            })) as Arc<dyn monoloop_loop::ToolHandler>,
        )
    };
    let tools = HostToolRegistry::build(vec![make("t1"), make("t2"), make("t3")]).unwrap();
    let rt = monoloop_loop::DefaultTransactionRuntime::start(monoloop_loop::RuntimeBootstrap {
        config: monoloop_loop::RuntimeConfig {
            enable_mcp_listener: false,
            transaction_limits: TransactionLimits {
                max_tools_per_transaction: 2,
                max_active_transactions: 4,
                max_active_per_channel: 4,
                ..Default::default()
            },
            ..Default::default()
        },
        channels: monoloop_loop::ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools,
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap();
    let mut req = free_request(
        "llm",
        None,
        blocked_sink(Arc::new(Notify::new())),
        counting_completion(Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new())),
    );
    req.tools = vec![
        ToolId::try_new("t1").unwrap(),
        ToolId::try_new("t2").unwrap(),
        ToolId::try_new("t3").unwrap(),
    ];
    let err = TransactionRuntime::submit(rt.as_ref(), req).unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::InvalidInput);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// D-015: max_messages plus-one rejects at admission.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_messages_plus_one_rejected() {
    use monoloop_contracts::{CanonicalInput, CanonicalMessage, InputLimits, TextPart};

    let rt = start_with_limits(
        vec![llm_binding("llm", 4)],
        TransactionLimits {
            max_messages: 1,
            max_active_transactions: 4,
            max_active_per_channel: 4,
            ..Default::default()
        },
    )
    .await;
    let mut req = free_request(
        "llm",
        None,
        blocked_sink(Arc::new(Notify::new())),
        counting_completion(Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new())),
    );
    // Build two messages under roomy InputLimits; admission enforces TransactionLimits.
    let roomy = InputLimits {
        max_messages: 16,
        ..Default::default()
    };
    req.input = CanonicalInput::try_new(
        vec![
            CanonicalMessage::User {
                content: vec![TextPart::try_new("one", roomy.max_text_part_bytes).unwrap()],
                name: None,
            },
            CanonicalMessage::User {
                content: vec![TextPart::try_new("two", roomy.max_text_part_bytes).unwrap()],
                name: None,
            },
        ],
        &roomy,
    )
    .unwrap();
    let err = TransactionRuntime::submit(rt.as_ref(), req).unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::InvalidInput);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// D-015: max_input_bytes plus-one rejects at admission.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_input_bytes_plus_one_rejected() {
    use monoloop_contracts::{user_text_input, InputLimits};

    let rt = start_with_limits(
        vec![llm_binding("llm", 4)],
        TransactionLimits {
            max_input_bytes: 8,
            max_active_transactions: 4,
            max_active_per_channel: 4,
            ..Default::default()
        },
    )
    .await;
    let mut req = free_request(
        "llm",
        None,
        blocked_sink(Arc::new(Notify::new())),
        counting_completion(Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new())),
    );
    // Payload larger than 8 bytes once serialized/estimated.
    req.input = user_text_input("0123456789abcdef").unwrap();
    assert!(req.input.messages().len() == 1);
    let _ = InputLimits::default(); // keep import path stable if estimate uses contracts
    let err = TransactionRuntime::submit(rt.as_ref(), req).unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::InvalidInput);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// D-015: transaction-wide tool payload cap rejects oversized arguments.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tool_payload_transaction_cap_rejects() {
    use monoloop_contracts::{
        ChannelId, ExchangeId, JsonSchema, SessionId, SessionKey, ToolActionId, ToolExecutionClass,
        ToolId, ToolLimits, ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract,
        TransactionId,
    };
    use monoloop_loop::{
        DispatchOutcome, DispatchRequest, DispatcherLimits, HostToolRegistry, ImmediateToolHandler,
        RegisteredTool, ResolvedToolSet, SharedToolCapacity, TransactionToolDispatcher,
    };

    let schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "q": { "type": "string" } },
        "required": ["q"],
        "additionalProperties": false
    }))
    .unwrap();
    let success = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "ok": { "type": "boolean" } },
        "required": ["ok"],
        "additionalProperties": false
    }))
    .unwrap();
    let host = HostToolRegistry::build(vec![RegisteredTool::new(
        ToolSpec::try_new(
            ToolId::try_new("echo").unwrap(),
            ToolName::try_new("echo").unwrap(),
            "echo",
            schema,
            ToolOutputContract {
                success: ToolSuccessContract::json(success),
                error_data_schema: None,
            },
            ToolLimits {
                max_concurrent: 4,
                max_input_bytes: 1024 * 1024, // per-tool is large
                max_output_bytes: 1024,
                execution_deadline: Duration::from_secs(5),
            },
            ToolExecutionClass::CooperativeInProcess {
                grace: std::time::Duration::from_millis(50),
            },
        )
        .unwrap(),
        Arc::new(ImmediateToolHandler::new(|_, _| {
            Ok(monoloop_contracts::ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({"ok": true})),
            ))
        })) as Arc<dyn monoloop_loop::ToolHandler>,
    )])
    .unwrap();
    let tool = host.get(&ToolId::try_new("echo").unwrap()).unwrap().clone();
    let tools = ResolvedToolSet::from_registered(vec![tool]);
    // Transaction-wide payload cap is tiny (8 bytes) — JSON args will exceed it.
    let d = TransactionToolDispatcher::with_limits(
        TransactionId::generate(),
        SessionKey::new(
            ChannelId::try_new("ch").unwrap(),
            SessionId::try_new("s").unwrap(),
        ),
        tools,
        SharedToolCapacity::unlimited(),
        DispatcherLimits {
            max_concurrent_tools: 4,
            max_queued_tools: 8,
            max_tool_payload_bytes: 8,
            max_tool_output_bytes: 1024,
        },
    );
    let outcome = d
        .dispatch(DispatchRequest {
            exchange_id: ExchangeId::generate(),
            tool_action_id: ToolActionId::new("a1"),
            tool_name: ToolName::try_new("echo").unwrap(),
            provider_tool_call_id: "p1".into(),
            request_ordinal: 0,
            arguments_json: r#"{"q":"this-is-too-long"}"#.into(),
        })
        .await;
    match outcome {
        DispatchOutcome::Rejected { code, .. } => {
            assert!(
                code.contains("oversized") || code.contains("invalid") || code == "oversized_input",
                "expected oversized reject, got {code}"
            );
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
}

/// D-015: zero / inconsistent transaction limits fail startup validation.
#[test]
fn transaction_limits_zero_capacity_rejected() {
    use monoloop_contracts::{LimitsError, TransactionLimits};
    let limits = TransactionLimits {
        max_event_queue: 0,
        ..Default::default()
    };
    assert!(matches!(
        limits.validate(),
        Err(LimitsError::ZeroCapacity("max_event_queue"))
    ));
    let base = TransactionLimits::default();
    let limits = TransactionLimits {
        max_active_per_channel: base.max_active_transactions + 1,
        ..base
    };
    assert!(matches!(
        limits.validate(),
        Err(LimitsError::Inconsistent(_))
    ));
}

/// D-015: event byte budget rejects oversized enqueue.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_queue_byte_budget_plus_one() {
    use monoloop_contracts::{
        ChannelId, SessionId, TransactionEvent, TransactionEventPayload, TransactionId,
    };
    use monoloop_contracts::{
        EventDeliveryOutcome, TransactionEnd, TransactionEndKind, TransactionUsage,
    };
    use monoloop_loop::{BoundedEventSender, QueuedEvent};

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    // Tiny byte budget so one Ended event exceeds it.
    let sender = BoundedEventSender::new(tx, 32);
    let end = TransactionEnd {
        transaction_id: TransactionId::generate(),
        session_id: Some(SessionId::try_new("s").unwrap()),
        channel_id: ChannelId::try_new("ch").unwrap(),
        kind: TransactionEndKind::Completed,
        prior_terminal_cause: None,
        event_delivery: EventDeliveryOutcome::Accepted,
        emitted_events: 1,
        usage: TransactionUsage::default(),
        diagnostics: vec![],
    };
    let ev = TransactionEvent {
        transaction_id: end.transaction_id,
        channel_id: end.channel_id.clone(),
        session_id: SessionId::try_new("s").unwrap(),
        sequence: 1,
        payload: TransactionEventPayload::Ended(end),
    };
    let err = sender.send(QueuedEvent::new(ev, None)).await;
    assert!(err.is_err(), "oversized event must fail byte budget");
    assert!(rx.try_recv().is_err());
}

/// D-021: host event-sink panic at invoke → delivery failure, one terminal callback.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_panic_on_invoke_yields_event_delivery_failed() {
    let rt = start_with_limits(vec![llm_binding("llm", 4)], limits_cap(4, 4)).await;
    let ends = Arc::new(Mutex::new(Vec::<TransactionEndKind>::new()));
    let done = Arc::new(Notify::new());
    let ends_c = Arc::clone(&ends);
    let done_c = Arc::clone(&done);
    let sink: Arc<dyn monoloop_contracts::TransactionEventSink> =
        Arc::new(FnEventSink(move |_e| {
            panic!("host sink panic at invoke");
        }));
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            None,
            sink,
            Box::new(FnCompletionCallback(move |end: TransactionEnd| {
                ends_c.lock().unwrap().push(end.kind);
                done_c.notify_waiters();
                Box::pin(async { Ok(()) }) as monoloop_contracts::CompletionDelivery
            })),
        ),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("callback must fire after sink panic");
    let kinds = ends.lock().unwrap().clone();
    assert_eq!(kinds.len(), 1, "exactly one callback");
    assert_eq!(kinds[0], TransactionEndKind::EventDeliveryFailed);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    assert_eq!(rt.active_count(), 0);
}

/// D-021: host event-sink panic while polling deliver future → same fail-closed path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_panic_in_future_yields_event_delivery_failed() {
    let rt = start_with_limits(vec![llm_binding("llm", 4)], limits_cap(4, 4)).await;
    let ends = Arc::new(Mutex::new(Vec::<TransactionEndKind>::new()));
    let done = Arc::new(Notify::new());
    let ends_c = Arc::clone(&ends);
    let done_c = Arc::clone(&done);
    let sink: Arc<dyn monoloop_contracts::TransactionEventSink> =
        Arc::new(FnEventSink(move |_e| {
            Box::pin(async {
                panic!("host sink panic in future");
                #[allow(unreachable_code)]
                Ok(())
            }) as monoloop_contracts::EventDelivery
        }));
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            None,
            sink,
            Box::new(FnCompletionCallback(move |end: TransactionEnd| {
                ends_c.lock().unwrap().push(end.kind);
                done_c.notify_waiters();
                Box::pin(async { Ok(()) }) as monoloop_contracts::CompletionDelivery
            })),
        ),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("callback must fire after sink future panic");
    let kinds = ends.lock().unwrap().clone();
    assert_eq!(kinds.len(), 1);
    assert_eq!(kinds[0], TransactionEndKind::EventDeliveryFailed);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// D-021: slow host callback does not hold transaction capacity (runtime-owned service).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_callback_does_not_block_capacity_release() {
    let rt = start_with_limits(vec![llm_binding("llm", 2)], limits_cap(2, 2)).await;
    let hold = Arc::new(Notify::new());
    let started = Arc::new(AtomicUsize::new(0));
    for i in 0..2 {
        let hold_c = Arc::clone(&hold);
        let started_c = Arc::clone(&started);
        TransactionRuntime::submit(
            rt.as_ref(),
            free_request(
                "llm",
                Some(SessionId::try_new(format!("slow-cb-{i}")).unwrap()),
                Arc::new(FnEventSink(|_| {
                    Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
                })),
                Box::new(FnCompletionCallback(move |_end: TransactionEnd| {
                    started_c.fetch_add(1, Ordering::SeqCst);
                    let hold_c = Arc::clone(&hold_c);
                    Box::pin(async move {
                        hold_c.notified().await;
                        Ok(())
                    }) as monoloop_contracts::CompletionDelivery
                })),
            ),
        )
        .unwrap();
    }
    // Capacity must free even while callbacks are still held.
    for _ in 0..100 {
        if rt.active_count() == 0 && rt.capacity().global_active() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(rt.active_count(), 0);
    assert_eq!(rt.capacity().global_active(), 0);
    // D-029: callback slots reserved at admission are retained through callback
    // terminal, so a third admit fails closed while both slow callbacks hold slots.
    let after_err = TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            Some(SessionId::try_new("after-slow-cb").unwrap()),
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            Box::new(FnCompletionCallback(|_end: TransactionEnd| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::CompletionDelivery
            })),
        ),
    )
    .expect_err("callback capacity must fail closed while slow callbacks hold slots");
    assert_eq!(
        after_err.kind,
        monoloop_contracts::AdmissionErrorKind::CapacityExceeded
    );
    hold.notify_waiters();
    // After callbacks complete, admission succeeds again.
    let done = Arc::new(Notify::new());
    let ends = Arc::new(AtomicUsize::new(0));
    let mut admitted = false;
    for _ in 0..100 {
        match TransactionRuntime::submit(
            rt.as_ref(),
            free_request(
                "llm",
                Some(SessionId::try_new("after-slow-cb-released").unwrap()),
                Arc::new(FnEventSink(|_| {
                    Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
                })),
                counting_completion(Arc::clone(&ends), Arc::clone(&done)),
            ),
        ) {
            Ok(_) => {
                admitted = true;
                break;
            }
            Err(e) if e.kind == monoloop_contracts::AdmissionErrorKind::CapacityExceeded => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("unexpected admit error after callback release: {e:?}"),
        }
    }
    assert!(admitted, "admit after slow callbacks release");
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("third transaction completes");
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(2)).await;
}

/// D-021: completion callback panic must not kill runtime; capacity fully released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_panic_does_not_kill_runtime() {
    let rt = start_with_limits(vec![llm_binding("llm", 4)], limits_cap(4, 4)).await;
    let done = Arc::new(Notify::new());
    // Use a barrier so the test can observe that the transaction left active state.
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            None,
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            Box::new(FnCompletionCallback(move |_end: TransactionEnd| {
                done.notify_waiters();
                panic!("host callback panic at invoke");
            })),
        ),
    )
    .unwrap();
    // Callback may panic before notify if panic is at invoke — use capacity drain instead.
    for _ in 0..100 {
        if rt.active_count() == 0 && rt.capacity().global_active() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(rt.active_count(), 0);
    assert_eq!(rt.capacity().global_active(), 0);
    // Runtime still accepts a subsequent transaction.
    let done2 = Arc::new(Notify::new());
    let ends = Arc::new(AtomicUsize::new(0));
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            None,
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            counting_completion(Arc::clone(&ends), Arc::clone(&done2)),
        ),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), done2.notified())
        .await
        .expect("second transaction completes");
    assert_eq!(ends.load(Ordering::SeqCst), 1);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// D-015: max_distinct_sessions plus-one rejects a new session on the channel.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinct_sessions_plus_one_rejected() {
    let binding = ChannelBinding {
        id: ChannelId::try_new("llm").unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::ModelToolCalls,
        connector_factory: Arc::new(FakeConnectorFactory::direct_llm()),
        encoder: Arc::new(TestTextEncoder),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "default".into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: caps(),
        limits: ChannelLimits {
            max_active_transactions: 8,
            max_distinct_sessions: 2,
            max_encoded_exchange_bytes: 4 * 1024 * 1024,
        },
    };
    let rt = start_with_limits(
        vec![binding],
        TransactionLimits {
            max_active_transactions: 8,
            max_active_per_channel: 8,
            ..Default::default()
        },
    )
    .await;
    let gate = Arc::new(Notify::new());
    let ends = Arc::new(AtomicUsize::new(0));
    for i in 0..2 {
        let done = Arc::new(Notify::new());
        TransactionRuntime::submit(
            rt.as_ref(),
            free_request(
                "llm",
                Some(SessionId::try_new(format!("ds-{i}")).unwrap()),
                blocked_sink(Arc::clone(&gate)),
                counting_completion(Arc::clone(&ends), done),
            ),
        )
        .unwrap();
    }
    let err = TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            Some(SessionId::try_new("ds-overflow").unwrap()),
            blocked_sink(Arc::clone(&gate)),
            counting_completion(Arc::clone(&ends), Arc::new(Notify::new())),
        ),
    )
    .unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::CapacityExceeded);
    gate.notify_waiters();
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(2)).await;
}

/// D-015: channel max_encoded_exchange_bytes fails closed as EncodingFailed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encoded_exchange_bytes_plus_one_fails() {
    let binding = ChannelBinding {
        id: ChannelId::try_new("llm").unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::ModelToolCalls,
        connector_factory: Arc::new(FakeConnectorFactory::direct_llm()),
        encoder: Arc::new(TestTextEncoder),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "default".into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: caps(),
        limits: ChannelLimits {
            max_active_transactions: 4,
            max_distinct_sessions: 4,
            max_encoded_exchange_bytes: 8, // tiny
        },
    };
    let rt = start_with_limits(
        vec![binding],
        TransactionLimits {
            max_input_bytes: 64 * 1024,
            max_active_transactions: 4,
            max_active_per_channel: 4,
            ..Default::default()
        },
    )
    .await;
    let ends = Arc::new(Mutex::new(Vec::<TransactionEndKind>::new()));
    let done = Arc::new(Notify::new());
    let ends_c = Arc::clone(&ends);
    let done_c = Arc::clone(&done);
    let mut req = free_request(
        "llm",
        None,
        Arc::new(FnEventSink(|_| {
            Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
        })),
        Box::new(FnCompletionCallback(move |end: TransactionEnd| {
            ends_c.lock().unwrap().push(end.kind);
            done_c.notify_waiters();
            Box::pin(async { Ok(()) }) as monoloop_contracts::CompletionDelivery
        })),
    );
    // Body well over 8 encoded bytes once TestTextEncoder serializes it.
    req.input = monoloop_contracts::user_text_input("0123456789abcdef-extra").unwrap();
    TransactionRuntime::submit(rt.as_ref(), req).unwrap();
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("terminal after encode limit");
    let kinds = ends.lock().unwrap().clone();
    assert_eq!(kinds.len(), 1);
    assert_eq!(kinds[0], TransactionEndKind::EncodingFailed);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// D-012: cancel while FakeConnector hangs after open (response wait) releases capacity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_during_response_wait_releases_capacity() {
    use monoloop_connector::{FakeConnectorConfig, FakeEndpoint};

    let factory = FakeConnectorFactory::direct_llm_with_config(FakeConnectorConfig {
        default_endpoint: FakeEndpoint::Hang,
        ..Default::default()
    });
    let binding = ChannelBinding {
        id: ChannelId::try_new("llm").unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::ModelToolCalls,
        connector_factory: Arc::new(factory),
        encoder: Arc::new(TestTextEncoder),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "default".into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: caps(),
        limits: ChannelLimits {
            max_active_transactions: 4,
            max_distinct_sessions: 4,
            max_encoded_exchange_bytes: 4 * 1024 * 1024,
        },
    };
    let rt = start_with_limits(vec![binding], limits_cap(4, 4)).await;
    let ends = Arc::new(Mutex::new(Vec::<TransactionEndKind>::new()));
    let done = Arc::new(Notify::new());
    let ends_c = Arc::clone(&ends);
    let done_c = Arc::clone(&done);
    let receipt = TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            Some(SessionId::try_new("hang-resp").unwrap()),
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            Box::new(FnCompletionCallback(move |end: TransactionEnd| {
                ends_c.lock().unwrap().push(end.kind);
                done_c.notify_waiters();
                Box::pin(async { Ok(()) }) as monoloop_contracts::CompletionDelivery
            })),
        ),
    )
    .unwrap();
    // Allow open + encode + send into hang owner.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let _ = TransactionRuntime::terminate(
        rt.as_ref(),
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("cancel during hang must callback");
    let kinds = ends.lock().unwrap().clone();
    assert_eq!(kinds.len(), 1);
    assert!(
        matches!(
            kinds[0],
            TransactionEndKind::Cancelled
                | TransactionEndKind::Terminated
                | TransactionEndKind::ConnectorFailed
                | TransactionEndKind::DeadlineExceeded
        ),
        "expected cancel/terminal path, got {:?}",
        kinds[0]
    );
    for _ in 0..50 {
        if rt.active_count() == 0 && rt.capacity().global_active() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(rt.active_count(), 0);
    assert_eq!(rt.capacity().global_active(), 0);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// D-012: cancel while FakeConnector open is delayed leaves zero active capacity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_during_slow_open_releases_capacity() {
    use monoloop_connector::FakeConnectorConfig;

    let factory = FakeConnectorFactory::direct_llm_with_config(FakeConnectorConfig {
        open_delay: Duration::from_secs(30),
        ..Default::default()
    });
    let binding = ChannelBinding {
        id: ChannelId::try_new("llm").unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::ModelToolCalls,
        connector_factory: Arc::new(factory),
        encoder: Arc::new(TestTextEncoder),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "default".into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: caps(),
        limits: ChannelLimits {
            max_active_transactions: 4,
            max_distinct_sessions: 4,
            max_encoded_exchange_bytes: 4 * 1024 * 1024,
        },
    };
    let rt = start_with_limits(vec![binding], limits_cap(4, 4)).await;
    let ends = Arc::new(Mutex::new(Vec::<TransactionEndKind>::new()));
    let done = Arc::new(Notify::new());
    let ends_c = Arc::clone(&ends);
    let done_c = Arc::clone(&done);
    let receipt = TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            Some(SessionId::try_new("slow-open").unwrap()),
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            Box::new(FnCompletionCallback(move |end: TransactionEnd| {
                ends_c.lock().unwrap().push(end.kind);
                done_c.notify_waiters();
                Box::pin(async { Ok(()) }) as monoloop_contracts::CompletionDelivery
            })),
        ),
    )
    .unwrap();
    // Allow actor to enter open with delay.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = TransactionRuntime::terminate(
        rt.as_ref(),
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("cancel during open must callback");
    let kinds = ends.lock().unwrap().clone();
    assert_eq!(kinds.len(), 1);
    assert!(
        matches!(
            kinds[0],
            TransactionEndKind::Cancelled | TransactionEndKind::Terminated
        ),
        "expected cancel path, got {:?}",
        kinds[0]
    );
    for _ in 0..50 {
        if rt.active_count() == 0 && rt.capacity().global_active() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(rt.active_count(), 0);
    assert_eq!(rt.capacity().global_active(), 0);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// D-015: bound_diagnostics truncates count and message bytes.
#[test]
fn bound_diagnostics_respects_limits() {
    use monoloop_contracts::{SafeDiagnostic, TransactionDiagnostic};
    use monoloop_loop::bound_diagnostics;

    let make = |i: usize, msg: &str| TransactionDiagnostic {
        diagnostic: SafeDiagnostic::try_new(format!("c{i}"), Some(msg), 10_000).unwrap(),
    };
    let diags = vec![
        make(0, "aaaaaaaaaa"),
        make(1, "bbbbbbbbbb"),
        make(2, "cccccccccc"),
    ];
    let bounded = bound_diagnostics(diags, 2, 4);
    assert_eq!(bounded.len(), 2);
    assert!(bounded[0].diagnostic.message.as_ref().unwrap().len() <= 4);
}

/// D-012: configured cleanup_deadline is accepted and used (non-default value path).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_deadline_non_default_completes() {
    let limits = TransactionLimits {
        cleanup_deadline: Duration::from_millis(100),
        max_active_transactions: 4,
        max_active_per_channel: 4,
        ..Default::default()
    };
    let rt = start_with_limits(vec![llm_binding("llm", 4)], limits).await;
    let done = Arc::new(Notify::new());
    let ends = Arc::new(AtomicUsize::new(0));
    TransactionRuntime::submit(
        rt.as_ref(),
        free_request(
            "llm",
            None,
            Arc::new(FnEventSink(|_| {
                Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
            })),
            counting_completion(Arc::clone(&ends), Arc::clone(&done)),
        ),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("transaction with short cleanup_deadline completes");
    assert_eq!(ends.load(Ordering::SeqCst), 1);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

/// Redaction: external session Display and MCP capability Debug hide secrets.
#[test]
fn security_redaction_surfaces() {
    use monoloop_contracts::ExternalSessionId;
    use monoloop_loop::CapabilityToken;

    let sid = ExternalSessionId::try_new("super-secret-session-xyz").unwrap();
    let d = format!("{sid}");
    assert!(!d.contains("super-secret"));
    assert!(d.contains("external-session") || d.contains('<'));

    let tok = CapabilityToken::generate().expect("entropy");
    let dbg = format!("{tok:?}");
    assert!(dbg.contains("redacted"));
    assert!(!dbg.contains(&tok.to_hex()));
}
