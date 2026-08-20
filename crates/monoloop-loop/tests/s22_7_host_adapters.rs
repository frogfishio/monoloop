//! §22.7 Adversarial host behavior — outside the runtime core.
//!
//! Host adapters drain v2 mailboxes on the **caller** task. These proofs show
//! that blocking / non-yielding / dropped hosts cannot stall the supervisor.

use monoloop_connector::FakeConnectorFactory;
use monoloop_contracts::{
    transaction_delivery, user_text_input, ChannelCapabilities, ChannelDefaults, ChannelId,
    ChannelKind, ChannelLimits, CompletionDelivery, ContinuationPolicy, DeliveryLimits,
    DialectDescriptor, EventDelivery, ExchangeMode, FnCompletionCallback, FnEventSink,
    InvocationConfig, McpConfigurationCapability, McpReachability, OptionPolicy, SessionMode,
    ShutdownWaitOutcome, ToolExecutionMode, TransactionLimits, TransactionSubmitRequest,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    adapt_completion_callback, adapt_event_sink, ChannelBinding, ChannelRegistry, HostToolRegistry,
    RuntimeBootstrap, RuntimeConfig, StartedRuntime, TestTextEncoder,
};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn llm_binding() -> ChannelBinding {
    let d = DialectDescriptor::test_raw();
    ChannelBinding {
        id: ChannelId::try_new("llm").unwrap(),
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
            max_active_transactions: 2,
            ..ChannelLimits::default()
        },
    }
}

fn start_runtime() -> StartedRuntime {
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            enable_mcp_listener: false,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding()]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start")
}

/// Callback blocks before returning a future — must not stall runtime Stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_7_completion_callback_blocks_before_future_runtime_still_stops() {
    let started = start_runtime();
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect("admit");

    let entered = Arc::new(AtomicBool::new(false));
    let entered2 = Arc::clone(&entered);
    let callback = Box::new(FnCompletionCallback(move |_end| {
        entered2.store(true, Ordering::SeqCst);
        // Block the host adapter task before producing a future.
        // Concurrent with wait_stopped — proves the supervisor is not waiting
        // on this host sleep (50ms is enough to overlap ShutdownDrain).
        std::thread::sleep(Duration::from_millis(50));
        Box::pin(async { Ok(()) }) as CompletionDelivery
    }));

    let monoloop_contracts::TransactionReceiver {
        mut events,
        completion,
    } = receiver;
    let adapter = tokio::spawn(async move {
        let _ = adapt_completion_callback(completion, callback).await;
    });
    // Drain events on a separate host task (optional).
    let events_task = tokio::spawn(async move { while events.recv().await.is_some() {} });

    let mut owner = started.owner;
    let outcome = owner.wait_stopped(Duration::from_secs(3)).await;
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "blocking host completion callback must not prevent Stopped, got {outcome:?}"
    );
    let _ = adapter.await;
    events_task.abort();
    let _ = events_task.await;
    assert!(
        entered.load(Ordering::SeqCst),
        "host callback should have been invoked on the adapter task"
    );
}

/// Callback future never yields — host task hangs; runtime must still Stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_7_completion_future_never_yields_runtime_still_stops() {
    let started = start_runtime();
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect("admit");

    let callback = Box::new(FnCompletionCallback(move |_end| {
        Box::pin(async {
            // Never-yielding future on the host adapter task.
            std::future::pending::<()>().await;
            Ok(())
        }) as CompletionDelivery
    }));

    let monoloop_contracts::TransactionReceiver {
        mut events,
        completion,
    } = receiver;
    let adapter = tokio::spawn(async move {
        let _ = adapt_completion_callback(completion, callback).await;
    });
    let events_task = tokio::spawn(async move { while events.recv().await.is_some() {} });

    let mut owner = started.owner;
    let outcome = owner.wait_stopped(Duration::from_secs(3)).await;
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "non-yielding host completion future must not prevent Stopped, got {outcome:?}"
    );
    adapter.abort();
    let _ = adapter.await;
    events_task.abort();
    let _ = events_task.await;
}

/// Event consumer stops draining — runtime publishes to mailbox and still Stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_7_event_consumer_stops_draining_runtime_still_stops() {
    let started = start_runtime();
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(8, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect("admit");

    let drained = Arc::new(AtomicU32::new(0));
    let drained2 = Arc::clone(&drained);
    let sink: Arc<dyn monoloop_contracts::TransactionEventSink> =
        Arc::new(FnEventSink(move |_e| {
            drained2.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) }) as EventDelivery
        }));

    let monoloop_contracts::TransactionReceiver {
        mut events,
        completion,
    } = receiver;
    drop(completion);
    // Drain only one event then stop (adversarial consumer).
    let adapter = tokio::spawn(async move {
        if let Some(ev) = events.recv().await {
            let _ = sink.deliver(ev).await;
        }
        // Intentionally stop draining.
    });

    let mut owner = started.owner;
    let outcome = owner.wait_stopped(Duration::from_secs(3)).await;
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "stopped event drain must not prevent Stopped, got {outcome:?}"
    );
    let _ = adapter.await;
    let _ = drained.load(Ordering::SeqCst);
}

/// Receivers dropped immediately — runtime still completes and Stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_7_receivers_dropped_immediately_runtime_still_stops() {
    let started = start_runtime();
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect("admit");
    drop(receiver); // immediate drop of both event + completion receivers

    let mut owner = started.owner;
    let outcome = owner.wait_stopped(Duration::from_secs(3)).await;
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "dropping receivers immediately must not prevent Stopped, got {outcome:?}"
    );
}

/// Host adapter task destroyed mid-drain — runtime still Stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_7_host_adapter_task_destroyed_runtime_still_stops() {
    let started = start_runtime();
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect("admit");

    let monoloop_contracts::TransactionReceiver { events, completion } = receiver;
    let sink: Arc<dyn monoloop_contracts::TransactionEventSink> = Arc::new(FnEventSink(|_e| {
        Box::pin(async { Ok(()) }) as EventDelivery
    }));
    let adapter = tokio::spawn(adapt_event_sink(events, sink));
    // Destroy the host adapter executor task while the runtime is still live.
    adapter.abort();
    let _ = adapter.await;
    drop(completion);

    let mut owner = started.owner;
    let outcome = owner.wait_stopped(Duration::from_secs(3)).await;
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "destroying host adapter task must not prevent Stopped, got {outcome:?}"
    );
}
