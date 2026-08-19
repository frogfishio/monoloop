//! WP-05: fake Channel end-to-end through public TransactionRuntime API.

use monoloop_connector::FakeConnectorFactory;
use monoloop_contracts::{
    user_text_input, ChannelCapabilities, ChannelDefaults, ChannelId, ChannelKind, ChannelLimits,
    ContinuationPolicy, DialectDescriptor, ExchangeMode, FnCompletionCallback, FnEventSink,
    InvocationConfig, McpConfigurationCapability, McpReachability, SessionMode, ToolExecutionMode,
    TransactionEnd, TransactionEndKind, TransactionEvent, TransactionEventPayload,
    TransactionRequest, TransactionRuntime,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, DefaultTransactionRuntime, HostToolRegistry, RuntimeBootstrap,
    RuntimeConfig, TestTextEncoder,
};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

fn test_llm(id: &str) -> ChannelBinding {
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
            option_policy: monoloop_contracts::OptionPolicy::direct_llm(),
        },
        limits: ChannelLimits::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fake_provider_transaction_emits_canonical_units() {
    let rt = DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![test_llm("echo")]).unwrap(),
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap();

    let events = Arc::new(Mutex::new(Vec::<TransactionEvent>::new()));
    let ends = Arc::new(Mutex::new(0u32));
    let done = Arc::new(Notify::new());

    let events_s = Arc::clone(&events);
    let sink: Arc<dyn monoloop_contracts::TransactionEventSink> = Arc::new(FnEventSink(move |e| {
        let events_s = Arc::clone(&events_s);
        Box::pin(async move {
            events_s.lock().unwrap().push(e);
            Ok(())
        }) as monoloop_contracts::EventDelivery
    }));

    let ends_s = Arc::clone(&ends);
    let done_s = Arc::clone(&done);
    let completion: Box<dyn monoloop_contracts::CompletionCallback> =
        Box::new(FnCompletionCallback(move |end: TransactionEnd| {
            let ends_s = Arc::clone(&ends_s);
            let done_s = Arc::clone(&done_s);
            Box::pin(async move {
                assert_eq!(end.kind, TransactionEndKind::Completed);
                *ends_s.lock().unwrap() += 1;
                done_s.notify_waiters();
                Ok(())
            }) as monoloop_contracts::CompletionDelivery
        }));

    let receipt = TransactionRuntime::submit(
        rt.as_ref(),
        TransactionRequest {
            channel_id: ChannelId::try_new("echo").unwrap(),
            session_id: None,
            input: user_text_input("Hello from WP-05 exchange").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(10)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            events: sink,
            completion,
        },
    )
    .unwrap();
    assert!(receipt.session_id.is_some());

    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("transaction completed");

    assert_eq!(*ends.lock().unwrap(), 1);
    {
        let evs = events.lock().unwrap();
        let unit_count = evs
            .iter()
            .filter(|e| matches!(e.payload, TransactionEventPayload::CanonicalUnit(_)))
            .count();
        assert!(
            unit_count > 0,
            "expected at least one CanonicalUnit from FakeConnector echo + Test dialect"
        );
        assert!(evs
            .iter()
            .any(|e| matches!(e.payload, TransactionEventPayload::Ended(_))));
    }

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    assert_eq!(rt.active_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_transactions_isolated() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let rt = DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![test_llm("a"), test_llm("b")]).unwrap(),
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap();

    let mk = |channel: &str| {
        let ch = channel.to_string();
        let finished = Arc::new(AtomicBool::new(false));
        let notify = Arc::new(Notify::new());
        let finished2 = Arc::clone(&finished);
        let notify2 = Arc::clone(&notify);
        let ch_mark = ch.clone();
        let sink: Arc<dyn monoloop_contracts::TransactionEventSink> = Arc::new(FnEventSink(|_| {
            Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
        }));
        let completion: Box<dyn monoloop_contracts::CompletionCallback> =
            Box::new(FnCompletionCallback(move |end: TransactionEnd| {
                let finished2 = Arc::clone(&finished2);
                let notify2 = Arc::clone(&notify2);
                let ch_mark = ch_mark.clone();
                Box::pin(async move {
                    assert_eq!(end.kind, TransactionEndKind::Completed);
                    assert_eq!(end.channel_id.as_str(), ch_mark);
                    finished2.store(true, Ordering::SeqCst);
                    notify2.notify_waiters();
                    Ok(())
                }) as monoloop_contracts::CompletionDelivery
            }));
        let req = TransactionRequest {
            channel_id: ChannelId::try_new(&ch).unwrap(),
            session_id: None,
            input: user_text_input(format!("msg on {ch}")).unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(10)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            events: sink,
            completion,
        };
        (req, finished, notify)
    };

    let (r1, f1, n1) = mk("a");
    let (r2, f2, n2) = mk("b");
    TransactionRuntime::submit(rt.as_ref(), r1).unwrap();
    TransactionRuntime::submit(rt.as_ref(), r2).unwrap();

    let wait = |f: Arc<AtomicBool>, n: Arc<Notify>| async move {
        while !f.load(Ordering::SeqCst) {
            n.notified().await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), wait(f1, n1))
        .await
        .expect("channel a completed");
    tokio::time::timeout(Duration::from_secs(10), wait(f2, n2))
        .await
        .expect("channel b completed");
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}
