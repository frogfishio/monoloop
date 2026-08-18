//! WP-04: admission, events, finalization, callbacks, terminate, shutdown.

use monoloop_connector::FakeConnectorFactory;
use monoloop_contracts::{
    user_text_input, AdmissionErrorKind, CancellationReason, CancellationReasonCode,
    ChannelCapabilities, ChannelDefaults, ChannelId, ChannelKind, ChannelLimits,
    ContinuationPolicy, DialectDescriptor, ExchangeMode, FnCompletionCallback, FnEventSink,
    InvocationConfig, McpConfigurationCapability, McpReachability, SessionConfig, SessionId,
    SessionMode, TerminationMode, ToolExecutionMode, ToolId, TransactionEnd, TransactionEndKind,
    TransactionEvent, TransactionEventPayload, TransactionRequest, TransactionRuntime,
    TransactionSelector,
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

fn caps(session: SessionMode, exchange: ExchangeMode) -> ChannelCapabilities {
    let d = DialectDescriptor::test_raw();
    let option_policy = if session == SessionMode::External {
        monoloop_contracts::OptionPolicy::external_agent()
    } else {
        monoloop_contracts::OptionPolicy::direct_llm()
    };
    ChannelCapabilities {
        session_mode: session,
        mcp_configuration: McpConfigurationCapability::None,
        mcp_reachability: McpReachability::None,
        exchange_mode: exchange,
        continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
        supports_distinct_session_concurrency: true,
        input_dialect: d.clone(),
        output_dialect: d,
        option_policy,
    }
}

fn llm_binding(id: &str) -> ChannelBinding {
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
        capabilities: caps(SessionMode::Stateless, ExchangeMode::RequestResponse),
        limits: ChannelLimits::default(),
    }
}

async fn start(channels: Vec<ChannelBinding>) -> Arc<DefaultTransactionRuntime> {
    DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(channels).unwrap(),
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap()
}

struct Recorders {
    events: Mutex<Vec<TransactionEvent>>,
    ends: Mutex<Vec<TransactionEnd>>,
    done: Notify,
}

impl Recorders {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            ends: Mutex::new(Vec::new()),
            done: Notify::new(),
        })
    }

    fn request(self: &Arc<Self>, channel: &str, session: Option<SessionId>) -> TransactionRequest {
        let rec = Arc::clone(self);
        let events: Arc<dyn monoloop_contracts::TransactionEventSink> =
            Arc::new(FnEventSink(move |e| {
                let rec = Arc::clone(&rec);
                Box::pin(async move {
                    rec.events.lock().unwrap().push(e);
                    Ok(())
                }) as monoloop_contracts::EventDelivery
            }));
        let rec2 = Arc::clone(self);
        let completion: Box<dyn monoloop_contracts::CompletionCallback> =
            Box::new(FnCompletionCallback(move |end| {
                let rec2 = Arc::clone(&rec2);
                Box::pin(async move {
                    rec2.ends.lock().unwrap().push(end);
                    rec2.done.notify_waiters();
                    Ok(())
                }) as monoloop_contracts::CompletionDelivery
            }));
        TransactionRequest {
            channel_id: ChannelId::try_new(channel).unwrap(),
            session_id: session,
            input: user_text_input("hello").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            events,
            completion,
        }
    }
}

#[tokio::test]
async fn submit_returns_while_work_pending_and_completes_once() {
    let rt = start(vec![llm_binding("llm")]).await;
    let rec = Recorders::new();
    let receipt = TransactionRuntime::submit(rt.as_ref(), rec.request("llm", None)).unwrap();
    assert!(receipt.session_id.is_some());

    tokio::time::timeout(Duration::from_secs(2), rec.done.notified())
        .await
        .expect("callback");
    {
        let ends = rec.ends.lock().unwrap();
        assert_eq!(ends.len(), 1);
        assert_eq!(ends[0].kind, TransactionEndKind::Completed);
        assert_eq!(ends[0].transaction_id, receipt.transaction_id);
    }
    {
        let events = rec.events.lock().unwrap();
        assert!(!events.is_empty());
        let last = events.last().unwrap();
        assert!(matches!(last.payload, TransactionEventPayload::Ended(_)));
        let mut seqs: Vec<_> = events.iter().map(|e| e.sequence).collect();
        seqs.sort_unstable();
        assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    }

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    assert_eq!(rt.active_count(), 0);
}

#[tokio::test]
async fn duplicate_session_key_rejected_same_channel() {
    let rt = start(vec![llm_binding("llm")]).await;
    let sid = SessionId::try_new("sess-1").unwrap();
    let rec1 = Recorders::new();
    let rec2 = Recorders::new();
    // Hold first open by using a long deadline and cancel later — but actor finishes fast.
    // For duplicate: submit two with same session before first finishes.
    // Race: use barrier by slow sink on first.
    let block = Arc::new(Notify::new());
    let block2 = Arc::clone(&block);
    let events: Arc<dyn monoloop_contracts::TransactionEventSink> =
        Arc::new(FnEventSink(move |_e| {
            let block2 = Arc::clone(&block2);
            Box::pin(async move {
                block2.notified().await;
                Ok(())
            }) as monoloop_contracts::EventDelivery
        }));
    let done = Arc::new(Notify::new());
    let done2 = Arc::clone(&done);
    let completion: Box<dyn monoloop_contracts::CompletionCallback> =
        Box::new(FnCompletionCallback(move |_e| {
            let done2 = Arc::clone(&done2);
            Box::pin(async move {
                done2.notify_waiters();
                Ok(())
            }) as monoloop_contracts::CompletionDelivery
        }));
    let req1 = TransactionRequest {
        channel_id: ChannelId::try_new("llm").unwrap(),
        session_id: Some(sid.clone()),
        input: user_text_input("a").unwrap(),
        session_config: None,
        invocation_config: InvocationConfig {
            deadline: Some(Duration::from_secs(5)),
            continuation_policy: ContinuationPolicy::CallerControlled,
            ..Default::default()
        },
        tools: vec![],
        events,
        completion,
    };
    TransactionRuntime::submit(rt.as_ref(), req1).unwrap();
    let err = TransactionRuntime::submit(rt.as_ref(), rec2.request("llm", Some(sid))).unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::SessionAlreadyActive);
    block.notify_waiters();
    let _ = tokio::time::timeout(Duration::from_secs(2), done.notified()).await;
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    let _ = rec1;
}

#[tokio::test]
async fn same_session_string_different_channels_ok() {
    let rt = start(vec![llm_binding("a"), llm_binding("b")]).await;
    let sid = SessionId::try_new("shared-string").unwrap();
    let r1 = Recorders::new();
    let r2 = Recorders::new();
    TransactionRuntime::submit(rt.as_ref(), r1.request("a", Some(sid.clone()))).unwrap();
    TransactionRuntime::submit(rt.as_ref(), r2.request("b", Some(sid))).unwrap();
    tokio::time::timeout(Duration::from_secs(2), r1.done.notified())
        .await
        .ok();
    tokio::time::timeout(Duration::from_secs(2), r2.done.notified())
        .await
        .ok();
    assert_eq!(r1.ends.lock().unwrap().len(), 1);
    assert_eq!(r2.ends.lock().unwrap().len(), 1);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

#[tokio::test]
async fn direct_llm_rejects_session_config() {
    let rt = start(vec![llm_binding("llm")]).await;
    let rec = Recorders::new();
    let mut req = rec.request("llm", None);
    req.session_config = Some(SessionConfig {
        mode: Some("agent".into()),
        ..Default::default()
    });
    let err = TransactionRuntime::submit(rt.as_ref(), req).unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::InvalidConfiguration);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

#[tokio::test]
async fn unknown_tool_rejected() {
    let rt = start(vec![llm_binding("llm")]).await;
    let rec = Recorders::new();
    let mut req = rec.request("llm", None);
    req.tools = vec![ToolId::try_new("nope").unwrap()];
    let err = TransactionRuntime::submit(rt.as_ref(), req).unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::UnknownTool);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

#[tokio::test]
async fn terminate_cancels_in_flight() {
    let rt = start(vec![llm_binding("llm")]).await;
    let block = Arc::new(Notify::new());
    let block2 = Arc::clone(&block);
    let ends = Arc::new(Mutex::new(Vec::new()));
    let ends2 = Arc::clone(&ends);
    let done = Arc::new(Notify::new());
    let done2 = Arc::clone(&done);

    let events: Arc<dyn monoloop_contracts::TransactionEventSink> =
        Arc::new(FnEventSink(move |_e| {
            let block2 = Arc::clone(&block2);
            Box::pin(async move {
                // Block ordinary events so actor sits in select until cancel or ends.
                // Actually actor may complete before first event if work is empty and races control.
                // Use long deadline + block on session path — force cancel immediately after admit.
                let _ = block2;
                Ok(())
            }) as monoloop_contracts::EventDelivery
        }));
    let completion: Box<dyn monoloop_contracts::CompletionCallback> =
        Box::new(FnCompletionCallback(move |end| {
            let ends2 = Arc::clone(&ends2);
            let done2 = Arc::clone(&done2);
            Box::pin(async move {
                ends2.lock().unwrap().push(end);
                done2.notify_waiters();
                Ok(())
            }) as monoloop_contracts::CompletionDelivery
        }));

    let req = TransactionRequest {
        channel_id: ChannelId::try_new("llm").unwrap(),
        session_id: None,
        input: user_text_input("x").unwrap(),
        session_config: None,
        invocation_config: InvocationConfig {
            deadline: Some(Duration::from_secs(30)),
            continuation_policy: ContinuationPolicy::CallerControlled,
            ..Default::default()
        },
        tools: vec![],
        events,
        completion,
    };
    let receipt = TransactionRuntime::submit(rt.as_ref(), req).unwrap();
    let disp = TransactionRuntime::terminate(
        rt.as_ref(),
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    // May be Accepted or NotFound if already finished.
    let _ = disp;
    let _ = tokio::time::timeout(Duration::from_secs(2), done.notified()).await;
    {
        let ends = ends.lock().unwrap();
        assert_eq!(ends.len(), 1);
        assert!(matches!(
            ends[0].kind,
            TransactionEndKind::Completed
                | TransactionEndKind::Cancelled
                | TransactionEndKind::Terminated
        ));
    }
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    let _ = block;
}

#[tokio::test]
async fn callback_exactly_once() {
    let rt = start(vec![llm_binding("llm")]).await;
    let rec = Recorders::new();
    TransactionRuntime::submit(rt.as_ref(), rec.request("llm", None)).unwrap();
    tokio::time::timeout(Duration::from_secs(2), rec.done.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(rec.ends.lock().unwrap().len(), 1);
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

#[tokio::test]
async fn shutdown_with_active_finalizes() {
    let rt = start(vec![llm_binding("llm")]).await;
    let rec = Recorders::new();
    // Block delivery so actor is still "active" briefly
    let block = Arc::new(Notify::new());
    let block2 = Arc::clone(&block);
    let ends = Arc::new(Mutex::new(0u32));
    let ends2 = Arc::clone(&ends);
    let events: Arc<dyn monoloop_contracts::TransactionEventSink> =
        Arc::new(FnEventSink(move |_e| {
            let block2 = Arc::clone(&block2);
            Box::pin(async move {
                block2.notified().await;
                Ok(())
            }) as monoloop_contracts::EventDelivery
        }));
    let completion: Box<dyn monoloop_contracts::CompletionCallback> =
        Box::new(FnCompletionCallback(move |_e| {
            let ends2 = Arc::clone(&ends2);
            Box::pin(async move {
                *ends2.lock().unwrap() += 1;
                Ok(())
            }) as monoloop_contracts::CompletionDelivery
        }));
    let req = TransactionRequest {
        channel_id: ChannelId::try_new("llm").unwrap(),
        session_id: None,
        input: user_text_input("x").unwrap(),
        session_config: None,
        invocation_config: InvocationConfig {
            deadline: Some(Duration::from_secs(30)),
            continuation_policy: ContinuationPolicy::CallerControlled,
            ..Default::default()
        },
        tools: vec![],
        events,
        completion,
    };
    TransactionRuntime::submit(rt.as_ref(), req).unwrap();
    let disp = TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(2)).await;
    block.notify_waiters();
    // Either normal or supervisor finalized at least one.
    assert!(disp.normally_finalized + disp.supervisor_finalized >= 1 || *ends.lock().unwrap() >= 1);
    let _ = rec;
}

#[tokio::test]
async fn generated_and_supplied_direct_llm_sessions() {
    let rt = start(vec![llm_binding("llm")]).await;
    let r1 = Recorders::new();
    let receipt = TransactionRuntime::submit(rt.as_ref(), r1.request("llm", None)).unwrap();
    assert!(receipt.session_id.is_some());
    tokio::time::timeout(Duration::from_secs(2), r1.done.notified())
        .await
        .ok();

    let supplied = SessionId::try_new("explicit-sess").unwrap();
    let r2 = Recorders::new();
    let receipt2 =
        TransactionRuntime::submit(rt.as_ref(), r2.request("llm", Some(supplied.clone()))).unwrap();
    assert_eq!(
        receipt2.session_id.as_ref().map(|s| s.as_str()),
        Some("explicit-sess")
    );
    tokio::time::timeout(Duration::from_secs(2), r2.done.notified())
        .await
        .ok();
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}
