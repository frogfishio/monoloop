//! WP-12: presentation reconstructed solely from canonical TransactionEvents.
//!
//! Proves the testkit projection path does not require feeding presentation
//! state back into the runtime — only Interpreter/canonical units matter.

use monoloop_connector::FakeConnectorFactory;
use monoloop_contracts::{
    user_text_input, ChannelCapabilities, ChannelDefaults, ChannelId, ChannelKind, ChannelLimits,
    ContinuationPolicy, DialectDescriptor, ExchangeMode, FnCompletionCallback, FnEventSink,
    InterpreterOutputEvent, InvocationConfig, McpConfigurationCapability, McpReachability,
    SessionMode, ToolExecutionMode, TransactionEnd, TransactionEndKind, TransactionEvent,
    TransactionEventPayload, TransactionRequest, TransactionRuntime,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, DefaultTransactionRuntime, HostToolRegistry, RuntimeBootstrap,
    RuntimeConfig, TestTextEncoder,
};
use monoloop_testkit::{project_chat, ChatRole};
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
        },
        limits: ChannelLimits::default(),
    }
}

/// Map TransactionEvent stream → InterpreterOutputEvent units (downstream only).
fn units_from_transaction_events(events: &[TransactionEvent]) -> Vec<InterpreterOutputEvent> {
    events
        .iter()
        .filter_map(|e| match &e.payload {
            TransactionEventPayload::CanonicalUnit(u) => {
                Some(InterpreterOutputEvent::Unit(Box::new(u.clone())))
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn chat_projection_from_transaction_events_only() {
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
    let done = Arc::new(Notify::new());
    let events_s = Arc::clone(&events);
    let sink: Arc<dyn monoloop_contracts::TransactionEventSink> = Arc::new(FnEventSink(move |e| {
        let events_s = Arc::clone(&events_s);
        Box::pin(async move {
            events_s.lock().unwrap().push(e);
            Ok(())
        }) as monoloop_contracts::EventDelivery
    }));
    let done_s = Arc::clone(&done);
    let completion: Box<dyn monoloop_contracts::CompletionCallback> =
        Box::new(FnCompletionCallback(move |end: TransactionEnd| {
            let done_s = Arc::clone(&done_s);
            Box::pin(async move {
                assert_eq!(end.kind, TransactionEndKind::Completed);
                done_s.notify_waiters();
                Ok(())
            }) as monoloop_contracts::CompletionDelivery
        }));

    let prompt = "Hello presentation reconstruction";
    TransactionRuntime::submit(
        rt.as_ref(),
        TransactionRequest {
            channel_id: ChannelId::try_new("echo").unwrap(),
            session_id: None,
            input: user_text_input(prompt).unwrap(),
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

    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("transaction completed");

    let evs = events.lock().unwrap().clone();
    assert!(
        evs.iter()
            .any(|e| matches!(e.payload, TransactionEventPayload::Ended(_))),
        "need Ended for a complete stream"
    );
    let units = units_from_transaction_events(&evs);
    assert!(
        !units.is_empty(),
        "expected CanonicalUnit events for echo dialect"
    );

    // Presentation is built only from extracted canonical units — no runtime handle.
    let chat = project_chat(&units);
    // Fake echo + test dialect yields public response text containing the prompt.
    let agent: Vec<_> = chat
        .lines
        .iter()
        .filter(|l| l.role == ChatRole::Agent)
        .collect();
    assert!(
        !agent.is_empty()
            || chat.lines.iter().any(|l| l.text.contains("Hello"))
            || !chat.plain_text.is_empty(),
        "projection should surface content from units; chat={chat:?}"
    );

    // Replay is pure: same units → same projection.
    let chat2 = project_chat(&units);
    assert_eq!(chat.lines.len(), chat2.lines.len());
    for (a, b) in chat.lines.iter().zip(chat2.lines.iter()) {
        assert_eq!(a.role, b.role);
        assert_eq!(a.text, b.text);
    }

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    assert_eq!(rt.active_count(), 0);
}
