//! WP-12: presentation reconstructed solely from canonical TransactionEvents.
//!
//! Proves the testkit projection path does not require feeding presentation
//! state back into the runtime — only Interpreter/canonical units matter.
//!
//! Runtime v2: `StartedRuntime::start` + push `transaction_delivery`.

use monoloop_connector::FakeConnectorFactory;
use monoloop_contracts::{
    transaction_delivery, user_text_input, ChannelCapabilities, ChannelDefaults, ChannelId,
    ChannelKind, ChannelLimits, ContinuationPolicy, DeliveryLimits, DialectDescriptor,
    ExchangeMode, InterpreterOutputEvent, InvocationConfig, McpConfigurationCapability,
    McpReachability, SessionMode, ShutdownWaitOutcome, ToolExecutionMode, TransactionEvent,
    TransactionEventPayload, TransactionSubmitRequest,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, HostToolRegistry, RuntimeBootstrap, RuntimeConfig,
    StartedRuntime, TestTextEncoder,
};
use monoloop_testkit::{project_chat, ChatRole};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

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

#[test]
fn chat_projection_from_transaction_events_only() {
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![test_llm("echo")]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");

    let handle = started.handle.clone();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(64, 256 * 1024).unwrap()).unwrap();

    let prompt = "Hello presentation reconstruction";
    handle
        .submit(TransactionSubmitRequest {
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
            delivery,
        })
        .expect("admit");

    let wait_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let (completion, evs) = wait_rt.block_on(async {
        let completion = tokio::time::timeout(Duration::from_secs(5), receiver.completion.recv())
            .await
            .expect("completion timeout")
            .expect("completion channel");
        let mut evs = Vec::new();
        while let Ok(ev) = receiver.events.try_recv() {
            evs.push(ev);
        }
        (completion, evs)
    });

    assert!(
        evs.iter()
            .any(|e| matches!(e.payload, TransactionEventPayload::EndedEvent(_))),
        "need EndedEvent for a complete stream"
    );
    let units = units_from_transaction_events(&evs);
    assert!(
        !units.is_empty(),
        "expected CanonicalUnit events for echo dialect"
    );

    // Presentation is built only from extracted canonical units — no runtime handle.
    let chat = project_chat(&units);
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

    let _ = completion;
    let mut owner = started.owner;
    let outcome = wait_rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
}
