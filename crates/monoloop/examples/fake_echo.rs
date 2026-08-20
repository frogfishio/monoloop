//! Minimal DirectLlm smoke assembly with FakeConnector (no network, no testkit).
//!
//! Uses loop-owned `TestTextEncoder` + `DialectDescriptor::test_raw()` only.
//! Live hosts should prefer profile `*_channel_binding` helpers.
//!
//! Runtime v2: `StartedRuntime::start` owns the executor (no external `Handle`).
//!
//! Run: `cargo run -p monoloop --example fake_echo`

use monoloop::connector::FakeConnectorFactory;
use monoloop::contracts::{
    transaction_delivery, user_text_input, ChannelCapabilities, ChannelDefaults, ChannelId,
    ChannelKind, ChannelLimits, ContinuationPolicy, DeliveryLimits, DialectDescriptor,
    ExchangeMode, InvocationConfig, McpConfigurationCapability, McpReachability, OptionPolicy,
    SessionMode, ShutdownWaitOutcome, ToolExecutionMode, TransactionEventPayload,
    TransactionSubmitRequest,
};
use monoloop::interpreter::DefaultInterpreterFactory;
use monoloop::loop_runtime::{
    ChannelBinding, ChannelRegistry, HostToolRegistry, RuntimeBootstrap, RuntimeConfig,
    StartedRuntime, TestTextEncoder,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

fn echo_channel(id: &str) -> ChannelBinding {
    let d = DialectDescriptor::test_raw();
    ChannelBinding {
        id: ChannelId::try_new(id).expect("channel id"),
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
        limits: ChannelLimits::default(),
    }
}

fn main() {
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![echo_channel("echo")]).expect("registry"),
        tools: HostToolRegistry::empty(),
    })
    .expect("runtime start");

    let handle = started.handle.clone();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).expect("limits"))
            .expect("delivery");

    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("echo").expect("id"),
            session_id: None,
            input: user_text_input("Hello from monoloop fake_echo").expect("input"),
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
        .expect("wait runtime");

    let completion = wait_rt
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), receiver.completion.recv()).await
        })
        .expect("completion timeout")
        .expect("completion channel");

    let mut units = 0usize;
    while let Ok(ev) = receiver.events.try_recv() {
        if matches!(ev.payload, TransactionEventPayload::CanonicalUnit(_)) {
            units = units.saturating_add(1);
        }
    }

    let mut owner = started.owner;
    let outcome = wait_rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );

    println!(
        "ok: {:?} with {} canonical unit event(s)",
        completion.end.kind, units
    );
}
