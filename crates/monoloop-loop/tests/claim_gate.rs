//! D-026 / D-053: claim-gate fail-closed typing on StartedRuntime.
//!
//! Missing provider session id on ExternalAgent create → InvariantFailed
//! (must not remapped to Cancelled).

use monoloop_connector::{FakeConnectorConfig, FakeConnectorFactory, FakeSessionAdapterConfig};
use monoloop_contracts::{
    transaction_delivery, user_text_input, ChannelCapabilities, ChannelDefaults, ChannelId,
    ChannelKind, ChannelLimits, ContinuationPolicy, DeliveryLimits, DialectDescriptor,
    ExchangeMode, InvocationConfig, McpConfigurationCapability, McpReachability, OptionPolicy,
    SessionMode, ShutdownWaitOutcome, ToolExecutionMode, TransactionEndKind,
    TransactionSubmitRequest,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, HostToolRegistry, RuntimeBootstrap, RuntimeConfig,
    StartedRuntime, TestTextEncoder,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

fn external_agent_omit_session(id: &str) -> ChannelBinding {
    let d = DialectDescriptor::test_raw();
    let connector_cfg = FakeConnectorConfig {
        omit_created_session_id: true,
        ..Default::default()
    };
    ChannelBinding {
        id: ChannelId::try_new(id).unwrap(),
        kind: ChannelKind::ExternalAgent,
        tool_mode: ToolExecutionMode::None,
        connector_factory: Arc::new(FakeConnectorFactory::external_agent_with_connector_config(
            FakeSessionAdapterConfig::default(),
            connector_cfg,
        )),
        encoder: Arc::new(TestTextEncoder),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "default".into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: ChannelCapabilities {
            session_mode: SessionMode::External,
            mcp_configuration: McpConfigurationCapability::None,
            mcp_reachability: McpReachability::None,
            exchange_mode: ExchangeMode::Bidirectional,
            continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
            supports_distinct_session_concurrency: true,
            input_dialect: d.clone(),
            output_dialect: d,
            option_policy: OptionPolicy::external_agent(),
        },
        limits: ChannelLimits::default(),
    }
}

#[test]
fn create_without_provider_session_id_ends_invariant_failed() {
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_omit_session("agent")]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    // `receiver` is mut for completion.recv(); events drained only if needed.
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: None,
            input: user_text_input("hello").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(5)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            delivery,
        })
        .expect("admit");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), receiver.completion.recv()).await
        })
        .expect("completion timeout")
        .expect("completion");

    assert_eq!(
        completion.end.kind,
        TransactionEndKind::InvariantFailed,
        "missing provider sessionId must not remapped to Cancelled; got {:?}",
        completion.end.kind
    );

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
}
