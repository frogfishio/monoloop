//! Shared lifecycle test helpers (Fake DirectLlm / Hang / admit / shutdown).

use super::super::{StartedRuntime, TransactionRuntimeHandle};
use crate::transaction::bootstrap::{RuntimeBootstrap, RuntimeConfig};
use crate::transaction::channel_registry::{ChannelBinding, ChannelRegistry};
use crate::transaction::fake_support::TestTextEncoder;
use crate::transaction::host_tools::HostToolRegistry;
use monoloop_connector::{FakeConnectorConfig, FakeConnectorFactory, FakeEndpoint};
use monoloop_contracts::{
    transaction_delivery, user_text_input, AdmissionError, AdmissionReceipt, ChannelCapabilities,
    ChannelDefaults, ChannelId, ChannelKind, ChannelLimits, ContinuationPolicy, DeliveryLimits,
    DialectDescriptor, ExchangeMode, InvocationConfig, McpConfigurationCapability, McpReachability,
    OptionPolicy, SessionId, SessionMode, ToolExecutionMode, TransactionLimits,
    TransactionReceiver, TransactionSubmitRequest,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

pub(super) fn llm_binding(id: &str, channel_max: usize) -> ChannelBinding {
    llm_binding_with_factory(
        id,
        channel_max,
        Arc::new(FakeConnectorFactory::direct_llm()),
    )
}

pub(super) fn hang_llm_binding(id: &str, channel_max: usize) -> ChannelBinding {
    let cfg = FakeConnectorConfig {
        default_endpoint: FakeEndpoint::Hang,
        ..FakeConnectorConfig::default()
    };
    llm_binding_with_factory(
        id,
        channel_max,
        Arc::new(FakeConnectorFactory::direct_llm_with_config(cfg)),
    )
}

pub(super) fn llm_binding_with_factory(
    id: &str,
    channel_max: usize,
    connector_factory: Arc<dyn monoloop_connector::ConnectorFactory>,
) -> ChannelBinding {
    let d = DialectDescriptor::test_raw();
    ChannelBinding {
        id: ChannelId::try_new(id).unwrap(),
        kind: ChannelKind::DirectLlm,
        tool_mode: ToolExecutionMode::ModelToolCalls,
        connector_factory,
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
            max_active_transactions: channel_max,
            ..ChannelLimits::default()
        },
    }
}

pub(super) fn start_runtime(max_active: usize, channel_max: usize) -> StartedRuntime {
    start_runtime_with_mcp(max_active, channel_max, false)
}

pub(super) fn start_runtime_with_mcp(
    max_active: usize,
    channel_max: usize,
    mcp: bool,
) -> StartedRuntime {
    let limits = TransactionLimits {
        max_active_transactions: max_active,
        max_active_per_channel: channel_max.min(max_active),
        // Keep exchange/shutdown tests bounded (default is 600s).
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            enable_mcp_listener: mcp,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", channel_max)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start")
}

pub(super) fn submit_ports(
    handle: &TransactionRuntimeHandle,
    session: Option<&str>,
) -> (
    Result<AdmissionReceipt, AdmissionError>,
    TransactionReceiver,
) {
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let result = handle.submit(TransactionSubmitRequest {
        channel_id: ChannelId::try_new("llm").unwrap(),
        session_id: session.map(|s| SessionId::try_new(s).unwrap()),
        input: user_text_input("hi").unwrap(),
        session_config: None,
        invocation_config: InvocationConfig::default(),
        tools: vec![],
        delivery,
    });
    (result, receiver)
}

pub(super) fn submit(
    handle: &TransactionRuntimeHandle,
    session: Option<&str>,
) -> Result<(AdmissionReceipt, TransactionReceiver), AdmissionError> {
    let (result, receiver) = submit_ports(handle, session);
    result.map(|receipt| (receipt, receiver))
}

/// §22.1: rejected admission publishes no event and no completion.
pub(super) fn assert_rejected_silent(mut receiver: TransactionReceiver) {
    match receiver.events.try_recv() {
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {}
        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
            panic!("event sender still live after rejected admission");
        }
        Ok(ev) => panic!("rejected admission published event: {ev:?}"),
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(receiver.completion.recv());
    assert!(
        completion.is_err(),
        "rejected admission must not publish completion, got {completion:?}"
    );
}

pub(super) fn shutdown_owner(started: StartedRuntime) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let _ = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
}

pub(super) fn submit_ports_on(
    handle: &TransactionRuntimeHandle,
    channel: &str,
    session: Option<&str>,
) -> (
    Result<AdmissionReceipt, AdmissionError>,
    TransactionReceiver,
) {
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let result = handle.submit(TransactionSubmitRequest {
        channel_id: ChannelId::try_new(channel).unwrap(),
        session_id: session.map(|s| SessionId::try_new(s).unwrap()),
        input: user_text_input("hi").unwrap(),
        session_config: None,
        invocation_config: InvocationConfig::default(),
        tools: vec![],
        delivery,
    });
    (result, receiver)
}

pub(super) fn submit_on(
    handle: &TransactionRuntimeHandle,
    channel: &str,
    session: Option<&str>,
) -> Result<(AdmissionReceipt, TransactionReceiver), AdmissionError> {
    let (result, receiver) = submit_ports_on(handle, channel, session);
    result.map(|receipt| (receipt, receiver))
}
