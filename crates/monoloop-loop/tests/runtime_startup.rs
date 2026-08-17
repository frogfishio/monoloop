//! WP-03: RuntimeBootstrap, Channel registry, startup/shutdown.

use monoloop_connector::{FakeConnectorFactory, FakeSessionAdapterConfig};
use monoloop_contracts::{
    user_text_input, AdmissionErrorKind, ChannelCapabilities, ChannelDefaults, ChannelId,
    ChannelKind, ChannelLimits, ContinuationPolicy, DialectDescriptor, ExchangeMode,
    FnCompletionCallback, FnEventSink, InvocationConfig, JsonSchema, McpConfigurationCapability,
    McpReachability, SessionMode, ToolCancellationPolicy, ToolExecutionMode, ToolId, ToolLimits,
    ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionRequest,
    TransactionRuntime,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, DefaultTransactionRuntime, HostToolRegistry, TestTextEncoder,
    RuntimeBootstrap, RuntimeConfig, RuntimeState, StartupError,
};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

fn test_caps(session: SessionMode, exchange: ExchangeMode) -> ChannelCapabilities {
    let d = DialectDescriptor::test_raw();
    ChannelCapabilities {
        session_mode: session,
        mcp_configuration: McpConfigurationCapability::None,
        mcp_reachability: McpReachability::None,
        exchange_mode: exchange,
        continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
        supports_distinct_session_concurrency: true,
        input_dialect: d.clone(),
        output_dialect: d,
    }
}

fn direct_llm_binding(id: &str) -> ChannelBinding {
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
        capabilities: test_caps(SessionMode::Stateless, ExchangeMode::RequestResponse),
        limits: ChannelLimits::default(),
    }
}

fn external_agent_binding(id: &str) -> ChannelBinding {
    ChannelBinding {
        id: ChannelId::try_new(id).unwrap(),
        kind: ChannelKind::ExternalAgent,
        tool_mode: ToolExecutionMode::None,
        connector_factory: Arc::new(FakeConnectorFactory::external_agent(
            FakeSessionAdapterConfig::default(),
        )),
        encoder: Arc::new(TestTextEncoder),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "default".into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: test_caps(SessionMode::External, ExchangeMode::Bidirectional),
        limits: ChannelLimits::default(),
    }
}

async fn start_runtime(channels: Vec<ChannelBinding>) -> Arc<DefaultTransactionRuntime> {
    let registry = ChannelRegistry::build(channels).unwrap();
    let bootstrap = RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: true,
            ..Default::default()
        },
        channels: registry,
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    };
    DefaultTransactionRuntime::start(bootstrap).await.unwrap()
}

#[tokio::test]
async fn starts_and_stops_with_fake_channels() {
    let rt = start_runtime(vec![
        direct_llm_binding("llm"),
        external_agent_binding("agent"),
    ])
    .await;
    assert_eq!(rt.state(), RuntimeState::Accepting);
    assert_eq!(rt.channel_count(), 2);
    assert!(rt.mcp_local_addr().await.unwrap().ip().is_loopback());

    let disp = TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(2)).await;
    assert_eq!(disp.invariant_failed, 0);
    assert_eq!(rt.state(), RuntimeState::Stopped);
}

#[tokio::test]
async fn no_submit_after_draining() {
    let rt = start_runtime(vec![direct_llm_binding("llm")]).await;
    let _ = TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    let err = TransactionRuntime::submit(rt.as_ref(), dummy_request("llm")).unwrap_err();
    assert_eq!(err.kind, AdmissionErrorKind::RuntimeShuttingDown);
}

#[tokio::test]
async fn submit_while_accepting_admits() {
    let rt = start_runtime(vec![direct_llm_binding("llm")]).await;
    let receipt = TransactionRuntime::submit(rt.as_ref(), dummy_request("llm")).unwrap();
    assert!(receipt.session_id.is_some());
    // Allow actor to finish.
    tokio::time::sleep(Duration::from_millis(50)).await;
    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}

#[tokio::test]
async fn repeated_start_shutdown_no_listener_leak() {
    for _ in 0..8 {
        let rt = start_runtime(vec![direct_llm_binding("llm")]).await;
        let addr = rt.mcp_local_addr().await;
        assert!(addr.is_some());
        TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
    }
}

#[test]
fn invalid_capability_combinations_rejected_at_registry() {
    let mut bad = direct_llm_binding("x");
    bad.capabilities.session_mode = SessionMode::External;
    let err = match ChannelRegistry::build(vec![bad]) {
        Err(e) => e,
        Ok(_) => panic!("expected capability error"),
    };
    assert!(matches!(err, StartupError::ChannelCapability(_)));
}

#[test]
fn duplicate_channel_ids_rejected() {
    let err = match ChannelRegistry::build(vec![
        direct_llm_binding("same"),
        direct_llm_binding("same"),
    ]) {
        Err(e) => e,
        Ok(_) => panic!("expected duplicate id error"),
    };
    assert!(matches!(err, StartupError::ChannelRegistry(_)));
}

#[test]
fn duplicate_tool_ids_rejected() {
    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let mk = |id: &str, name: &str| {
        ToolSpec::try_new(
            ToolId::try_new(id).unwrap(),
            ToolName::try_new(name).unwrap(),
            "d",
            schema.clone(),
            ToolOutputContract {
                success: ToolSuccessContract::json(
                    JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap(),
                ),
                error_data_schema: None,
            },
            ToolLimits::default(),
            ToolCancellationPolicy::Abortable,
        )
        .unwrap()
    };
    let err = HostToolRegistry::build(vec![mk("t1", "a"), mk("t1", "b")]).unwrap_err();
    assert!(matches!(err, StartupError::ToolRegistry(_)));
}

#[test]
fn duplicate_tool_names_rejected() {
    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let mk = |id: &str| {
        ToolSpec::try_new(
            ToolId::try_new(id).unwrap(),
            ToolName::try_new("same-name").unwrap(),
            "d",
            schema.clone(),
            ToolOutputContract {
                success: ToolSuccessContract::json(
                    JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap(),
                ),
                error_data_schema: None,
            },
            ToolLimits::default(),
            ToolCancellationPolicy::Abortable,
        )
        .unwrap()
    };
    let err = HostToolRegistry::build(vec![mk("a"), mk("b")]).unwrap_err();
    assert!(matches!(err, StartupError::ToolRegistry(_)));
}

#[tokio::test]
async fn partial_startup_cleans_mcp_on_connector_failure() {
    // First channel ok; second factory fails.
    struct FailFactory;
    impl monoloop_connector::ConnectorFactory for FailFactory {
        fn create(
            &self,
        ) -> Result<monoloop_connector::ConnectorInstance, monoloop_connector::ConnectorBuildError>
        {
            Err(monoloop_connector::ConnectorBuildError::ConfigurationInvalid(
                "forced",
            ))
        }
    }

    let ok = direct_llm_binding("ok");
    let mut bad = direct_llm_binding("bad");
    bad.connector_factory = Arc::new(FailFactory);

    let registry = ChannelRegistry::build(vec![ok, bad]).unwrap();
    let bootstrap = RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: true,
            ..Default::default()
        },
        channels: registry,
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    };
    let err = match DefaultTransactionRuntime::start(bootstrap).await {
        Err(e) => e,
        Ok(_) => panic!("expected connector build failure"),
    };
    assert!(matches!(err, StartupError::ConnectorBuild(_)));
}

#[tokio::test]
async fn direct_llm_with_session_adapter_fails_startup() {
    // External factory on DirectLlm kind.
    let mut b = direct_llm_binding("mismatch");
    b.connector_factory = Arc::new(FakeConnectorFactory::external_agent(
        FakeSessionAdapterConfig::default(),
    ));
    let registry = ChannelRegistry::build(vec![b]).unwrap();
    let bootstrap = RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: registry,
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    };
    let err = match DefaultTransactionRuntime::start(bootstrap).await {
        Err(e) => e,
        Ok(_) => panic!("expected session adapter mismatch"),
    };
    assert!(matches!(err, StartupError::SessionAdapterMismatch(_)));
}

#[tokio::test]
async fn capacity_managers_installed() {
    let rt = start_runtime(vec![direct_llm_binding("llm")]).await;
    assert_eq!(rt.capacity().global_active(), 0);
    assert!(rt.capacity().max_global() > 0);
    let id = ChannelId::try_new("llm").unwrap();
    assert!(rt.capacity().try_reserve(&id));
    assert_eq!(rt.capacity().global_active(), 1);
    rt.capacity().release(&id);
    assert_eq!(rt.capacity().global_active(), 0);
}

fn dummy_request(channel: &str) -> TransactionRequest {
    let events: Arc<dyn monoloop_contracts::TransactionEventSink> =
        Arc::new(FnEventSink(|_e| {
            Box::pin(async { Ok(()) }) as monoloop_contracts::EventDelivery
        }));
    let completion: Box<dyn monoloop_contracts::CompletionCallback> =
        Box::new(FnCompletionCallback(|_e| {
            Box::pin(async { Ok(()) }) as monoloop_contracts::CompletionDelivery
        }));
    TransactionRequest {
        channel_id: ChannelId::try_new(channel).unwrap(),
        session_id: None,
        input: user_text_input("hi").unwrap(),
        session_config: None,
        invocation_config: InvocationConfig::default(),
        tools: vec![],
        events,
        completion,
    }
}
