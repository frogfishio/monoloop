//! D-026 claim-gate fail-closed typing: missing provider session id → InvariantFailed.

use monoloop_connector::{FakeConnectorConfig, FakeConnectorFactory, FakeSessionAdapterConfig};
use monoloop_contracts::{
    user_text_input, ChannelCapabilities, ChannelDefaults, ChannelId, ChannelKind, ChannelLimits,
    ContinuationPolicy, DialectDescriptor, ExchangeMode, FnCompletionCallback, FnEventSink,
    InvocationConfig, McpConfigurationCapability, McpReachability, OptionPolicy, SessionMode,
    ToolExecutionMode, TransactionEnd, TransactionEndKind, TransactionRequest, TransactionRuntime,
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

#[tokio::test]
async fn create_without_provider_session_id_ends_invariant_failed() {
    let rt = DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_omit_session("agent")]).unwrap(),
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .unwrap();

    let ends = Arc::new(Mutex::new(Vec::<TransactionEnd>::new()));
    let done = Arc::new(Notify::new());
    let ends_s = Arc::clone(&ends);
    let done_s = Arc::clone(&done);

    let events: Arc<dyn monoloop_contracts::TransactionEventSink> =
        Arc::new(FnEventSink(|_| Box::pin(async { Ok(()) }) as _));
    let completion: Box<dyn monoloop_contracts::CompletionCallback> =
        Box::new(FnCompletionCallback(move |end: TransactionEnd| {
            let ends_s = Arc::clone(&ends_s);
            let done_s = Arc::clone(&done_s);
            Box::pin(async move {
                ends_s.lock().unwrap().push(end);
                done_s.notify_waiters();
                Ok(())
            }) as _
        }));

    TransactionRuntime::submit(
        rt.as_ref(),
        TransactionRequest {
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
            events,
            completion,
        },
    )
    .unwrap();

    tokio::time::timeout(Duration::from_secs(5), done.notified())
        .await
        .expect("completion");

    {
        let ends = ends.lock().unwrap();
        assert_eq!(ends.len(), 1);
        assert_eq!(
            ends[0].kind,
            TransactionEndKind::InvariantFailed,
            "missing provider sessionId must not remapped to Cancelled; got {:?}",
            ends[0].kind
        );
    }

    TransactionRuntime::shutdown(rt.as_ref(), Duration::from_secs(1)).await;
}
