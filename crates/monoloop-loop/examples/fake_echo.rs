//! Minimal DirectLlm smoke assembly with FakeConnector (no network, no testkit).
//!
//! Component-level twin of `monoloop`'s `fake_echo` example.
//!
//! Run: `cargo run -p monoloop-loop --example fake_echo`

use monoloop_connector::FakeConnectorFactory;
use monoloop_contracts::{
    user_text_input, ChannelCapabilities, ChannelDefaults, ChannelId, ChannelKind, ChannelLimits,
    ContinuationPolicy, DialectDescriptor, ExchangeMode, FnCompletionCallback, FnEventSink,
    InvocationConfig, McpConfigurationCapability, McpReachability, OptionPolicy, SessionMode,
    ToolExecutionMode, TransactionEnd, TransactionEndKind, TransactionEvent,
    TransactionEventPayload, TransactionRequest, TransactionRuntime,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use monoloop_loop::{
    ChannelBinding, ChannelRegistry, DefaultTransactionRuntime, HostToolRegistry, RuntimeBootstrap,
    RuntimeConfig, TestTextEncoder,
};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

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

#[tokio::main]
async fn main() {
    let rt = DefaultTransactionRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: false,
            ..Default::default()
        },
        channels: ChannelRegistry::build(vec![echo_channel("echo")]).expect("registry"),
        tools: HostToolRegistry::empty(),
        executor: tokio::runtime::Handle::current(),
    })
    .await
    .expect("runtime start");

    let units = Arc::new(Mutex::new(0usize));
    let (done_tx, done_rx) = oneshot::channel::<()>();
    let done_tx = Arc::new(Mutex::new(Some(done_tx)));

    let units_sink = Arc::clone(&units);
    let events = Arc::new(FnEventSink(move |ev: TransactionEvent| {
        let units_sink = Arc::clone(&units_sink);
        Box::pin(async move {
            if matches!(ev.payload, TransactionEventPayload::CanonicalUnit(_)) {
                *units_sink.lock().expect("lock") += 1;
            }
            Ok(())
        }) as monoloop_contracts::EventDelivery
    }));

    let done_cb = Arc::clone(&done_tx);
    let completion = Box::new(FnCompletionCallback(move |end: TransactionEnd| {
        let done_cb = Arc::clone(&done_cb);
        Box::pin(async move {
            assert_eq!(end.kind, TransactionEndKind::Completed);
            if let Some(tx) = done_cb.lock().expect("lock").take() {
                let _ = tx.send(());
            }
            Ok(())
        }) as monoloop_contracts::CompletionDelivery
    }));

    TransactionRuntime::submit(
        rt.as_ref(),
        TransactionRequest {
            channel_id: ChannelId::try_new("echo").expect("id"),
            session_id: None,
            input: user_text_input("Hello from monoloop-loop fake_echo").expect("input"),
            session_config: None,
            invocation_config: InvocationConfig {
                deadline: Some(Duration::from_secs(10)),
                continuation_policy: ContinuationPolicy::CallerControlled,
                ..Default::default()
            },
            tools: vec![],
            events,
            completion,
        },
    )
    .expect("admit");

    tokio::time::timeout(Duration::from_secs(5), done_rx)
        .await
        .expect("completion timeout")
        .expect("completion channel");

    println!(
        "ok: completed with {} canonical unit event(s)",
        *units.lock().expect("lock")
    );
}
