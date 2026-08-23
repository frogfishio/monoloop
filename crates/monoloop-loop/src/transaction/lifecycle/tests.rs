//! M2 admission / ownership tests (v2 §22.1 subset).

use super::{StartedRuntime, TransactionRuntimeHandle};
use crate::transaction::bootstrap::{
    FinalizerHoldGate, JoinOnlySpillInject, RuntimeBootstrap, RuntimeConfig, StartHoldGate,
    StoppedGate,
};
use crate::transaction::channel_registry::{ChannelBinding, ChannelRegistry};
use crate::transaction::fake_support::PanicEncoder;
use crate::transaction::fake_support::TestTextEncoder;
use crate::transaction::host_tools::HostToolRegistry;
use crate::transaction::state::RuntimeState;
use monoloop_connector::{FakeConnectorConfig, FakeConnectorFactory, FakeEndpoint};
use monoloop_contracts::{
    transaction_delivery, user_text_input, AdmissionError, AdmissionErrorKind, AdmissionReceipt,
    CancellationReason, CancellationReasonCode, ChannelCapabilities, ChannelDefaults, ChannelId,
    ChannelKind, ChannelLimits, ContinuationPolicy, DeliveryLimits, DialectDescriptor,
    ExchangeMode, InvocationConfig, McpConfigurationCapability, McpReachability, OptionPolicy,
    SessionId, SessionMode, ShutdownWaitOutcome, TerminationDisposition, TerminationMode,
    TerminationReason, TerminationReasonCode, ToolExecutionMode, TransactionEndKind, TransactionId,
    TransactionLimits, TransactionReceiver, TransactionSelector, TransactionSubmitRequest,
};
use monoloop_interpreter::DefaultInterpreterFactory;
use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn llm_binding(id: &str, channel_max: usize) -> ChannelBinding {
    llm_binding_with_factory(
        id,
        channel_max,
        Arc::new(FakeConnectorFactory::direct_llm()),
    )
}

fn hang_llm_binding(id: &str, channel_max: usize) -> ChannelBinding {
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

fn llm_binding_with_factory(
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

fn start_runtime(max_active: usize, channel_max: usize) -> StartedRuntime {
    start_runtime_with_mcp(max_active, channel_max, false)
}

fn start_runtime_with_mcp(max_active: usize, channel_max: usize, mcp: bool) -> StartedRuntime {
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

fn submit_ports(
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

fn submit(
    handle: &TransactionRuntimeHandle,
    session: Option<&str>,
) -> Result<(AdmissionReceipt, TransactionReceiver), AdmissionError> {
    let (result, receiver) = submit_ports(handle, session);
    result.map(|receipt| (receipt, receiver))
}

/// §22.1: rejected admission publishes no event and no completion.
fn assert_rejected_silent(mut receiver: TransactionReceiver) {
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

fn shutdown_owner(started: StartedRuntime) {
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

/// D-035: TransactionLimits.max_messages exact admits; plus-one rejects.
#[test]
fn max_messages_exact_admits_plus_one_rejects() {
    use monoloop_contracts::{CanonicalInput, CanonicalMessage, InputLimits, TextPart};

    let limits = TransactionLimits {
        max_messages: 1,
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let roomy = InputLimits {
        max_messages: 16,
        ..Default::default()
    };

    let exact = CanonicalInput::try_new(
        vec![CanonicalMessage::User {
            content: vec![TextPart::try_new("one", roomy.max_text_part_bytes).unwrap()],
            name: None,
        }],
        &roomy,
    )
    .unwrap();
    let (delivery_ok, mut recv_ok) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("exact-msg").unwrap()),
            input: exact,
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery: delivery_ok,
        })
        .expect("exact max_messages must admit");
    while recv_ok.events.try_recv().is_ok() {}
    drop(recv_ok);

    let plus = CanonicalInput::try_new(
        vec![
            CanonicalMessage::User {
                content: vec![TextPart::try_new("one", roomy.max_text_part_bytes).unwrap()],
                name: None,
            },
            CanonicalMessage::User {
                content: vec![TextPart::try_new("two", roomy.max_text_part_bytes).unwrap()],
                name: None,
            },
        ],
        &roomy,
    )
    .unwrap();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("plus-msg").unwrap()),
            input: plus,
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect_err("plus-one messages must reject");
    assert_eq!(err.kind, AdmissionErrorKind::InvalidInput);
    assert_rejected_silent(receiver);
    shutdown_owner(started);
}

/// D-035: TransactionLimits.max_messages plus-one rejects at admission.
#[test]
fn max_messages_plus_one_rejected_at_admit() {
    use monoloop_contracts::{CanonicalInput, CanonicalMessage, InputLimits, TextPart};

    let limits = TransactionLimits {
        max_messages: 1,
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let roomy = InputLimits {
        max_messages: 16,
        ..Default::default()
    };
    let input = CanonicalInput::try_new(
        vec![
            CanonicalMessage::User {
                content: vec![TextPart::try_new("one", roomy.max_text_part_bytes).unwrap()],
                name: None,
            },
            CanonicalMessage::User {
                content: vec![TextPart::try_new("two", roomy.max_text_part_bytes).unwrap()],
                name: None,
            },
        ],
        &roomy,
    )
    .unwrap();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: None,
            input,
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect_err("plus-one messages must reject");
    assert_eq!(err.kind, AdmissionErrorKind::InvalidInput);
    assert_rejected_silent(receiver);
    shutdown_owner(started);
}

/// D-035: TransactionLimits.max_input_bytes plus-one rejects at admission.
#[test]
fn max_input_bytes_plus_one_rejected_at_admit() {
    let limits = TransactionLimits {
        max_input_bytes: 8,
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: None,
            input: user_text_input("0123456789abcdef").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect_err("oversized text must reject");
    assert_eq!(err.kind, AdmissionErrorKind::InvalidInput);
    assert_rejected_silent(receiver);
    shutdown_owner(started);
}

/// D-035: TransactionLimits.max_content_parts exact admits; plus-one rejects.
#[test]
fn max_content_parts_exact_admits_plus_one_rejects() {
    use monoloop_contracts::{CanonicalInput, CanonicalMessage, InputLimits, TextPart};

    let limits = TransactionLimits {
        max_content_parts: 1,
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let roomy = InputLimits {
        max_content_parts: 8,
        ..Default::default()
    };

    let exact = CanonicalInput::try_new(
        vec![CanonicalMessage::User {
            content: vec![TextPart::try_new("a", roomy.max_text_part_bytes).unwrap()],
            name: None,
        }],
        &roomy,
    )
    .unwrap();
    let (delivery_ok, mut recv_ok) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("exact-parts").unwrap()),
            input: exact,
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery: delivery_ok,
        })
        .expect("exact max_content_parts must admit");
    while recv_ok.events.try_recv().is_ok() {}
    drop(recv_ok);

    let plus = CanonicalInput::try_new(
        vec![CanonicalMessage::User {
            content: vec![
                TextPart::try_new("a", roomy.max_text_part_bytes).unwrap(),
                TextPart::try_new("b", roomy.max_text_part_bytes).unwrap(),
            ],
            name: None,
        }],
        &roomy,
    )
    .unwrap();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("plus-parts").unwrap()),
            input: plus,
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect_err("plus-one content parts must reject");
    assert_eq!(err.kind, AdmissionErrorKind::InvalidInput);
    assert_rejected_silent(receiver);
    shutdown_owner(started);
}

/// D-035: TransactionLimits.max_content_parts plus-one rejects at admission.
#[test]
fn max_content_parts_plus_one_rejected_at_admit() {
    use monoloop_contracts::{CanonicalInput, CanonicalMessage, InputLimits, TextPart};

    let limits = TransactionLimits {
        max_content_parts: 1,
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let roomy = InputLimits {
        max_content_parts: 8,
        ..Default::default()
    };
    let input = CanonicalInput::try_new(
        vec![CanonicalMessage::User {
            content: vec![
                TextPart::try_new("a", roomy.max_text_part_bytes).unwrap(),
                TextPart::try_new("b", roomy.max_text_part_bytes).unwrap(),
            ],
            name: None,
        }],
        &roomy,
    )
    .unwrap();
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: None,
            input,
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect_err("plus-one content parts must reject");
    assert_eq!(err.kind, AdmissionErrorKind::InvalidInput);
    assert_rejected_silent(receiver);
    shutdown_owner(started);
}

/// D-035 / §23: `max_tools_per_transaction` exact admits; plus-one rejects.
///
/// Ports the unregistered v1 `hardening::tools_per_transaction_plus_one_rejected`
/// cell onto `StartedRuntime`. Error kind is `InvalidConfiguration` (current
/// admission vocabulary), not the v1 `InvalidInput` assertion.
#[test]
fn max_tools_per_transaction_exact_admits_plus_one_rejects() {
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        JsonSchema, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits, ToolName,
        ToolOutputContract, ToolSpec, ToolSuccessContract,
    };

    let schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    }))
    .unwrap();
    let make = |id: &str| {
        RegisteredTool::new(
            ToolSpec::try_new(
                ToolId::try_new(id).unwrap(),
                ToolName::try_new(id).unwrap(),
                "t",
                schema.clone(),
                ToolOutputContract {
                    success: ToolSuccessContract::json(schema.clone()),
                    error_data_schema: None,
                },
                ToolLimits::default(),
                ToolExecutionClass::CooperativeInProcess {
                    grace: Duration::from_millis(50),
                },
            )
            .unwrap(),
            Arc::new(ImmediateToolHandler::new(|_, _| {
                Ok(ToolCompletion::Succeeded(
                    monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
                ))
            })) as Arc<dyn crate::transaction::tool_handler::ToolHandler>,
        )
    };
    let tools = HostToolRegistry::build(vec![make("t1"), make("t2"), make("t3")]).unwrap();
    let limits = TransactionLimits {
        max_tools_per_transaction: 2,
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools,
    })
    .expect("start");
    let handle = started.handle.clone();

    let (delivery_ok, mut recv_ok) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("exact-tools").unwrap()),
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![
                ToolId::try_new("t1").unwrap(),
                ToolId::try_new("t2").unwrap(),
            ],
            delivery: delivery_ok,
        })
        .expect("exact max_tools_per_transaction must admit");
    while recv_ok.events.try_recv().is_ok() {}
    drop(recv_ok);

    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("plus-tools").unwrap()),
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![
                ToolId::try_new("t1").unwrap(),
                ToolId::try_new("t2").unwrap(),
                ToolId::try_new("t3").unwrap(),
            ],
            delivery,
        })
        .expect_err("plus-one tools must reject");
    assert_eq!(err.kind, AdmissionErrorKind::InvalidConfiguration);
    assert_rejected_silent(receiver);
    shutdown_owner(started);
}

/// D-035: exact max_input_bytes admits; plus-one rejects (equality, not only oversized).
#[test]
fn max_input_bytes_exact_admits_plus_one_rejects() {
    use monoloop_contracts::{
        estimate_canonical_input_bytes, CanonicalInput, CanonicalMessage, InputLimits, TextPart,
    };

    let roomy = InputLimits::default();
    let exact = CanonicalInput::try_new(
        vec![CanonicalMessage::User {
            content: vec![TextPart::try_new("01234567", roomy.max_text_part_bytes).unwrap()],
            name: None,
        }],
        &roomy,
    )
    .unwrap();
    let exact_bytes = estimate_canonical_input_bytes(&exact).unwrap();
    assert_eq!(exact_bytes, 8);

    let limits = TransactionLimits {
        max_input_bytes: exact_bytes,
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let (delivery_ok, mut recv_ok) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("exact").unwrap()),
            input: exact,
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery: delivery_ok,
        })
        .expect("exact max_input_bytes must admit");
    // Drop receiver so shutdown can drain.
    while recv_ok.events.try_recv().is_ok() {}
    drop(recv_ok);

    let plus = CanonicalInput::try_new(
        vec![CanonicalMessage::User {
            content: vec![TextPart::try_new("012345678", roomy.max_text_part_bytes).unwrap()],
            name: None,
        }],
        &roomy,
    )
    .unwrap();
    assert_eq!(estimate_canonical_input_bytes(&plus).unwrap(), exact_bytes + 1);
    let (delivery_bad, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("plus").unwrap()),
            input: plus,
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery: delivery_bad,
        })
        .expect_err("exact+1 must reject");
    assert_eq!(err.kind, AdmissionErrorKind::InvalidInput);
    assert_rejected_silent(receiver);
    shutdown_owner(started);
}

/// D-035: large historical tool arguments cannot bypass max_input_bytes via text-only construction.
#[test]
fn large_tool_arguments_counted_toward_max_input_bytes() {
    use monoloop_contracts::{
        estimate_canonical_input_bytes, CanonicalAssistantToolCall, CanonicalInput,
        CanonicalMessage, InputLimits, TextPart, ToolName,
    };

    let limits = TransactionLimits {
        // Text "hi" alone fits; args JSON pushes estimate over the cap.
        max_input_bytes: 32,
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let roomy = InputLimits::default();
    let args = serde_json::json!({"blob": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"});
    let input = CanonicalInput::try_new(
        vec![
            CanonicalMessage::User {
                content: vec![TextPart::try_new("hi", roomy.max_text_part_bytes).unwrap()],
                name: None,
            },
            CanonicalMessage::Assistant {
                content: vec![],
                tool_calls: vec![CanonicalAssistantToolCall {
                    tool_call_id: "c1".into(),
                    tool_name: ToolName::try_new("search").unwrap(),
                    arguments: args,
                }],
            },
        ],
        &roomy,
    )
    .unwrap();
    let estimated = estimate_canonical_input_bytes(&input).unwrap();
    assert!(
        estimated > 32,
        "fixture must exceed max_input_bytes via tool args, got {estimated}"
    );
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: None,
            input,
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect_err("tool-arg bytes must count toward max_input_bytes");
    assert_eq!(err.kind, AdmissionErrorKind::InvalidInput);
    assert_rejected_silent(receiver);
    shutdown_owner(started);
}

/// §18.4: Dropping RuntimeOwner initiates shutdown and joins the executor thread
/// (no abandon-after-grace detach). Empty cooperative runtime reaches Stopped.
#[test]
fn runtime_owner_drop_joins_executor_thread_reaches_stopped() {
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    // Contract violation path: drop without wait_stopped — ownership still joins.
    drop(started.owner);
    assert_eq!(
        handle.state(),
        RuntimeState::Stopped,
        "§18.4 Drop must join through Stopped, not abandon Quiescing"
    );
}

#[test]
fn submit_from_plain_os_thread_no_tokio_context() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let join = std::thread::spawn(move || submit(&handle, Some("s1")));
    let (receipt, receiver) = join.join().unwrap().expect("admit");
    assert!(receipt.session_id.is_some());

    // Shutdown from a tokio context for wait_stopped.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
    assert!(matches!(outcome, ShutdownWaitOutcome::Stopped(_)));
    let completion = rt.block_on(receiver.completion.recv()).expect("completion");
    assert_eq!(
        completion.end.kind,
        monoloop_contracts::TransactionEndKind::RuntimeShutdown
    );
    let _ = receiver.events;
}

#[test]
fn duplicate_session_rejects_second() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    submit(&handle, Some("same")).expect("first");
    let (err, recv) = submit_ports(&handle, Some("same"));
    assert_eq!(
        err.expect_err("duplicate").kind,
        AdmissionErrorKind::SessionAlreadyActive
    );
    assert_rejected_silent(recv);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let _ = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
}

#[test]
fn capacity_plus_one_rejects() {
    let started = start_runtime(1, 1);
    let handle = started.handle.clone();
    let (_r1, _recv1) = submit(&handle, Some("a")).expect("first");
    let (err, recv) = submit_ports(&handle, Some("b"));
    assert_eq!(
        err.expect_err("capacity").kind,
        AdmissionErrorKind::CapacityExceeded
    );
    assert_rejected_silent(recv);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let _ = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
}

/// D-015 / §23: `ChannelLimits.max_distinct_sessions` exact admits; plus-one
/// rejects on v2 ledger admission (Hang-pinned so slots stay occupied).
///
/// Distinct from global/per-channel `max_active` capacity — channel max_active
/// and global headroom remain above the distinct-session bound.
#[test]
fn max_distinct_sessions_exact_admits_plus_one_rejects() {
    let distinct_max = 2usize;
    let limits = TransactionLimits {
        max_active_transactions: 8,
        max_active_per_channel: 8,
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let mut binding = hang_llm_binding("llm", 8);
    binding.limits.max_distinct_sessions = distinct_max;
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![binding]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let mut receivers = Vec::new();
    for i in 0..distinct_max {
        let (receipt, recv) =
            submit(&handle, Some(&format!("ds-{i}"))).expect("exact distinct session");
        let _ = receipt;
        receivers.push(recv);
    }
    assert_eq!(started.owner.ledger_len(), distinct_max);

    let (err, overflow_recv) = submit_ports(&handle, Some("ds-overflow"));
    assert_eq!(
        err.expect_err("distinct sessions plus-one").kind,
        AdmissionErrorKind::CapacityExceeded
    );
    assert_rejected_silent(overflow_recv);
    assert_eq!(started.owner.ledger_len(), distinct_max);

    // Session-less admit does not consume a distinct-session slot at admit
    // (no SessionKey until external claim).
    let (none_ok, none_recv) = submit_ports(&handle, None);
    assert!(
        none_ok.is_ok(),
        "session-less admit must not hit max_distinct_sessions, got {none_ok:?}"
    );
    receivers.push(none_recv);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(10)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(
                report.completions_published as usize,
                distinct_max + 1,
                "exact distinct + session-less must each complete once"
            );
        }
        other => panic!("expected Stopped after distinct-sessions proof, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0);

    for recv in receivers {
        let _ = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
    }
}

/// Broader stress: N+1 barrier submits at exact `max_active` (global).
///
/// Distinct from sequential `capacity_plus_one_rejects`. Hang pins capacity so
/// completions cannot free slots mid-race; exactly `max` admit and one
/// `CapacityExceeded` reject are required.
#[test]
fn concurrent_global_capacity_exhaustion_admits_exactly_max() {
    let max = 4usize;
    let limits = TransactionLimits {
        max_active_transactions: max,
        max_active_per_channel: max,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", max)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let n = max + 1;
    let barrier = Arc::new(Barrier::new(n));
    let mut joins = Vec::new();
    for i in 0..n {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        let session = format!("cap-race-{i}");
        joins.push(std::thread::spawn(move || {
            b.wait();
            submit_ports(&h, Some(&session))
        }));
    }

    let mut admitted = 0usize;
    let mut rejected = 0usize;
    let mut receivers = Vec::new();
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(receipt) => {
                admitted += 1;
                let _ = receipt;
                receivers.push(recv);
            }
            Err(e) => {
                assert_eq!(e.kind, AdmissionErrorKind::CapacityExceeded);
                rejected += 1;
                assert_rejected_silent(recv);
            }
        }
    }
    assert_eq!(admitted, max, "exactly max_active must admit under barrier race");
    assert_eq!(rejected, 1, "exactly one CapacityExceeded overflow");
    assert_eq!(started.owner.ledger_len(), max);
    assert_eq!(started.owner.global_reservations(), max);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(
                report.completions_published as usize, max,
                "every admitted Hang tx must complete once"
            );
        }
        other => panic!("expected Stopped after capacity race, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0);
    assert_eq!(owner.global_reservations(), 0);

    for recv in receivers {
        let completion = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
        assert!(matches!(
            completion.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
                | monoloop_contracts::TransactionEndKind::Cancelled
                | monoloop_contracts::TransactionEndKind::Terminated
        ));
    }
}

/// Broader stress: per-channel max tighter than global — N+1 barrier submits
/// on one channel admit exactly `max_active_per_channel`.
#[test]
fn concurrent_per_channel_capacity_exhaustion_admits_exactly_channel_max() {
    let global = 8usize;
    let channel_max = 2usize;
    let limits = TransactionLimits {
        max_active_transactions: global,
        max_active_per_channel: channel_max,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", channel_max)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let n = channel_max + 1;
    let barrier = Arc::new(Barrier::new(n));
    let mut joins = Vec::new();
    for i in 0..n {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        let session = format!("ch-cap-race-{i}");
        joins.push(std::thread::spawn(move || {
            b.wait();
            submit_ports(&h, Some(&session))
        }));
    }

    let mut admitted = 0usize;
    let mut rejected = 0usize;
    let mut receivers = Vec::new();
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(receipt) => {
                admitted += 1;
                let _ = receipt;
                receivers.push(recv);
            }
            Err(e) => {
                assert_eq!(e.kind, AdmissionErrorKind::CapacityExceeded);
                rejected += 1;
                assert_rejected_silent(recv);
            }
        }
    }
    assert_eq!(
        admitted, channel_max,
        "exactly max_active_per_channel must admit"
    );
    assert_eq!(rejected, 1, "exactly one per-channel CapacityExceeded");
    assert_eq!(started.owner.ledger_len(), channel_max);
    let llm = ChannelId::try_new("llm").unwrap();
    assert_eq!(
        started.owner.global_reservations(),
        channel_max,
        "provisional global permits held for admitted only"
    );
    assert_eq!(
        started.owner.channel_reservations(&llm),
        channel_max,
        "channel pool must be at exact max"
    );
    assert!(
        channel_max < global,
        "topology must leave global headroom so overflow is channel-scoped"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(report.completions_published as usize, channel_max);
        }
        other => panic!("expected Stopped after per-channel capacity race, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0);
    assert_eq!(owner.global_reservations(), 0);
    assert_eq!(owner.channel_reservations(&llm), 0);

    for recv in receivers {
        let completion = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
        assert!(matches!(
            completion.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
                | monoloop_contracts::TransactionEndKind::Cancelled
                | monoloop_contracts::TransactionEndKind::Terminated
        ));
    }
}

/// WP-12 Golden progress: concurrent multi-Channel / multi-session load with
/// SessionKey isolation (Fake Hang path; not live Grok).
///
/// Distinct from single-channel capacity races and from
/// `s22_6_same_session_string_different_channels_isolated` (two-submit smoke):
/// barrier-raced admits across ≥3 Channels, shared external session strings
/// across Channels (independent SessionKeys), same-Channel duplicate reject
/// with capacity headroom, then fill-to-capacity `CapacityExceeded`, then one
/// shutdown completing every admission.
#[test]
fn multi_channel_multi_session_concurrent_load() {
    let channel_ids = ["ch-a", "ch-b", "ch-c"];
    let per_channel = 4usize;
    // Leave one free slot per channel so SessionAlreadyActive is observable
    // (capacity check must not mask the session reject).
    let occupied_per_channel = per_channel - 1;
    let global = channel_ids.len() * per_channel;
    let wave = channel_ids.len() * occupied_per_channel;
    let limits = TransactionLimits {
        max_active_transactions: global,
        max_active_per_channel: per_channel,
        // Wide pin window so Hang deadline cannot free slots mid-probe (Expert).
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(
            channel_ids
                .iter()
                .map(|id| hang_llm_binding(id, per_channel))
                .collect(),
        )
        .unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let barrier = Arc::new(Barrier::new(wave));
    let mut joins = Vec::new();
    for (ci, ch) in channel_ids.iter().enumerate() {
        for j in 0..occupied_per_channel {
            let h = handle.clone();
            let b = Arc::clone(&barrier);
            let channel = (*ch).to_string();
            // Even slots: same external string on every Channel (SessionKey
            // isolation). Odd slots: channel-unique sessions.
            let session = if j % 2 == 0 {
                format!("shared-slot-{j}")
            } else {
                format!("uniq-{channel}-j{j}-c{ci}")
            };
            joins.push(std::thread::spawn(move || {
                b.wait();
                submit_ports_on(&h, &channel, Some(&session))
            }));
        }
    }

    let mut admitted = 0usize;
    let mut receivers = Vec::new();
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(receipt) => {
                admitted += 1;
                let _ = receipt;
                receivers.push(recv);
            }
            Err(e) => panic!("multi-channel load admit must succeed, got {e:?}"),
        }
    }
    assert_eq!(admitted, wave, "partial-fill multi-channel wave must admit");
    assert_eq!(started.owner.ledger_len(), wave);
    assert_eq!(started.owner.global_reservations(), wave);
    for ch in channel_ids {
        let id = ChannelId::try_new(ch).unwrap();
        assert_eq!(
            started.owner.channel_reservations(&id),
            occupied_per_channel,
            "channel {ch} occupancy"
        );
    }

    // Headroom remains: duplicate SessionKey on ch-a is SessionAlreadyActive,
    // not CapacityExceeded.
    let (dup_err, dup_recv) = submit_ports_on(&handle, "ch-a", Some("shared-slot-0"));
    assert_eq!(
        dup_err.expect_err("duplicate SessionKey").kind,
        AdmissionErrorKind::SessionAlreadyActive
    );
    assert_rejected_silent(dup_recv);

    // Fill each channel's remaining slot with a unique session.
    for ch in channel_ids {
        let (res, recv) = submit_ports_on(
            &handle,
            ch,
            Some(&format!("fill-{ch}")),
        );
        let receipt = res.expect("fill slot must admit");
        let _ = receipt;
        receivers.push(recv);
        admitted += 1;
    }
    assert_eq!(admitted, global);
    assert_eq!(started.owner.ledger_len(), global);
    for ch in channel_ids {
        assert_eq!(
            started.owner.channel_reservations(&ChannelId::try_new(ch).unwrap()),
            per_channel
        );
    }

    let (cap_err, cap_recv) = submit_ports_on(&handle, "ch-a", Some("overflow-unique"));
    assert_eq!(
        cap_err.expect_err("channel full").kind,
        AdmissionErrorKind::CapacityExceeded
    );
    assert_rejected_silent(cap_recv);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(10)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(
                report.completions_published as usize, global,
                "every multi-channel admission completes once"
            );
        }
        other => panic!("expected Stopped after multi-channel load, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0);
    assert_eq!(owner.global_reservations(), 0);
    for ch in channel_ids {
        assert_eq!(
            owner.channel_reservations(&ChannelId::try_new(ch).unwrap()),
            0
        );
    }

    for recv in receivers {
        let completion = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
        assert!(matches!(
            completion.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
                | monoloop_contracts::TransactionEndKind::Cancelled
                | monoloop_contracts::TransactionEndKind::Terminated
        ));
    }
}

#[test]
fn shutdown_publishes_one_completion_per_admission() {
    let started = start_runtime(8, 8);
    let handle = started.handle.clone();
    let mut receivers = Vec::new();
    for i in 0..5 {
        let (_r, recv) = submit(&handle, Some(&format!("s{i}"))).unwrap();
        receivers.push(recv);
    }
    assert_eq!(started.owner.ledger_len(), 5);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            // Coordinators may finish with Completed before shutdown selects
            // RuntimeShutdown; every admission still gets exactly one publish.
            assert_eq!(report.completions_published, 5);
        }
        ShutdownWaitOutcome::TimedOut(snap) => {
            panic!("expected Stopped, got TimedOut: {snap:?}");
        }
    }
    let mut kinds = Vec::new();
    for recv in receivers {
        let c = rt
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(1), recv.completion.recv()).await
            })
            .expect("completion timed out")
            .expect("completion channel closed");
        kinds.push(c.end.kind);
    }
    assert_eq!(kinds.len(), 5);
    assert!(kinds.iter().all(|k| matches!(
        k,
        monoloop_contracts::TransactionEndKind::RuntimeShutdown
            | monoloop_contracts::TransactionEndKind::Completed
    )));
}

#[test]
fn fake_echo_exchange_emits_canonical_text_unit() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let (receipt, receiver) = submit(&handle, Some("echo")).unwrap();
    assert!(receipt.session_id.is_some());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (completion, saw_text) = rt.block_on(async {
        let mut events = receiver.events;
        let completion = tokio::time::timeout(Duration::from_secs(3), receiver.completion.recv())
            .await
            .expect("completion timed out")
            .expect("completion channel closed");
        let mut saw_text = false;
        while let Ok(ev) = events.try_recv() {
            if let monoloop_contracts::TransactionEventPayload::CanonicalUnit(unit) = &ev.payload {
                if let monoloop_contracts::CanonicalUnit::Text(t) = &unit.snapshot().unit {
                    // TestTextEncoder appends ". " to "hi"
                    assert!(t.content.contains("hi"), "unexpected text: {}", t.content);
                    saw_text = true;
                }
            }
        }
        (completion, saw_text)
    });
    assert!(saw_text, "expected Text canonical unit from Fake echo");
    assert_eq!(
        completion.end.kind,
        monoloop_contracts::TransactionEndKind::Completed
    );

    let mut owner = started.owner;
    let _ = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
}

#[test]
fn coordinator_publishes_sequenced_unit_and_completed() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let (receipt, receiver) = submit(&handle, Some("seq")).unwrap();
    assert!(receipt.session_id.is_some());

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(async {
        let mut events = receiver.events;
        let completion_rx = receiver.completion;
        // Allow coordinator + publisher to run.
        let completion = tokio::time::timeout(Duration::from_secs(2), completion_rx.recv())
            .await
            .expect("completion timed out")
            .expect("completion channel closed");
        let mut seqs = Vec::new();
        let mut saw_unit = false;
        while let Ok(ev) = events.try_recv() {
            seqs.push(ev.sequence);
            if matches!(
                ev.payload,
                monoloop_contracts::TransactionEventPayload::CanonicalUnit(_)
            ) {
                saw_unit = true;
            }
        }
        assert!(saw_unit, "expected at least one CanonicalUnit event");
        assert!(!seqs.is_empty());
        seqs.sort_unstable();
        for w in seqs.windows(2) {
            assert_eq!(w[1], w[0] + 1, "sequences must be contiguous");
        }
        completion
    });
    assert_eq!(
        completion.end.kind,
        monoloop_contracts::TransactionEndKind::Completed
    );

    let mut owner = started.owner;
    let _ = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
}

#[test]
fn reservation_pool_rejects_zero_capacity() {
    use super::ReservationPool;
    use monoloop_contracts::ChannelId;
    assert!(ReservationPool::try_new(0, vec![]).is_err());
    assert!(ReservationPool::try_new(1, vec![(ChannelId::try_new("c").unwrap(), 0)]).is_err());
}

#[test]
fn shutdown_control_not_starved_when_start_queue_full() {
    // max_active=2: admit two (fills start queue processing), then shutdown via
    // control path must still reach Stopped with both completions.
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let mut receivers = Vec::new();
    for i in 0..2 {
        let (_r, recv) = submit(&handle, Some(&format!("full{i}"))).unwrap();
        receivers.push(recv);
    }
    // Third admit must fail on capacity (reservations), not hang.
    let err = submit(&handle, Some("overflow")).expect_err("capacity");
    assert_eq!(err.kind, AdmissionErrorKind::CapacityExceeded);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == 2),
        "expected Stopped with 2 completions, got {outcome:?}"
    );
    for recv in receivers {
        let c = rt.block_on(recv.completion.recv()).unwrap();
        assert_eq!(
            c.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
        );
    }
}

/// D-040 / §22.1: short wait MUST TimedOut while Quiescing (not conditional Stopped).
#[test]
fn short_wait_may_timeout_while_quiescing_then_complete() {
    use crate::transaction::state::RuntimeState;

    let gate = Arc::new(StoppedGate::new());
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            block_stopped: Some(Arc::clone(&gate)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, _recv) = submit(&handle, Some("q")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    owner.begin_shutdown();
    let first = rt.block_on(owner.wait_stopped(Duration::ZERO));
    assert!(
        matches!(first, ShutdownWaitOutcome::TimedOut(_)),
        "§22.1 / D-040: short wait must TimedOut under block_stopped, got {first:?}"
    );
    assert_eq!(owner.state(), RuntimeState::Quiescing);
    gate.release();
    let second = rt.block_on(owner.wait_stopped(Duration::from_secs(2)));
    assert!(
        matches!(second, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped after release, got {second:?}"
    );
}

/// D-040 / §22.1: parked Hang worker cannot delay synchronous admission.
#[test]
fn parked_worker_cannot_delay_synchronous_admission() {
    let limits = TransactionLimits {
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r1, _recv1) = submit(&handle, Some("hang1")).expect("first admit");
    // Give Hang exchange a moment to park a runtime worker.
    std::thread::sleep(Duration::from_millis(20));
    let h2 = handle.clone();
    let t0 = Instant::now();
    let join = std::thread::spawn(move || submit(&h2, Some("hang2")));
    let (_r2, _recv2) = join.join().unwrap().expect("second admit");
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(250),
        "synchronous admit must not wait on parked Hang worker, elapsed={elapsed:?}"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
}

/// D-040 / §22.1: start-queue full rolls back ledger, session, delivery, permits.
#[test]
fn start_queue_full_rolls_back_all_permits() {
    let hold = Arc::new(StartHoldGate::new());
    hold.hold();
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            hold_start: Some(Arc::clone(&hold)),
            // Queue of 1 fills while reservation pool still has headroom (2).
            start_queue_capacity: Some(1),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let channel = ChannelId::try_new("llm").unwrap();

    let (_r1, _recv1) = submit(&handle, Some("q1")).expect("first fills start queue");
    // Brief settle so the Start is enqueued while drain is held.
    std::thread::sleep(Duration::from_millis(10));

    let (err, rejected_rx) = submit_ports(&handle, Some("q2"));
    let err = err.expect_err("start queue full");
    assert_eq!(err.kind, AdmissionErrorKind::SpawnFailed);

    // Rollback: only the first admission's reservation remains.
    assert_eq!(started.owner.global_reservations(), 1);
    assert_eq!(started.owner.channel_reservations(&channel), 1);
    assert_eq!(started.owner.ledger_len(), 1);
    assert_rejected_silent(rejected_rx);

    // Rolled-back session is free: retry fails on the still-full queue, not
    // SessionAlreadyActive.
    let (err2, recv2) = submit_ports(&handle, Some("q2"));
    assert_eq!(
        err2.expect_err("queue still full").kind,
        AdmissionErrorKind::SpawnFailed
    );
    assert_rejected_silent(recv2);

    hold.release();
    shutdown_owner(started);
}

/// D-040 / §22.1: parked unprocessed Starts still complete on shutdown.
#[test]
fn parked_starts_reach_stopped_on_shutdown() {
    let hold = Arc::new(StartHoldGate::new());
    hold.hold();
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            hold_start: Some(Arc::clone(&hold)),
            start_queue_capacity: Some(2),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let mut receivers = Vec::new();
    for i in 0..2 {
        let (_r, recv) = submit(&handle, Some(&format!("park{i}"))).unwrap();
        receivers.push(recv);
    }
    assert_eq!(started.owner.ledger_len(), 2);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    // Keep start drain held: unprocessed Starts stay in the queue while control
    // shutdown still reaches Stopped (D-039 parked-Start proof).
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    let _ = hold;
    assert!(
        matches!(
            outcome,
            ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == 2
        ),
        "parked Starts must still get completions, got {outcome:?}"
    );
    for recv in receivers {
        let c = rt.block_on(recv.completion.recv()).unwrap();
        assert_eq!(
            c.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
        );
        // D-041: shutdown-before-Start never Sealed — not Published.
        assert_eq!(
            c.terminal_event_delivery,
            monoloop_contracts::TerminalEventDelivery::NotAttempted
        );
    }
}

/// D-010 / §22.1: barrier-controlled concurrent submit vs begin_shutdown.
///
/// Each racer ends as either silent reject (`RuntimeShuttingDown`) or fully
/// admitted into the shutdown ledger (completion observed). Stopped implies
/// zero ledger / capacity.
#[test]
fn submit_versus_shutdown_barrier_race_two_outcomes() {
    let started = start_runtime(16, 16);
    let handle = started.handle.clone();
    let n_submitters = 8usize;
    // +1 for the shutdown thread.
    let barrier = Arc::new(Barrier::new(n_submitters + 1));
    let mut joins = Vec::new();
    for i in 0..n_submitters {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        let session = format!("race-shut-{i}");
        joins.push(std::thread::spawn(move || {
            b.wait();
            submit_ports(&h, Some(&session))
        }));
    }

    let owner = started.owner;
    let shut_barrier = Arc::clone(&barrier);
    let shut_join = std::thread::spawn(move || {
        shut_barrier.wait();
        owner.begin_shutdown();
        owner
    });

    let mut admitted = 0usize;
    let mut rejected = 0usize;
    let mut receivers = Vec::new();
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(receipt) => {
                admitted += 1;
                let _ = receipt;
                receivers.push(recv);
            }
            Err(e) => {
                assert_eq!(
                    e.kind,
                    AdmissionErrorKind::RuntimeShuttingDown,
                    "only RuntimeShuttingDown (or Ok) is legal, got {e:?}"
                );
                rejected += 1;
                assert_rejected_silent(recv);
            }
        }
    }
    assert_eq!(
        admitted + rejected,
        n_submitters,
        "every racer must resolve as Ok(admit) or RuntimeShuttingDown"
    );

    let mut owner = shut_join.join().unwrap();
    // Do not assert mid-Quiescing ledger_len == admitted: Echo may drain before
    // this thread rejoins. The durable proof is Stopped completions == admitted.

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(5)));
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(
                report.completions_published as usize, admitted,
                "every admitted transaction must complete once (no force-remove ghosts)"
            );
        }
        other => panic!("expected Stopped after race, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0, "Stopped ⇒ empty ledger");
    assert_eq!(owner.global_reservations(), 0, "Stopped ⇒ zero capacity");

    for recv in receivers {
        let completion = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
        assert!(matches!(
            completion.end.kind,
            monoloop_contracts::TransactionEndKind::RuntimeShutdown
                | monoloop_contracts::TransactionEndKind::Completed
                | monoloop_contracts::TransactionEndKind::Cancelled
        ));
    }
}

/// D-010: Hang harness pins both outcomes in one interleaving.
///
/// Pre-admit a Hang worker (stays live through Quiescing), barrier-race more
/// submits against `begin_shutdown`, then one post-Quiescing submit that MUST
/// reject — so `admitted >= 1` and `rejected >= 1` are deterministic.
#[test]
fn submit_versus_shutdown_hang_barrier_both_outcomes() {
    let limits = TransactionLimits {
        max_active_transactions: 16,
        max_active_per_channel: 16,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", 16)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let (pre_receipt, pre_recv) = submit(&handle, Some("pre-hang")).expect("pre-admit Hang");
    let _ = pre_receipt;
    // Let Hang register ConnectorOwner under the supervisor.
    std::thread::sleep(Duration::from_millis(30));

    let n_submitters = 6usize;
    let barrier = Arc::new(Barrier::new(n_submitters + 1));
    let mut joins = Vec::new();
    for i in 0..n_submitters {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        let session = format!("hang-race-{i}");
        joins.push(std::thread::spawn(move || {
            b.wait();
            submit_ports(&h, Some(&session))
        }));
    }

    let owner = started.owner;
    let shut_barrier = Arc::clone(&barrier);
    let shut_join = std::thread::spawn(move || {
        shut_barrier.wait();
        owner.begin_shutdown();
        owner
    });

    let mut race_admitted = 0usize;
    let mut race_rejected = 0usize;
    let mut receivers = vec![pre_recv];
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(receipt) => {
                race_admitted += 1;
                let _ = receipt;
                receivers.push(recv);
            }
            Err(e) => {
                assert_eq!(e.kind, AdmissionErrorKind::RuntimeShuttingDown);
                race_rejected += 1;
                assert_rejected_silent(recv);
            }
        }
    }

    let mut owner = shut_join.join().unwrap();
    // Deterministic reject after Quiescing (does not rely on barrier timing).
    let (post_err, post_recv) = submit_ports(&handle, Some("post-quiesce"));
    assert_eq!(
        post_err.expect_err("must reject after Quiescing").kind,
        AdmissionErrorKind::RuntimeShuttingDown
    );
    assert_rejected_silent(post_recv);

    let admitted = race_admitted + 1; // pre-Hang
    let rejected = race_rejected + 1; // post-Quiescing
    assert!(admitted >= 1, "Hang pre-admit guarantees an admit");
    assert!(rejected >= 1, "post-Quiescing submit guarantees a reject");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(5)));
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(
                report.completions_published as usize, admitted,
                "every admitted Hang/race tx must complete once"
            );
        }
        other => panic!("expected Stopped after Hang race, got {other:?}"),
    }
    assert_eq!(owner.ledger_len(), 0);
    assert_eq!(owner.global_reservations(), 0);

    for recv in receivers {
        let completion = rt
            .block_on(recv.completion.recv())
            .expect("admitted must complete");
        assert!(
            matches!(
                completion.end.kind,
                monoloop_contracts::TransactionEndKind::RuntimeShutdown
                    | monoloop_contracts::TransactionEndKind::Cancelled
                    | monoloop_contracts::TransactionEndKind::Terminated
            ),
            "Hang shutdown path should not invent Completed, got {:?}",
            completion.end.kind
        );
    }
}

/// D-040 / §22.1: submit vs begin_shutdown — only reject or fully admit into ledger.
#[test]
fn submit_versus_begin_shutdown_two_outcomes() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let (receipt, recv_ok) = submit(&handle, Some("before")).expect("admit before shutdown");
    assert_eq!(started.owner.ledger_len(), 1);
    assert_eq!(started.owner.global_reservations(), 1);

    let mut owner = started.owner;
    owner.begin_shutdown();
    // Fully admitted: still present in the shutdown ledger until completion.
    assert_eq!(owner.ledger_len(), 1);
    let _ = receipt;

    let (err, recv_rej) = submit_ports(&handle, Some("after"));
    assert_eq!(
        err.expect_err("must reject after Quiescing").kind,
        AdmissionErrorKind::RuntimeShuttingDown
    );
    assert_rejected_silent(recv_rej);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    match outcome {
        ShutdownWaitOutcome::Stopped(report) => {
            assert_eq!(report.completions_published, 1);
        }
        other => panic!("expected Stopped with the admitted transaction, got {other:?}"),
    }
    let completion = rt
        .block_on(recv_ok.completion.recv())
        .expect("admitted must complete");
    assert!(matches!(
        completion.end.kind,
        monoloop_contracts::TransactionEndKind::RuntimeShutdown
            | monoloop_contracts::TransactionEndKind::Completed
    ));
}

/// D-040 / §22.1: concurrent duplicate SessionKey admits exactly one.
#[test]
fn duplicate_session_race_admits_exactly_one() {
    let started = start_runtime(8, 8);
    let handle = started.handle.clone();
    let n = 8;
    let barrier = Arc::new(Barrier::new(n));
    let mut joins = Vec::new();
    for _ in 0..n {
        let h = handle.clone();
        let b = Arc::clone(&barrier);
        joins.push(std::thread::spawn(move || {
            b.wait();
            submit_ports(&h, Some("race-same"))
        }));
    }
    let mut winners = 0usize;
    let mut rejected = 0usize;
    let mut winner_recv = None;
    for j in joins {
        let (res, recv) = j.join().unwrap();
        match res {
            Ok(_) => {
                winners += 1;
                winner_recv = Some(recv);
            }
            Err(e) => {
                assert_eq!(e.kind, AdmissionErrorKind::SessionAlreadyActive);
                rejected += 1;
                assert_rejected_silent(recv);
            }
        }
    }
    assert_eq!(winners, 1, "exactly one duplicate-session admit");
    assert_eq!(rejected, n - 1);
    assert_eq!(started.owner.ledger_len(), 1);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == 1),
        "one admission → one completion, got {outcome:?}"
    );
    let _ = rt.block_on(winner_recv.expect("winner").completion.recv());
}

/// D-039: unknown transaction id is NotFound — never AlreadyTerminal from a missed send.
#[test]
fn terminate_unknown_transaction_is_not_found() {
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let disp = handle.terminate(
        TransactionSelector::Transaction(TransactionId::generate()),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    assert_eq!(disp, TerminationDisposition::NotFound);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let _ = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(2)).await
    });
}

/// D-039: terminate dispositions are ledger-honest — never Full→AlreadyTerminal.
#[test]
fn terminate_after_cancel_is_ledger_honest() {
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let (receipt, recv) = submit(&handle, Some("term")).unwrap();
    let cancel = TerminationMode::Cancel {
        reason: CancellationReason {
            code: CancellationReasonCode::CallerRequested,
            detail: None,
        },
    };
    let first = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        cancel.clone(),
    );
    assert_eq!(first, TerminationDisposition::Accepted);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Wait until completion (tombstone may clear) or a short settle.
    let _ = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), recv.completion.recv()).await
    });
    let second = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        cancel,
    );
    // After terminal: AlreadyTerminal while the row remains, or NotFound once
    // the tombstone cleared. Never ControlCapacityExceeded→AlreadyTerminal lie.
    assert!(
        matches!(
            second,
            TerminationDisposition::AlreadyTerminal | TerminationDisposition::NotFound
        ),
        "expected ledger-honest AlreadyTerminal|NotFound, got {second:?}"
    );

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped after cancel+shutdown, got {outcome:?}"
    );
}

/// D-039: wait_stopped while Quiescing re-announces; TimedOut then later Stopped.
#[test]
fn wait_stopped_reannounce_while_quiescing_then_stopped() {
    let gate = Arc::new(StoppedGate::new());
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            block_stopped: Some(Arc::clone(&gate)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, _recv) = submit(&handle, Some("reannounce")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    owner.begin_shutdown();
    let first = rt.block_on(owner.wait_stopped(Duration::from_millis(20)));
    assert!(
        matches!(first, ShutdownWaitOutcome::TimedOut(_)),
        "expected TimedOut under block_stopped, got {first:?}"
    );
    gate.release();
    let second = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(
        matches!(second, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped after re-announce + release, got {second:?}"
    );
}

/// §22.4 / Law 23 / M5.4: TaskSupervisor-owned JoinOnly-style work blocks Stopped.
#[test]
fn join_only_owned_task_blocks_stopped_until_released() {
    use crate::transaction::state::RuntimeState;

    let inject = Arc::new(JoinOnlySpillInject::new());
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            inject_join_only_spill: Some(Arc::clone(&inject)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Wait until supervised JoinOnly-style task has entered park.
    let entered = rt.block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !inject.is_entered() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        inject.is_entered()
    });
    assert!(
        entered,
        "JoinOnly owned-task inject must enter before shutdown proof"
    );

    let mut owner = started.owner;
    assert!(
        owner.owned_task_count() >= 1,
        "TaskSupervisor must own JoinOnly work before begin_shutdown, owned={}",
        owner.owned_task_count()
    );
    owner.begin_shutdown();
    let mid = rt.block_on(owner.wait_stopped(Duration::from_millis(80)));
    assert!(
        matches!(mid, ShutdownWaitOutcome::TimedOut(_)),
        "JoinOnly owned task must keep Quiescing (not false Stopped), got {mid:?}"
    );
    assert_eq!(owner.state(), RuntimeState::Quiescing);
    assert!(
        owner.owned_task_count() >= 1,
        "JoinOnly must remain registered while Quiescing, owned={}",
        owner.owned_task_count()
    );

    inject.release();
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    match outcome {
        ShutdownWaitOutcome::Stopped(_) => {
            assert_eq!(owner.state(), RuntimeState::Stopped);
            assert_eq!(owner.owned_task_count(), 0);
        }
        other => panic!("expected Stopped after JoinOnly release, got {other:?}"),
    }
}

/// M6 §22.5: short wait may TimedOut while Quiescing; later wait reaches Stopped
/// with empty ledger (same shutdown ticket generation on TimedOut snapshot).
#[test]
fn m6_short_timeout_then_repeat_wait_same_generation_stopped() {
    use crate::transaction::state::RuntimeState;

    // Gate Stopped so ZERO-deadline TimedOut cannot race a fast FakeConnector drain.
    let gate = Arc::new(StoppedGate::new());
    let limits = TransactionLimits {
        max_active_transactions: 4,
        max_active_per_channel: 4,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            block_stopped: Some(Arc::clone(&gate)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 4)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r1, _recv1) = submit(&handle, Some("m6a")).unwrap();
    let (_r2, _recv2) = submit(&handle, Some("m6b")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let ticket = owner.begin_shutdown();
    // Idempotent: second begin_shutdown must not bump generation (§22.5).
    let ticket2 = owner.begin_shutdown();
    assert_eq!(ticket.generation(), ticket2.generation());
    assert_eq!(ticket.generation(), 1);

    // Zero deadline under block_stopped: must observe TimedOut while Quiescing.
    let first = rt.block_on(owner.wait_stopped(Duration::ZERO));
    let ShutdownWaitOutcome::TimedOut(snap) = first else {
        panic!("§22.5 requires short wait → TimedOut while work drains, got {first:?}");
    };
    assert_eq!(snap.generation, ticket.generation());
    assert_eq!(owner.state(), RuntimeState::Quiescing);
    // §18.2: TimedOut snapshot reports remaining work while gate holds Stopped.
    assert!(
        snap.ledger_entries > 0 || snap.owned_tasks > 0,
        "TimedOut under block_stopped must report residual ledger/tasks, got {snap:?}"
    );

    gate.release();
    let second = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    match second {
        ShutdownWaitOutcome::Stopped(_) => {
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(owner.global_reservations(), 0);
            assert_eq!(owner.state(), RuntimeState::Stopped);
        }
        other => panic!("expected Stopped after long wait, got {other:?}"),
    }
}

/// §22.5: concurrent begin_shutdown callers share one generation (CAS 0→1).
#[test]
fn m6_concurrent_begin_shutdown_same_generation() {
    let started = start_runtime(2, 2);
    let owner = Arc::new(started.owner);
    let mut handles = Vec::new();
    for _ in 0..8 {
        let o = Arc::clone(&owner);
        handles.push(std::thread::spawn(move || o.begin_shutdown().generation()));
    }
    let mut gens: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    gens.sort_unstable();
    assert!(
        gens.iter().all(|g| *g == 1),
        "all tickets must share gen 1, got {gens:?}"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Arc::into_inner fails if clones remain; we moved all thread clones out.
    let mut owner = Arc::try_unwrap(owner).unwrap_or_else(|_| panic!("owner still shared"));
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(outcome, ShutdownWaitOutcome::Stopped(_)));
}

/// §22.5: repeated TimedOut waiters observe compatible snapshots (same generation).
///
/// `wait_stopped` takes `&mut self` (thread join on Stopped), so true concurrent
/// `&mut` waiters are not an API surface; compatible Quiescing snapshots are.
#[test]
fn m6_wait_stopped_timed_out_snapshots_compatible() {
    use crate::transaction::state::RuntimeState;

    let gate = Arc::new(StoppedGate::new());
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            block_stopped: Some(Arc::clone(&gate)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let mut owner = started.owner;
    let ticket = owner.begin_shutdown();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let first = rt.block_on(owner.wait_stopped(Duration::ZERO));
    let ShutdownWaitOutcome::TimedOut(snap_a) = first else {
        panic!("expected TimedOut under block_stopped, got {first:?}");
    };
    assert_eq!(snap_a.generation, ticket.generation());
    assert_eq!(owner.state(), RuntimeState::Quiescing);

    let second = rt.block_on(owner.wait_stopped(Duration::ZERO));
    let ShutdownWaitOutcome::TimedOut(snap_b) = second else {
        panic!("second wait must TimedOut while gated, got {second:?}");
    };
    assert_eq!(
        snap_a.generation, snap_b.generation,
        "compatible TimedOut snapshots share shutdown generation"
    );

    gate.release();
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// M6 / D-004: Seal with authoritative session id replaces synthetic key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_publisher_prefers_authoritative_session_on_seal() {
    use super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TerminalEventDelivery,
        TransactionDiagnostic, TransactionEndEvent, TransactionEndKind, TransactionEventPayload,
        TransactionId, TransactionUsage,
    };
    use tokio::sync::{mpsc, oneshot};

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(16, 64 * 1024).unwrap()).unwrap();
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        Arc::new(crate::transaction::sticky_cancel::StickyCancel::new()),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    // First ordinary event invents tx-{id}.
    cmd_tx
        .send(EventPublisherCommand::Publish(Box::new(
            TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("noop", Some("x"), 64).unwrap(),
            }),
        )))
        .await
        .unwrap();
    let first = receiver.events.recv().await.expect("first event");
    let synthetic = first.session_id.clone();
    assert!(
        synthetic.as_str().starts_with("tx-") || synthetic.as_str() == "direct",
        "expected synthetic, got {}",
        synthetic.as_str()
    );

    let authoritative = SessionId::try_new("grok-session-auth").unwrap();
    let (reply_tx, reply_rx) = oneshot::channel();
    cmd_tx
        .send(EventPublisherCommand::Seal {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: Some(authoritative.clone()),
                channel_id: channel.clone(),
                kind: TransactionEndKind::Completed,
                emitted_events: 0,
                usage: TransactionUsage::default(),
                diagnostics: vec![],
            },
            reply: reply_tx,
        })
        .await
        .unwrap();
    let pub_result = reply_rx.await.unwrap();
    assert_eq!(pub_result.delivery, TerminalEventDelivery::Published);
    let ended = receiver.events.recv().await.expect("ended");
    // Envelope SessionId must match authoritative.
    assert_eq!(ended.session_id, authoritative);
    assert_ne!(ended.session_id, synthetic);
    // EndedEvent payload SessionId must match the envelope (Seal sync).
    match &ended.payload {
        TransactionEventPayload::EndedEvent(term) => {
            assert_eq!(
                term.session_id.as_ref(),
                Some(&authoritative),
                "payload session_id must match envelope"
            );
        }
        other => panic!("expected EndedEvent, got {other:?}"),
    }
    let _ = pub_task.await;
}

/// D-043 / §17 / §7.1: MCP handle published before start returns; RuntimeService
/// joins before Stopped.
#[test]
fn mcp_listener_owned_shutdown_reaches_stopped() {
    let started = start_runtime_with_mcp(2, 2, true);
    let handle = started.handle.clone();
    // §7.1: start returns only after gateway handle/addr are published.
    let addr = handle
        .mcp_local_addr()
        .expect("MCP loopback addr published before start returns");
    assert!(addr.ip().is_loopback());
    assert!(
        handle.mcp_gateway().is_some(),
        "MCP gateway handle published before start returns"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        // Handle is ready at start return; serve may need one supervisor poll.
        // Retry connect only — not a publication poll (§7.1 already asserted).
        let url = format!(
            "http://{addr}/mcp/deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
        let client = reqwest::Client::new();
        let mut resp = None;
        for _ in 0..50 {
            match client.get(&url).send().await {
                Ok(r) => {
                    resp = Some(r);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        let resp = resp.expect("HTTP to live MCP gateway");
        assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

        owner.begin_shutdown();
        let stopped = owner.wait_stopped(Duration::from_secs(3)).await;
        assert!(
            owner.mcp_local_addr().is_none(),
            "MCP addr cleared after Stopped"
        );
        assert!(
            owner.mcp_gateway().is_none(),
            "MCP handle cleared after Stopped"
        );
        stopped
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped with MCP joined, got {outcome:?}"
    );
}

fn external_agent_binding(id: &str, channel_max: usize) -> ChannelBinding {
    external_agent_binding_with_session(id, channel_max, Default::default())
}

fn hang_external_agent_binding(id: &str, channel_max: usize) -> ChannelBinding {
    let mut binding = external_agent_binding_with_session_and_connector(
        id,
        channel_max,
        Default::default(),
        FakeConnectorConfig {
            default_endpoint: FakeEndpoint::Hang,
            ..FakeConnectorConfig::default()
        },
    );
    binding.limits.max_distinct_sessions = channel_max;
    binding
}

fn external_agent_binding_with_session(
    id: &str,
    channel_max: usize,
    session_config: monoloop_connector::FakeSessionAdapterConfig,
) -> ChannelBinding {
    external_agent_binding_with_session_and_connector(
        id,
        channel_max,
        session_config,
        FakeConnectorConfig::default(),
    )
}

fn external_agent_binding_with_session_and_connector(
    id: &str,
    channel_max: usize,
    session_config: monoloop_connector::FakeSessionAdapterConfig,
    connector_config: FakeConnectorConfig,
) -> ChannelBinding {
    let d = DialectDescriptor::test_raw();
    ChannelBinding {
        id: ChannelId::try_new(id).unwrap(),
        kind: ChannelKind::ExternalAgent,
        tool_mode: ToolExecutionMode::McpGateway,
        connector_factory: Arc::new(FakeConnectorFactory::external_agent_with_connector_config(
            session_config,
            connector_config,
        )),
        encoder: Arc::new(TestTextEncoder),
        interpreter: Arc::new(DefaultInterpreterFactory::new()),
        endpoint_ref: "default".into(),
        credential_ref: None,
        defaults: ChannelDefaults::default(),
        capabilities: ChannelCapabilities {
            session_mode: SessionMode::External,
            mcp_configuration: McpConfigurationCapability::CreationOnly,
            mcp_reachability: McpReachability::SameLoopbackNamespace,
            exchange_mode: ExchangeMode::Bidirectional,
            continuation_policies: BTreeSet::from([ContinuationPolicy::CallerControlled]),
            supports_distinct_session_concurrency: true,
            input_dialect: d.clone(),
            output_dialect: d,
            option_policy: OptionPolicy::external_agent(),
        },
        limits: ChannelLimits {
            max_active_transactions: channel_max,
            ..ChannelLimits::default()
        },
    }
}

/// D-015 claim-time: ExternalAgent `session_id: None` admits, then
/// `bind_session` enforces `max_distinct_sessions` → `LimitExceeded`.
///
/// Distinct from admit-time Hang DirectLlm
/// `max_distinct_sessions_exact_admits_plus_one_rejects`: first two creates
/// claim successfully (Hang-pinned); the third admits without a SessionKey
/// then fails closed at claim with `LimitExceeded` (not `InvariantFailed`).
#[test]
fn external_agent_claim_time_distinct_sessions_plus_one_limit_exceeded() {
    let distinct_max = 2usize;
    let limits = TransactionLimits {
        max_active_transactions: 8,
        max_active_per_channel: 8,
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let mut binding = hang_external_agent_binding("agent", 8);
    binding.limits.max_distinct_sessions = distinct_max;
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            enable_mcp_listener: false,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![binding]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut held = Vec::new();
    for i in 0..distinct_max {
        let (delivery, mut recv) =
            transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
        handle
            .submit(TransactionSubmitRequest {
                channel_id: ChannelId::try_new("agent").unwrap(),
                session_id: None,
                input: user_text_input(&format!("hold-{i}")).unwrap(),
                session_config: None,
                invocation_config: InvocationConfig::default(),
                tools: vec![],
                delivery,
            })
            .unwrap_or_else(|e| panic!("create {i} must admit: {e:?}"));
        let established = rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while let Some(ev) = recv.events.recv().await {
                    if matches!(
                        ev.payload,
                        monoloop_contracts::TransactionEventPayload::SessionEstablished { .. }
                    ) {
                        return true;
                    }
                }
                false
            })
            .await
            .unwrap_or(false)
        });
        assert!(
            established,
            "create {i} must claim SessionKey before Hang holds"
        );
        held.push(recv);
    }
    assert_eq!(
        started.owner.ledger_len(),
        distinct_max,
        "claimed creates remain Hang-pinned in ledger"
    );

    // Third create: admit succeeds (no SessionKey yet); claim fails LimitExceeded.
    let (delivery, overflow) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: None,
            input: user_text_input("overflow").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect("third create must admit before claim");

    let overflow_kind = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), overflow.completion.recv())
            .await
            .expect("overflow completion timed out")
            .expect("overflow completion channel")
            .end
            .kind
    });
    assert_eq!(
        overflow_kind,
        TransactionEndKind::LimitExceeded,
        "claim-time distinct overflow must be LimitExceeded, not InvariantFailed"
    );
    // Overflow leaves the ledger after terminal cleanup; held creates remain.
    let drained = rt.block_on(async {
        for _ in 0..100 {
            if started.owner.ledger_len() == distinct_max {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    });
    assert!(
        drained,
        "overflow must leave ledger; held creates stay, got len={}",
        started.owner.ledger_len()
    );

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(10)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(_)),
        "expected Stopped, got {outcome:?}"
    );
    for recv in held {
        let _ = rt.block_on(recv.completion.recv());
    }
}

/// ExternalAgent empty-tool path: attach → open → EstablishExternal before prompt → Completed.
#[test]
fn external_agent_empty_tools_establishes_session_and_completes() {
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            enable_mcp_listener: true,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_binding("agent", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    assert!(
        started.handle.mcp_gateway().is_some(),
        "§7.1 gateway published at start"
    );
    let handle = started.handle.clone();
    let (delivery, mut recv) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let receipt = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![],
            delivery,
        })
        .expect("admit");
    let _ = receipt;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let kind = rt.block_on(async {
        let mut saw_established = false;
        let mut end_kind = None;
        while let Some(ev) = recv.events.recv().await {
            match &ev.payload {
                monoloop_contracts::TransactionEventPayload::SessionEstablished { .. } => {
                    saw_established = true;
                }
                monoloop_contracts::TransactionEventPayload::EndedEvent(term) => {
                    end_kind = Some(term.kind);
                    break;
                }
                _ => {}
            }
        }
        let completion = recv.completion.recv().await.expect("completion");
        assert!(saw_established, "SessionEstablished before end");
        assert_eq!(completion.end.kind, end_kind.unwrap());
        completion.end.kind
    });
    assert_eq!(kind, TransactionEndKind::Completed);
    let mut owner = started.owner;
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// §17: spawn Rejected (closed mailbox) fail closed with 503 (no ambient inline drive).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_mcp_owner_returns_503_when_spawn_rejected() {
    use super::mcp_request_owner::SupervisedMcpRequestOwner;
    use super::task_spawner::TransactionTaskSpawner;
    use crate::transaction::mcp::McpRequestOwner;
    use axum::body::Body;
    use axum::http::{Response, StatusCode};
    use monoloop_contracts::TransactionId;

    let (spawner, spawn_rx) = TransactionTaskSpawner::channel(1);
    drop(spawn_rx); // supervisor gone → Rejected
    let owner = SupervisedMcpRequestOwner::new(spawner);
    let resp = owner
        .run_owned(
            TransactionId::generate(),
            Box::pin(async { Response::new(Body::from("unused")) }),
        )
        .await;
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "Rejected spawn must fail closed with 503"
    );
}

/// RuntimeOwner injects SupervisedMcpRequestOwner onto the live gateway.
///
/// TaskClass::McpRequest observation is proven by
/// `mcp_http_request_registers_task_class_mcp_request` (instrumented pump).
/// This test proves StartedRuntime injection + live supervisor accept (non-503)
/// and that RuntimeService is registered before HTTP.
#[test]
fn runtime_owner_mcp_http_uses_supervised_request_owner() {
    use crate::transaction::dispatcher::TransactionToolDispatcher;
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::resolved_tools::ResolvedToolSet;
    use crate::transaction::tool_capacity::SharedToolCapacity;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        ExchangeId, JsonSchema, SessionKey, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits,
        ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionId,
    };

    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let tools = HostToolRegistry::build(vec![RegisteredTool::new(
        spec.clone(),
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    )])
    .unwrap();

    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: true,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_binding("agent", 2)]).unwrap(),
        tools,
    })
    .expect("start");
    let gw = started.handle.mcp_gateway().expect("injected gateway");
    let resolved = ResolvedToolSet::from_registered(vec![RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    )]);
    let tx = TransactionId::generate();
    let dispatcher = TransactionToolDispatcher::new(
        tx,
        SessionKey {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: SessionId::try_new("s1").unwrap(),
        },
        resolved.clone(),
        SharedToolCapacity::unlimited(),
        8,
        16,
    );
    let pending = gw
        .install_pending(tx, resolved, dispatcher, ExchangeId::generate())
        .unwrap();
    gw.activate(&pending.token).unwrap();

    let mut owner = started.owner;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let status = rt.block_on(async {
        for _ in 0..100 {
            if owner.owned_task_count() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            owner.owned_task_count() >= 1,
            "RuntimeService must be live under StartedRuntime before HTTP"
        );
        let url = format!("{}/mcp/{}", gw.base_url(), pending.token.to_hex());
        let mut last = None;
        for _ in 0..50 {
            match reqwest::Client::new().get(&url).send().await {
                Ok(r) => {
                    last = Some(r.status());
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        last.expect("HTTP through RuntimeOwner MCP path")
    });
    assert_ne!(
        status,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "live supervisor must accept McpRequest spawn (injection present)"
    );
    gw.revoke(&pending.token);
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// §17: SupervisedMcpRequestOwner registers HTTP work as TaskClass::McpRequest.
/// (Pump simulates TaskSupervisor drain; RuntimeOwner injects the same owner type.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mcp_http_request_registers_task_class_mcp_request() {
    use super::mcp_request_owner::SupervisedMcpRequestOwner;
    use super::task_spawner::TransactionTaskSpawner;
    use super::task_supervisor::{TaskClass, TaskSupervisor};
    use crate::transaction::dispatcher::TransactionToolDispatcher;
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::mcp::McpGateway;
    use crate::transaction::resolved_tools::ResolvedToolSet;
    use crate::transaction::tool_capacity::SharedToolCapacity;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        ExchangeId, JsonSchema, SessionKey, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits,
        ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionId,
    };
    use std::sync::atomic::{AtomicBool, Ordering};

    let (spawner, mut spawn_rx) = TransactionTaskSpawner::channel(16);
    let saw_mcp_request = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&saw_mcp_request);
    let pump = tokio::spawn(async move {
        let mut tasks = TaskSupervisor::new();
        while let Some(req) = spawn_rx.recv().await {
            if matches!(req.class, TaskClass::McpRequest(_)) {
                flag.store(true, Ordering::SeqCst);
            }
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        let _ = tasks.abort_and_drain().await;
    });

    let owner: Arc<dyn crate::transaction::mcp::McpRequestOwner> =
        Arc::new(SupervisedMcpRequestOwner::new(spawner));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let prepared =
        McpGateway::prepare_from_tokio_listener(listener, 8, Some(Arc::clone(&owner))).unwrap();
    let addr = prepared.local_addr();
    let handle = prepared.handle();
    let serve = tokio::spawn(prepared.serve());

    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let registered = RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    );
    let resolved = ResolvedToolSet::from_registered(vec![registered]);
    let tx = TransactionId::generate();
    let dispatcher = TransactionToolDispatcher::new(
        tx,
        SessionKey::new(
            ChannelId::try_new("agent").unwrap(),
            SessionId::try_new("s1").unwrap(),
        ),
        resolved.clone(),
        SharedToolCapacity::unlimited(),
        8,
        16,
    );
    let pending = handle
        .install_pending(tx, resolved, dispatcher, ExchangeId::generate())
        .unwrap();
    handle.activate(&pending.token).unwrap();

    let url = format!("{}/mcp/{}", handle.base_url(), pending.token.to_hex());
    let resp = reqwest::Client::new().get(&url).send().await.expect("http");
    // Unknown method / MCP protocol may 4xx/2xx; ownership is what we assert.
    let _ = resp.status();
    assert!(
        saw_mcp_request.load(Ordering::SeqCst),
        "HTTP MCP dispatch must register TaskClass::McpRequest"
    );

    handle.revoke(&pending.token);
    // Drop serve by aborting join — prepared.cancel is inside serve task.
    serve.abort();
    let _ = serve.await;
    drop(owner);
    // Close spawner by dropping pump's rx when pump exits — drop pump after abort.
    pump.abort();
    let _ = pump.await;
    let _ = addr;
}

/// Attach failure after install_pending must revoke the MCP route (no leak to shutdown).
#[test]
fn mcp_route_revoked_when_attach_fails_after_install() {
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_connector::FakeSessionAdapterConfig;
    use monoloop_contracts::{
        JsonSchema, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits, ToolName,
        ToolOutputContract, ToolSpec, ToolSuccessContract,
    };

    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let tools = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    )])
    .unwrap();

    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: true,
            transaction_limits: TransactionLimits {
                max_active_transactions: 2,
                max_active_per_channel: 2,
                transaction_deadline: Duration::from_secs(2),
                cleanup_deadline: Duration::from_millis(500),
                ..TransactionLimits::default()
            },
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_binding_with_session(
            "agent",
            2,
            FakeSessionAdapterConfig {
                reject_begin_attach: true,
                ..Default::default()
            },
        )])
        .unwrap(),
        tools,
    })
    .expect("start");
    let gw = started.handle.mcp_gateway().expect("mcp gateway");
    let handle = started.handle.clone();
    let (delivery, mut recv) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let _ = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let kind = rt.block_on(async {
        let completion = recv.completion.recv().await.expect("completion");
        while let Some(_ev) = recv.events.recv().await {}
        completion.end.kind
    });
    assert_eq!(kind, TransactionEndKind::InvariantFailed);
    assert_eq!(
        gw.routes().len(),
        0,
        "MCP route must be revoked when attach fails after install"
    );
    let mut owner = started.owner;
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// D-026 / LAW 7: provisional MCP dispatcher SessionKey is rebound before activate.
#[test]
fn mcp_dispatcher_rebind_session_before_activate() {
    use super::session_identity::session_key_for;
    use crate::transaction::dispatcher::TransactionToolDispatcher;
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::mcp::McpGateway;
    use crate::transaction::resolved_tools::ResolvedToolSet;
    use crate::transaction::tool_capacity::SharedToolCapacity;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        ExchangeId, JsonSchema, SessionKey, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits,
        ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract, TransactionId,
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        let spec = ToolSpec::try_new(
            ToolId::try_new("echo").unwrap(),
            ToolName::try_new("echo").unwrap(),
            "echo",
            schema,
            ToolOutputContract {
                success: ToolSuccessContract::json(out),
                error_data_schema: None,
            },
            ToolLimits {
                max_concurrent: 1,
                max_input_bytes: 256,
                max_output_bytes: 256,
                execution_deadline: Duration::from_secs(1),
            },
            ToolExecutionClass::CooperativeInProcess {
                grace: Duration::from_millis(50),
            },
        )
        .unwrap();
        let registered = RegisteredTool::new(
            spec,
            Arc::new(ImmediateToolHandler::new(|_c, _x| {
                Ok(ToolCompletion::Succeeded(
                    monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
                ))
            })),
        );
        let resolved = ResolvedToolSet::from_registered(vec![registered]);
        let tx = TransactionId::generate();
        let provisional = session_key_for(ChannelId::try_new("agent").unwrap(), None, tx);
        let dispatcher = TransactionToolDispatcher::new(
            tx,
            provisional.clone(),
            resolved.clone(),
            SharedToolCapacity::unlimited(),
            8,
            16,
        );
        assert_eq!(dispatcher.session_key(), provisional);
        assert!(
            provisional.session_id.as_str().starts_with("tx-"),
            "provisional key is transaction-scoped"
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback bind");
        let prepared = McpGateway::prepare_from_tokio_listener(listener, 8, None).unwrap();
        let gw = prepared.handle();
        let cancel = prepared.cancel_token();
        let join = tokio::spawn(prepared.serve());
        let pending = gw
            .install_pending(
                tx,
                resolved,
                Arc::clone(&dispatcher),
                ExchangeId::generate(),
            )
            .unwrap();
        let claimed = SessionKey {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: SessionId::try_new("fake-created-authoritative").unwrap(),
        };
        // Coordinator must rebind before activate (D-026).
        pending.dispatcher.rebind_session(claimed.clone());
        assert_eq!(pending.dispatcher.session_key(), claimed);
        assert_ne!(pending.dispatcher.session_key(), provisional);
        gw.activate(&pending.token).unwrap();
        gw.revoke(&pending.token);
        gw.revoke_all_services();
        cancel.cancel();
        let _ = join.await;
    });
}

/// D-014: CreationOnly rejects tool-enabled existing-session reuse at admission.
#[test]
fn creation_only_tool_reuse_rejected_at_admission() {
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        JsonSchema, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits, ToolName,
        ToolOutputContract, ToolSpec, ToolSuccessContract,
    };

    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let tools = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    )])
    .unwrap();

    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            enable_mcp_listener: true,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_binding("agent", 2)]).unwrap(),
        tools,
    })
    .expect("start");
    let handle = started.handle.clone();
    let (delivery, _recv) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: Some(SessionId::try_new("existing-session").unwrap()),
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect_err("CreationOnly tool reuse must fail at admission");
    assert_eq!(err.kind, AdmissionErrorKind::CapabilityMismatch);
    let mut owner = started.owner;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// CreationOnly: non-empty tools install pending MCP, activate before prompt, revoke after.
#[test]
fn creation_only_mcp_install_activate_revoke_round_trip() {
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        JsonSchema, ToolCompletion, ToolExecutionClass, ToolId, ToolLimits, ToolName,
        ToolOutputContract, ToolSpec, ToolSuccessContract,
    };

    let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let out = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("echo").unwrap(),
        ToolName::try_new("echo").unwrap(),
        "echo",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 1,
            max_input_bytes: 256,
            max_output_bytes: 256,
            execution_deadline: Duration::from_secs(1),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let tools = HostToolRegistry::build(vec![RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({})),
            ))
        })),
    )])
    .unwrap();

    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            enable_mcp_listener: true,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![external_agent_binding("agent", 2)]).unwrap(),
        tools,
    })
    .expect("start");
    let gw = started.handle.mcp_gateway().expect("mcp gateway");
    let handle = started.handle.clone();
    let (delivery, mut recv) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let receipt = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("agent").unwrap(),
            session_id: None,
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: InvocationConfig::default(),
            tools: vec![ToolId::try_new("echo").unwrap()],
            delivery,
        })
        .expect("admit with tools");
    let _ = receipt;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let kind = rt.block_on(async {
        // Route may be live briefly during the turn; wait for terminal.
        let mut end_kind = None;
        while let Some(ev) = recv.events.recv().await {
            if let monoloop_contracts::TransactionEventPayload::EndedEvent(term) = &ev.payload {
                end_kind = Some(term.kind);
                break;
            }
        }
        let _ = recv.completion.recv().await;
        end_kind.expect("ended")
    });
    assert_eq!(kind, TransactionEndKind::Completed);
    // After coordinator revoke, route table must be empty.
    assert_eq!(gw.routes().len(), 0, "MCP route revoked after terminal");
    let mut owner = started.owner;
    let stopped = rt.block_on(owner.wait_stopped(Duration::from_secs(3)));
    assert!(matches!(stopped, ShutdownWaitOutcome::Stopped(_)));
}

/// §22.2: shutdown between Seal and completion send cannot lose ledger/completion.
#[test]
fn s22_2_shutdown_between_seal_and_completion_keeps_completion() {
    use crate::transaction::state::RuntimeState;

    let hold = Arc::new(FinalizerHoldGate::new());
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(5),
        cleanup_deadline: Duration::from_millis(200),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            hold_finalizer_after_seal: Some(Arc::clone(&hold)),
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, mut recv) = submit(&handle, Some("seal-hold")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    // Wait until Seal publishes EndedEvent while Finalizer is held before completion.
    let saw_ended = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(ev) = recv.events.recv().await {
                if matches!(
                    ev.payload,
                    monoloop_contracts::TransactionEventPayload::EndedEvent(_)
                ) {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false)
    });
    assert!(saw_ended, "Seal must publish Ended before completion hold");

    // Shutdown while Finalizer is still between Seal and completion send.
    let mut owner = started.owner;
    owner.begin_shutdown();
    let mid = rt.block_on(owner.wait_stopped(Duration::from_millis(100)));
    assert!(
        matches!(mid, ShutdownWaitOutcome::TimedOut(_)),
        "held Finalizer must keep Quiescing (not false Stopped), got {mid:?}"
    );
    assert_eq!(owner.state(), RuntimeState::Quiescing);

    // Release completion publish; hard-grace must not have dropped the attempt.
    hold.release();
    let outcome = rt.block_on(owner.wait_stopped(Duration::from_secs(5)));
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => {
            assert_eq!(
                r.completions_published, 1,
                "Seal→completion must not lose the one completion attempt"
            );
            assert_eq!(r.completions_invariant_failed, 0);
        }
        other => panic!("expected Stopped, got {other:?}"),
    }
    let completion = rt
        .block_on(recv.completion.recv())
        .expect("completion must arrive after Finalizer release");
    assert!(matches!(
        completion.end.kind,
        TransactionEndKind::Completed | TransactionEndKind::RuntimeShutdown
    ));
}

/// §22.3: spawn registers in the supervisor before the user future can first-poll.
#[tokio::test(flavor = "current_thread")]
async fn s22_3_spawn_registers_before_first_poll() {
    use super::task_supervisor::{TaskClass, TaskSupervisor};
    use std::sync::atomic::{AtomicBool, Ordering};

    let tx = TransactionId::generate();
    let entered = Arc::new(AtomicBool::new(false));
    let (block_tx, block_rx) = tokio::sync::oneshot::channel::<()>();
    let entered_task = Arc::clone(&entered);

    let mut tasks = TaskSupervisor::new();
    let id = tasks.spawn(TaskClass::TransactionCoordinator(tx), async move {
        // First action of the user future — must run only after registration.
        entered_task.store(true, Ordering::SeqCst);
        let _ = block_rx.await;
    });

    // Registration is synchronous in `spawn` before the start-gate release.
    assert_eq!(tasks.registered_count(), 1);
    assert_eq!(tasks.tasks_for(&tx), vec![id]);

    // Allow one poll; user future may now set `entered`.
    tokio::task::yield_now().await;
    assert!(
        entered.load(Ordering::SeqCst),
        "start-gate must release so the registered future can poll"
    );
    assert_eq!(
        tasks.registered_count(),
        1,
        "registration must outlive first poll"
    );

    let _ = block_tx.send(());
    let finished = tasks.join_next().await.expect("join");
    assert_eq!(finished.0, id);
    assert_eq!(tasks.registered_count(), 0);
}

/// §22.3: abort is followed by an observed join before stopped proof.
#[tokio::test(flavor = "current_thread")]
async fn s22_3_abort_then_observed_join() {
    use super::task_supervisor::{TaskClass, TaskExit, TaskSupervisor};

    let tx = TransactionId::generate();
    let mut tasks = TaskSupervisor::new();
    let id = tasks.spawn(TaskClass::EventPublisher(tx), async {
        std::future::pending::<()>().await;
    });
    assert_eq!(tasks.registered_count(), 1);

    tasks.abort(id);
    let (joined_id, class, exit) = tasks.join_next().await.expect("join after abort");
    assert_eq!(joined_id, id);
    assert!(matches!(class, TaskClass::EventPublisher(_)));
    assert_eq!(exit, TaskExit::Cancelled);
    assert!(tasks.is_empty());
}

/// §22.3: a yielding abortable future is aborted and joined.
#[tokio::test(flavor = "current_thread")]
async fn s22_3_yielding_abortable_aborted_and_joined() {
    use super::task_supervisor::{TaskClass, TaskExit, TaskSupervisor};

    let tx = TransactionId::generate();
    let mut tasks = TaskSupervisor::new();
    let id = tasks.spawn(TaskClass::LoopRuntime(tx), async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    // Let it yield at least once.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    tasks.abort(id);
    let (joined_id, _, exit) = tasks.join_next().await.expect("join yielding abort");
    assert_eq!(joined_id, id);
    assert_eq!(exit, TaskExit::Cancelled);
    assert_eq!(tasks.registered_count(), 0);
}

/// §22.3: abort_and_drain observes joins; counts return to zero (non-kill path).
#[tokio::test(flavor = "current_thread")]
async fn s22_3_abort_and_drain_counts_to_zero() {
    use super::task_supervisor::{TaskClass, TaskSupervisor};

    let tx = TransactionId::generate();
    let mut tasks = TaskSupervisor::new();
    for _ in 0..3 {
        tasks.spawn(
            TaskClass::ConnectorOwner(tx, monoloop_contracts::ExchangeId::generate()),
            async {
                std::future::pending::<()>().await;
            },
        );
    }
    assert_eq!(tasks.registered_count(), 3);
    assert!(tasks.abort_and_drain().await);
    assert!(tasks.is_empty());
    assert_eq!(tasks.registered_count(), 0);
    assert!(tasks.tasks_for(&tx).is_empty());
}

/// §22.3: runtime shutdown (normal path) leaves owned task count at zero.
#[test]
fn s22_3_runtime_normal_path_counts_to_zero() {
    use crate::transaction::state::RuntimeState;

    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let (_r, recv) = submit(&handle, Some("own-ok")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(3), recv.completion.recv()).await
        })
        .expect("completion timeout")
        .expect("completion");
    assert!(matches!(
        completion.end.kind,
        TransactionEndKind::Completed | TransactionEndKind::RuntimeShutdown
    ));

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => {
            assert_eq!(r.completions_published, 1);
            assert_eq!(owner.state(), RuntimeState::Stopped);
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(owner.owned_task_count(), 0);
        }
        other => panic!("expected Stopped with zero owned tasks, got {other:?}"),
    }
}

/// §22.3: cancel path still drains owned tasks to zero before Stopped.
#[test]
fn s22_3_runtime_cancel_path_counts_to_zero() {
    use crate::transaction::state::RuntimeState;

    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let (receipt, recv) = submit(&handle, Some("own-cancel")).unwrap();
    let disp = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    assert!(matches!(
        disp,
        TerminationDisposition::Accepted | TerminationDisposition::AlreadyTerminal
    ));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _ = rt.block_on(recv.completion.recv());

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(_) => {
            assert_eq!(owner.state(), RuntimeState::Stopped);
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(owner.owned_task_count(), 0);
        }
        other => panic!("expected Stopped after cancel drain, got {other:?}"),
    }
}

/// §22.3: coordinator panic / failure path still reaches zero owned tasks.
#[test]
fn s22_3_runtime_failure_path_counts_to_zero() {
    use crate::transaction::state::RuntimeState;

    // PanicEncoder forces coordinator failure → InvariantFailed completion.
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let mut binding = llm_binding("llm", 2);
    binding.encoder = Arc::new(PanicEncoder);
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![binding]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, recv) = submit(&handle, Some("own-fail")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(recv.completion.recv()).expect("completion");
    assert_eq!(completion.end.kind, TransactionEndKind::InvariantFailed);

    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(_) => {
            assert_eq!(owner.state(), RuntimeState::Stopped);
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(owner.owned_task_count(), 0);
        }
        other => panic!("expected Stopped after failure drain, got {other:?}"),
    }
}

/// §22.3: Connector/Interpreter pumps stay supervisor-owned (joined) — shutdown
/// after a live exchange does not leave detached pump tasks.
#[test]
fn s22_3_exchange_pumps_joined_not_detached() {
    // Hang keeps ConnectorOwner parked under TaskSupervisor while we shut down.
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, _recv) = submit(&handle, Some("pump-own")).unwrap();
    // Give Hang exchange time to register ConnectorOwner under the supervisor.
    std::thread::sleep(Duration::from_millis(50));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(_) => {
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(
                owner.owned_task_count(),
                0,
                "Connector/Interpreter pumps must be joined, not detached"
            );
        }
        other => panic!("expected Stopped with joined pumps, got {other:?}"),
    }
    let _ = handle;
}

/// §22.2: every admission → exactly one completion send attempt.
#[test]
fn s22_2_one_completion_per_admission() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let mut receivers = Vec::new();
    for i in 0..3 {
        let (_r, recv) = submit(&handle, Some(&format!("c{i}"))).unwrap();
        receivers.push(recv);
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => {
            assert_eq!(r.completions_published, 3);
            assert_eq!(r.completions_invariant_failed, 0);
        }
        other => panic!("expected Stopped, got {other:?}"),
    }
    for recv in receivers {
        let _ = rt.block_on(recv.completion.recv()).expect("completion");
    }
}

/// §22.2: receiver dropped before completion is accounted without a task leak.
#[test]
fn s22_2_dropped_completion_receiver_accounted() {
    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let (_r, recv) = submit(&handle, Some("drop-c")).unwrap();
    drop(recv.completion); // host drops before completion
    let mut events = recv.events;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => {
            assert_eq!(r.completions_published, 1);
            assert_eq!(r.completions_receiver_dropped, 1);
            assert_eq!(owner.ledger_len(), 0);
            assert_eq!(owner.global_reservations(), 0);
        }
        other => panic!("expected Stopped after dropped receiver, got {other:?}"),
    }
    let _ = events.try_recv();
}

/// §22.2: coordinator panic → one InvariantFailed completion.
#[test]
fn s22_2_coordinator_panic_one_invariant_failed() {
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(5),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let mut binding = llm_binding("llm", 2);
    binding.encoder = Arc::new(PanicEncoder);
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![binding]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (_r, recv) = submit(&handle, Some("panic")).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), recv.completion.recv())
            .await
            .expect("completion timeout")
            .expect("completion")
    });
    assert_eq!(completion.end.kind, TransactionEndKind::InvariantFailed);
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => {
            assert_eq!(r.completions_published, 1);
        }
        other => panic!("expected Stopped, got {other:?}"),
    }
}

/// §22.2: cancel then force-terminate → one Terminated completion.
#[test]
fn s22_2_cancel_upgraded_to_force_terminate() {
    // Hang so cancel/force race before natural Completed.
    let limits = TransactionLimits {
        max_active_transactions: 2,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(30),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![hang_llm_binding("llm", 2)]).unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();
    let (receipt, recv) = submit(&handle, Some("force")).unwrap();
    std::thread::sleep(Duration::from_millis(30));
    let _ = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    let _ = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::ForceTerminate {
            reason: TerminationReason {
                code: TerminationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), recv.completion.recv())
            .await
            .expect("completion timeout")
            .expect("completion")
    });
    assert_eq!(completion.end.kind, TransactionEndKind::Terminated);
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => assert_eq!(r.completions_published, 1),
        other => panic!("expected Stopped, got {other:?}"),
    }
}

/// §22.2: coordinator completion racing cancel → exactly one documented cause.
#[test]
fn s22_2_completion_racing_cancel_one_cause() {
    let started = start_runtime(4, 4);
    let handle = started.handle.clone();
    let (receipt, recv) = submit(&handle, Some("race-c")).unwrap();
    // Immediate cancel while echo may already be finishing.
    let _ = handle.terminate(
        TransactionSelector::Transaction(receipt.transaction_id),
        TerminationMode::Cancel {
            reason: CancellationReason {
                code: CancellationReasonCode::CallerRequested,
                detail: None,
            },
        },
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let completion = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), recv.completion.recv())
            .await
            .expect("completion timeout")
            .expect("completion")
    });
    assert!(
        matches!(
            completion.end.kind,
            TransactionEndKind::Cancelled | TransactionEndKind::Completed
        ),
        "one documented cause, got {:?}",
        completion.end.kind
    );
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(3)).await
    });
    match outcome {
        ShutdownWaitOutcome::Stopped(r) => assert_eq!(r.completions_published, 1),
        other => panic!("expected Stopped, got {other:?}"),
    }
}

/// §22.2: no ordinary event is published after Seal / terminal attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_2_no_event_after_terminal_attempt() {
    use super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TerminalEventDelivery,
        TransactionDiagnostic, TransactionEndEvent, TransactionEndKind, TransactionEventPayload,
        TransactionId, TransactionUsage,
    };
    use tokio::sync::{mpsc, oneshot};

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(16, 64 * 1024).unwrap()).unwrap();
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        Arc::new(crate::transaction::sticky_cancel::StickyCancel::new()),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    let (reply_tx, reply_rx) = oneshot::channel();
    cmd_tx
        .send(EventPublisherCommand::Seal {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: Some(SessionId::try_new("s").unwrap()),
                channel_id: channel.clone(),
                kind: TransactionEndKind::Completed,
                emitted_events: 0,
                usage: TransactionUsage::default(),
                diagnostics: vec![],
            },
            reply: reply_tx,
        })
        .await
        .unwrap();
    let seal = reply_rx.await.unwrap();
    assert_eq!(seal.delivery, TerminalEventDelivery::Published);
    let ended = receiver.events.recv().await.expect("ended");
    assert!(matches!(
        ended.payload,
        TransactionEventPayload::EndedEvent(_)
    ));

    // Post-Seal Publish must be ignored (no further events).
    let _ = cmd_tx
        .send(EventPublisherCommand::Publish(Box::new(
            TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("late", Some("x"), 64).unwrap(),
            }),
        )))
        .await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        receiver.events.try_recv().is_err(),
        "no event after terminal attempt"
    );
    drop(cmd_tx);
    let _ = pub_task.await;
}

/// D-047 / §22.2: full mailbox waits; drain preserves contiguous sequences and
/// payload count (no silent drop).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_2_failed_enqueue_consumes_no_sequence() {
    use super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TransactionDiagnostic,
        TransactionEventPayload, TransactionId,
    };
    use tokio::sync::mpsc;

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(1, 64 * 1024).unwrap()).unwrap();
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        Arc::new(crate::transaction::sticky_cancel::StickyCancel::new()),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    let diag = |tag: &str| {
        TransactionEventPayload::Diagnostic(TransactionDiagnostic {
            diagnostic: SafeDiagnostic::try_new("noop", Some(tag), 64).unwrap(),
        })
    };
    cmd_tx
        .send(EventPublisherCommand::Publish(Box::new(diag("a"))))
        .await
        .unwrap();
    // Fill capacity 1 without draining — second publish waits (D-047), does not drop.
    cmd_tx
        .send(EventPublisherCommand::Publish(Box::new(diag("b"))))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let first = receiver.events.recv().await.expect("first");
    assert_eq!(first.sequence, 1);
    let second = receiver.events.recv().await.expect("second after wait");
    assert_eq!(second.sequence, 2);
    cmd_tx
        .send(EventPublisherCommand::Publish(Box::new(diag("c"))))
        .await
        .unwrap();
    let third = receiver.events.recv().await.expect("third");
    assert_eq!(third.sequence, 3, "contiguous after waited enqueue");
    drop(cmd_tx);
    let _ = pub_task.await;
}

/// D-047: permanently full host queue → sticky DeadlineExceeded on Seal (not
/// Published / Completed).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn d047_full_queue_seal_reports_deadline_not_published() {
    use super::event_publisher::{run_event_publisher, EventPublisherCommand, TerminalPublicationResult};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TerminalEventDelivery,
        TransactionDiagnostic, TransactionEndEvent, TransactionEndKind, TransactionEventPayload,
        TransactionId, TransactionUsage,
    };
    use tokio::sync::{mpsc, oneshot};

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, _receiver) =
        transaction_delivery(DeliveryLimits::try_new(1, 64 * 1024).unwrap()).unwrap();
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let cancel = Arc::new(crate::transaction::sticky_cancel::StickyCancel::new());
    // Short deadline so the waiting second publish fails closed.
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        Arc::clone(&cancel),
        std::time::Instant::now() + Duration::from_millis(80),
    ));

    let diag = || {
        TransactionEventPayload::Diagnostic(TransactionDiagnostic {
            diagnostic: SafeDiagnostic::try_new("noop", Some("x"), 64).unwrap(),
        })
    };
    cmd_tx
        .send(EventPublisherCommand::Publish(Box::new(diag())))
        .await
        .unwrap();
    // Do not drain — second publish waits until deadline.
    cmd_tx
        .send(EventPublisherCommand::Publish(Box::new(diag())))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    let (reply_tx, reply_rx) = oneshot::channel();
    cmd_tx
        .send(EventPublisherCommand::Seal {
            terminal: TransactionEndEvent {
                transaction_id: tx_id,
                session_id: None,
                channel_id: channel,
                kind: TransactionEndKind::Completed,
                emitted_events: 0,
                usage: TransactionUsage::default(),
                diagnostics: vec![],
            },
            reply: reply_tx,
        })
        .await
        .unwrap();
    let res: TerminalPublicationResult = reply_rx.await.expect("seal reply");
    assert_eq!(
        res.delivery,
        TerminalEventDelivery::DeadlineExceeded,
        "sticky wait failure must surface on Seal, got {:?}",
        res.delivery
    );
    assert_eq!(res.last_sequence, 1, "only the first event committed");
    drop(cmd_tx);
    let _ = pub_task.await;
}

/// §22.6: SessionEstablished is sequence 1 for new external sessions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_6_session_established_is_sequence_one() {
    use super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, ExternalSessionId, SafeDiagnostic,
        TransactionDiagnostic, TransactionEventPayload, TransactionId,
    };
    use tokio::sync::mpsc;

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(16, 64 * 1024).unwrap()).unwrap();
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel.clone(),
        None,
        delivery.event_tx,
        cmd_rx,
        Arc::new(crate::transaction::sticky_cancel::StickyCancel::new()),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    let external = ExternalSessionId::try_new("grok-ext-1").unwrap();
    cmd_tx
        .send(EventPublisherCommand::EstablishExternal(external.clone()))
        .await
        .unwrap();
    let first = receiver.events.recv().await.expect("session established");
    assert_eq!(first.sequence, 1);
    match &first.payload {
        TransactionEventPayload::SessionEstablished {
            external_session_id,
        } => {
            assert_eq!(external_session_id.as_str(), external.as_str());
        }
        other => panic!("expected SessionEstablished, got {other:?}"),
    }

    cmd_tx
        .send(EventPublisherCommand::Publish(Box::new(
            TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("noop", Some("x"), 64).unwrap(),
            }),
        )))
        .await
        .unwrap();
    let second = receiver
        .events
        .recv()
        .await
        .expect("ordinary after establish");
    assert_eq!(second.sequence, 2);
    assert!(matches!(
        second.payload,
        TransactionEventPayload::Diagnostic(_)
    ));
    drop(cmd_tx);
    let _ = pub_task.await;
}

/// §22.6: concurrent producers through one publisher stay contiguous 1..N.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s22_6_concurrent_producers_contiguous_sequence() {
    use super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TransactionDiagnostic,
        TransactionEventPayload, TransactionId,
    };
    use tokio::sync::mpsc;

    let n = 32usize;
    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(n + 8, 1024 * 1024).unwrap()).unwrap();
    let (cmd_tx, cmd_rx) = mpsc::channel(n);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel,
        None,
        delivery.event_tx,
        cmd_rx,
        Arc::new(crate::transaction::sticky_cancel::StickyCancel::new()),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    let mut joins = Vec::new();
    for i in 0..n {
        let tx = cmd_tx.clone();
        joins.push(tokio::spawn(async move {
            tx.send(EventPublisherCommand::Publish(Box::new(
                TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                    diagnostic: SafeDiagnostic::try_new("noop", Some(&format!("p{i}")), 64)
                        .unwrap(),
                }),
            )))
            .await
            .unwrap();
        }));
    }
    for j in joins {
        j.await.unwrap();
    }
    drop(cmd_tx);

    let mut seqs = Vec::new();
    while let Some(ev) = receiver.events.recv().await {
        seqs.push(ev.sequence);
    }
    let _ = pub_task.await;
    assert_eq!(seqs.len(), n, "all publishes delivered, got {seqs:?}");
    let expected: Vec<u64> = (1..=n as u64).collect();
    let mut got = seqs.clone();
    got.sort_unstable();
    assert_eq!(got, expected, "contiguous 1..N allocated");
    // Delivery order matches allocation order (single publisher serializes).
    assert_eq!(seqs, expected, "delivery order must match sequence order");
}

/// §22.6: same session string on different Channels remains isolated.
#[test]
fn s22_6_same_session_string_different_channels_isolated() {
    let limits = TransactionLimits {
        max_active_transactions: 4,
        max_active_per_channel: 2,
        transaction_deadline: Duration::from_secs(2),
        cleanup_deadline: Duration::from_millis(500),
        ..TransactionLimits::default()
    };
    let started = StartedRuntime::start(RuntimeBootstrap {
        config: RuntimeConfig {
            transaction_limits: limits,
            ..RuntimeConfig::default()
        },
        channels: ChannelRegistry::build(vec![llm_binding("llm-a", 2), llm_binding("llm-b", 2)])
            .unwrap(),
        tools: HostToolRegistry::empty(),
    })
    .expect("start");
    let handle = started.handle.clone();

    let (r_a, recv_a) = submit_on(&handle, "llm-a", Some("shared-sid")).expect("a");
    let (r_b, recv_b) = submit_on(&handle, "llm-b", Some("shared-sid")).expect("b");
    assert_ne!(r_a.transaction_id, r_b.transaction_id);
    // Same session string is isolated by ChannelId inside SessionKey.
    assert_eq!(
        r_a.session_id.as_ref().map(|s| s.as_str()),
        Some("shared-sid")
    );
    assert_eq!(
        r_b.session_id.as_ref().map(|s| s.as_str()),
        Some("shared-sid")
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = started.owner;
    let outcome = rt.block_on(async {
        owner.begin_shutdown();
        owner.wait_stopped(Duration::from_secs(5)).await
    });
    assert!(
        matches!(outcome, ShutdownWaitOutcome::Stopped(ref r) if r.completions_published == 2),
        "both channel-isolated admissions complete, got {outcome:?}"
    );
    let _ = rt.block_on(recv_a.completion.recv());
    let _ = rt.block_on(recv_b.completion.recv());
}

/// §22.6: reused provider tool-call ids across exchanges stay distinct via helper.
#[test]
fn s22_6_reused_provider_tool_call_ids_across_exchanges_distinct() {
    use super::session_identity::tool_action_id_for_exchange;

    // Provider may reuse the same tool_call id string across exchanges; Monoloop
    // correlates with exchange-scoped ToolActionId (production helper).
    let provider_reuse = "call_abc";
    let exchange_a = monoloop_contracts::ExchangeId::generate();
    let exchange_b = monoloop_contracts::ExchangeId::generate();
    let action_a = tool_action_id_for_exchange(exchange_a, provider_reuse);
    let action_b = tool_action_id_for_exchange(exchange_b, provider_reuse);
    assert_ne!(
        action_a.as_str(),
        action_b.as_str(),
        "same provider id on different exchanges must remain distinct"
    );
    assert!(action_a.as_str().contains(provider_reuse));
    assert!(action_b.as_str().contains(provider_reuse));
}

/// §22.6: failed EstablishExternal must not mutate identity or lose seq 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s22_6_establish_external_capacity_fail_does_not_steal_seq1() {
    use super::event_publisher::{run_event_publisher, EventPublisherCommand};
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, ExternalSessionId, SafeDiagnostic,
        TransactionDiagnostic, TransactionEvent, TransactionEventPayload, TransactionId,
    };
    use tokio::sync::mpsc;

    let tx_id = TransactionId::generate();
    let channel = ChannelId::try_new("llm").unwrap();
    let (delivery, mut receiver) =
        transaction_delivery(DeliveryLimits::try_new(1, 64 * 1024).unwrap()).unwrap();
    // Pre-fill mailbox so EstablishExternal's try_send fails while next_seq is still 1.
    let filler = delivery.event_tx.clone();
    filler
        .try_send(TransactionEvent {
            transaction_id: tx_id,
            channel_id: channel.clone(),
            session_id: SessionId::try_new("prefill").unwrap(),
            sequence: 0,
            payload: TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("noop", Some("prefill"), 64).unwrap(),
            }),
        })
        .unwrap();

    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let pub_task = tokio::spawn(run_event_publisher(
        tx_id,
        channel,
        None,
        delivery.event_tx,
        cmd_rx,
        Arc::new(crate::transaction::sticky_cancel::StickyCancel::new()),
        std::time::Instant::now() + Duration::from_secs(30),
    ));

    let external = ExternalSessionId::try_new("grok-retry").unwrap();
    cmd_tx
        .send(EventPublisherCommand::EstablishExternal(external.clone()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Drain prefill; Establish must still be able to claim seq 1 on retry.
    let pre = receiver.events.recv().await.expect("prefill");
    assert_eq!(pre.sequence, 0);

    cmd_tx
        .send(EventPublisherCommand::EstablishExternal(external.clone()))
        .await
        .unwrap();
    let first = receiver.events.recv().await.expect("session established");
    assert_eq!(first.sequence, 1);
    assert!(matches!(
        first.payload,
        TransactionEventPayload::SessionEstablished { .. }
    ));

    cmd_tx
        .send(EventPublisherCommand::Publish(Box::new(
            TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: SafeDiagnostic::try_new("noop", Some("after"), 64).unwrap(),
            }),
        )))
        .await
        .unwrap();
    let second = receiver.events.recv().await.expect("ordinary");
    assert_eq!(second.sequence, 2);
    drop(cmd_tx);
    let _ = pub_task.await;
}

/// §22.6: event item plus-one fails closed.
#[test]
fn s22_6_event_item_plus_one_fails_closed() {
    use monoloop_contracts::{
        transaction_delivery, DeliveryLimits, SafeDiagnostic, TransactionDiagnostic,
        TransactionEvent, TransactionEventPayload, TransactionId,
    };

    let (delivery, _recv) =
        transaction_delivery(DeliveryLimits::try_new(1, 1024 * 1024).unwrap()).unwrap();
    let tx_id = TransactionId::generate();
    let ev = || TransactionEvent {
        transaction_id: tx_id,
        channel_id: ChannelId::try_new("ch").unwrap(),
        session_id: SessionId::try_new("s").unwrap(),
        sequence: 1,
        payload: TransactionEventPayload::Diagnostic(TransactionDiagnostic {
            diagnostic: SafeDiagnostic::try_new("noop", Some("x"), 64).unwrap(),
        }),
    };
    delivery.event_tx.try_send(ev()).unwrap();
    let err = delivery.event_tx.try_send(ev()).unwrap_err();
    assert_eq!(
        err,
        monoloop_contracts::EventEnqueueError::ItemCapacityExceeded
    );
}

/// §22.6: event byte plus-one fails closed.
#[test]
fn s22_6_event_byte_plus_one_fails_closed() {
    use monoloop_contracts::{
        estimate_event_bytes, transaction_delivery, DeliveryLimits, SafeDiagnostic,
        TransactionDiagnostic, TransactionEvent, TransactionEventPayload, TransactionId,
    };

    let tx_id = TransactionId::generate();
    let sample = TransactionEvent {
        transaction_id: tx_id,
        channel_id: ChannelId::try_new("ch").unwrap(),
        session_id: SessionId::try_new("s").unwrap(),
        sequence: 1,
        payload: TransactionEventPayload::Diagnostic(TransactionDiagnostic {
            diagnostic: SafeDiagnostic::try_new("noop", Some("payload-bytes"), 256).unwrap(),
        }),
    };
    let nbytes = estimate_event_bytes(&sample);
    // Budget exactly one event's bytes — second enqueue must fail closed.
    let (delivery, _recv) =
        transaction_delivery(DeliveryLimits::try_new(8, nbytes).unwrap()).unwrap();
    delivery.event_tx.try_send(sample.clone()).unwrap();
    let err = delivery.event_tx.try_send(sample).unwrap_err();
    assert_eq!(
        err,
        monoloop_contracts::EventEnqueueError::ByteCapacityExceeded
    );
}

fn submit_ports_on(
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

fn submit_on(
    handle: &TransactionRuntimeHandle,
    channel: &str,
    session: Option<&str>,
) -> Result<(AdmissionReceipt, TransactionReceiver), AdmissionError> {
    let (result, receiver) = submit_ports_on(handle, channel, session);
    result.map(|receipt| (receipt, receiver))
}

#[test]
fn needs_loop_dispatch_ready_only() {
    use super::loop_dispatch::needs_loop_dispatch;
    use monoloop_contracts::{
        CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, FlowId,
        InterpretationId, LaneId, ToolActionEvent, ToolActionId, ToolExecutionState,
        ToolRequestState, ToolResultState, UnitId, UnitState,
    };

    let ready = CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
        unit_id: UnitId::new("tool-1"),
        unit_generation: 1,
        unit_state: UnitState::Waiting,
        interpretation_id: InterpretationId::generate(),
        connection_id: ConnectionId::generate(),
        external_session_id: None,
        flow_id: FlowId::main(),
        lane_id: LaneId::tool(),
        lane_ordinal: 1,
        causal_parent_id: None,
        source_time: None,
        source_step: None,
        unit: CanonicalUnit::Tool(ToolActionEvent {
            tool_action_id: ToolActionId::new("a1"),
            tool_name: Some("bash".into()),
            request_state: ToolRequestState::Ready,
            execution_state: ToolExecutionState::Waiting,
            result_state: ToolResultState::Absent,
            request_payload: Some("{}".into()),
            result_payload: None,
            terminal_outcome: None,
            waiting_for: None,
        }),
    });
    let waiting = CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
        unit_id: UnitId::new("tool-2"),
        unit_generation: 1,
        unit_state: UnitState::Waiting,
        interpretation_id: InterpretationId::generate(),
        connection_id: ConnectionId::generate(),
        external_session_id: None,
        flow_id: FlowId::main(),
        lane_id: LaneId::tool(),
        lane_ordinal: 2,
        causal_parent_id: None,
        source_time: None,
        source_step: None,
        unit: CanonicalUnit::Tool(ToolActionEvent {
            tool_action_id: ToolActionId::new("a2"),
            tool_name: Some("bash".into()),
            request_state: ToolRequestState::Assembling,
            execution_state: ToolExecutionState::Waiting,
            result_state: ToolResultState::Absent,
            request_payload: None,
            result_payload: None,
            terminal_outcome: None,
            waiting_for: Some("args".into()),
        }),
    });
    assert!(needs_loop_dispatch(std::slice::from_ref(&ready)));
    assert!(!needs_loop_dispatch(std::slice::from_ref(&waiting)));
    assert!(needs_loop_dispatch(&[waiting, ready]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_loop_no_ambient_spawn_unavailable() {
    use crate::registry::EmptyToolRegistry;
    use crate::runtime::{DefaultLoopRuntime, StartLoop};
    use crate::subscription::SubscriptionPublisher;
    use crate::tools::NoToolRuntime;
    use monoloop_contracts::{
        CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, FlowId,
        InterpretationId, InterpreterOutputEvent, LaneId, LoopEndKind, LoopId, LoopLimits,
        LoopOutputEvent, LoopScope, MonoloopRunId, OutboundToolOutcome, ToolActionEvent,
        ToolActionId, ToolExecutionState, ToolRequestState, ToolResultState, UnitId, UnitState,
    };

    let (pub_, sub) = SubscriptionPublisher::channel("prep", 16);
    let run = MonoloopRunId::new("r-prep");
    let loop_id = LoopId::new("l-prep");
    let scope = LoopScope {
        monoloop_run_id: run.clone(),
        loop_id: loop_id.clone(),
        accepted_interpretation_ids: vec![],
        accepted_connection_ids: vec![],
        accepted_external_session_ids: vec![],
        accept_all_in_run: true,
    };
    let limits = LoopLimits::default();
    let (handle, fut) = DefaultLoopRuntime::new()
        .prepare(StartLoop {
            monoloop_run_id: run,
            loop_id,
            scope,
            subscription: sub,
            tool_registry: Arc::new(EmptyToolRegistry::new()),
            tool_runtime: Arc::new(NoToolRuntime::new()),
            output_capacity: 16,
            limits,
        })
        .expect("prepare");

    let ready = InterpreterOutputEvent::Unit(Box::new(CanonicalUnitEvent::Created(
        CanonicalUnitSnapshot {
            unit_id: UnitId::new("tool-1"),
            unit_generation: 1,
            unit_state: UnitState::Waiting,
            interpretation_id: InterpretationId::new("i1"),
            connection_id: ConnectionId::new("c1"),
            external_session_id: None,
            flow_id: FlowId::main(),
            lane_id: LaneId::tool(),
            lane_ordinal: 1,
            causal_parent_id: None,
            source_time: None,
            source_step: None,
            unit: CanonicalUnit::Tool(ToolActionEvent {
                tool_action_id: ToolActionId::new("a1"),
                tool_name: Some("bash".into()),
                request_state: ToolRequestState::Ready,
                execution_state: ToolExecutionState::Waiting,
                result_state: ToolResultState::Absent,
                request_payload: Some(r#"{"cmd":"ls"}"#.into()),
                result_payload: None,
                terminal_outcome: None,
                waiting_for: Some("external".into()),
            }),
        },
    )));

    let feed = async {
        pub_.publish(ready).await.unwrap();
        drop(pub_);
    };
    tokio::join!(fut, feed);

    let mut unavailable = 0;
    let mut outbound = 0;
    {
        let mut rx = handle.take_output().await;
        while let Some(ev) = rx.recv().await {
            match &ev {
                LoopOutputEvent::ToolUnavailable { .. } => unavailable += 1,
                LoopOutputEvent::OutboundToolResult(r) => {
                    assert_eq!(r.outcome, OutboundToolOutcome::ToolUnavailable);
                    assert!(r.tool_execution_id.is_none());
                    outbound += 1;
                }
                LoopOutputEvent::LoopEnded(_) => break,
                _ => {}
            }
        }
    }
    assert_eq!(unavailable, 1);
    assert_eq!(outbound, 1);
    let end = handle.completion.wait().await;
    assert_eq!(end.kind, LoopEndKind::Drained);
    assert_eq!(end.tools_unavailable, 1);
}

fn ready_tool_unit() -> monoloop_contracts::CanonicalUnitEvent {
    use monoloop_contracts::{
        CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, FlowId,
        InterpretationId, LaneId, ToolActionEvent, ToolActionId, ToolExecutionState,
        ToolRequestState, ToolResultState, UnitId, UnitState,
    };
    CanonicalUnitEvent::Created(CanonicalUnitSnapshot {
        unit_id: UnitId::new("tool-1"),
        unit_generation: 1,
        unit_state: UnitState::Waiting,
        interpretation_id: InterpretationId::new("i1"),
        connection_id: ConnectionId::new("c1"),
        external_session_id: None,
        flow_id: FlowId::main(),
        lane_id: LaneId::tool(),
        lane_ordinal: 1,
        causal_parent_id: None,
        source_time: None,
        source_step: None,
        unit: CanonicalUnit::Tool(ToolActionEvent {
            tool_action_id: ToolActionId::new("a1"),
            tool_name: Some("bash".into()),
            request_state: ToolRequestState::Ready,
            execution_state: ToolExecutionState::Waiting,
            result_state: ToolResultState::Absent,
            request_payload: Some(r#"{"cmd":"ls"}"#.into()),
            result_payload: None,
            terminal_outcome: None,
            waiting_for: None,
        }),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_empty_loop_ready_under_task_spawner() {
    use super::event_publisher::EventPublisherCommand;
    use super::loop_dispatch::run_supervised_empty_loop;
    use super::task_spawner::TransactionTaskSpawner;
    use super::task_supervisor::TaskSupervisor;
    use crate::transaction::sticky_cancel::StickyCancel;
    use monoloop_contracts::{ChannelId, ExchangeId, TransactionEventPayload, TransactionId};
    use tokio::sync::mpsc;

    let (spawner, mut spawn_rx) = TransactionTaskSpawner::channel(8);
    let pump = tokio::spawn(async move {
        let mut tasks = TaskSupervisor::new();
        while let Some(req) = spawn_rx.recv().await {
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        let _ = tasks.abort_and_drain().await;
    });

    let (publish_tx, mut publish_rx) = mpsc::channel::<EventPublisherCommand>(16);
    let collector = tokio::spawn(async move {
        let mut tool_lifecycle = 0u32;
        while let Some(cmd) = publish_rx.recv().await {
            if let EventPublisherCommand::Publish(payload) = cmd {
                if matches!(payload.as_ref(), TransactionEventPayload::ToolLifecycle(_)) {
                    tool_lifecycle = tool_lifecycle.saturating_add(1);
                }
            }
        }
        tool_lifecycle
    });

    let cancel = Arc::new(StickyCancel::new());
    let report = run_supervised_empty_loop(
        &spawner,
        TransactionId::generate(),
        ChannelId::try_new("llm").unwrap(),
        None,
        ExchangeId::generate(),
        vec![ready_tool_unit()],
        publish_tx,
        cancel,
    )
    .await
    .expect("supervised empty loop");

    assert_eq!(report.tools_unavailable, 1);
    assert_eq!(report.outbound_results, 1);

    drop(spawner);
    let lifecycle_count = collector.await.expect("collector");
    assert_eq!(lifecycle_count, 1);
    pump.await.expect("spawn pump");
}

/// Non-empty HostToolRegistry: Ready tool completes via supervised HostToolRuntime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_non_empty_loop_dispatches_registered_tool() {
    use super::event_publisher::EventPublisherCommand;
    use super::loop_dispatch::run_supervised_tool_loop;
    use super::task_spawner::TransactionTaskSpawner;
    use super::task_supervisor::TaskSupervisor;
    use crate::transaction::dispatcher::TransactionToolDispatcher;
    use crate::transaction::host_tools::RegisteredTool;
    use crate::transaction::loop_adapters::{HostToolRuntime, ResolvedToolRegistry};
    use crate::transaction::resolved_tools::ResolvedToolSet;
    use crate::transaction::sticky_cancel::StickyCancel;
    use crate::transaction::tool_capacity::SharedToolCapacity;
    use crate::transaction::tool_handler::ImmediateToolHandler;
    use monoloop_contracts::{
        ChannelId, ExchangeId, JsonSchema, SessionKey, ToolCompletion, ToolExecutionClass, ToolId,
        ToolLimits, ToolName, ToolOutputContract, ToolSpec, ToolSuccessContract,
        TransactionEventPayload, TransactionId,
    };
    use tokio::sync::mpsc;

    let schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "cmd": { "type": "string" } },
        "required": ["cmd"],
        "additionalProperties": false
    }))
    .unwrap();
    let out_schema = JsonSchema::try_new(serde_json::json!({
        "type": "object",
        "properties": { "ok": { "type": "boolean" } },
        "required": ["ok"],
        "additionalProperties": false
    }))
    .unwrap();
    let spec = ToolSpec::try_new(
        ToolId::try_new("bash").unwrap(),
        ToolName::try_new("bash").unwrap(),
        "bash tool",
        schema,
        ToolOutputContract {
            success: ToolSuccessContract::json(out_schema),
            error_data_schema: None,
        },
        ToolLimits {
            max_concurrent: 2,
            max_input_bytes: 1024,
            max_output_bytes: 1024,
            execution_deadline: Duration::from_secs(2),
        },
        ToolExecutionClass::CooperativeInProcess {
            grace: Duration::from_millis(50),
        },
    )
    .unwrap();
    let registered = RegisteredTool::new(
        spec,
        Arc::new(ImmediateToolHandler::new(|_c, _x| {
            Ok(ToolCompletion::Succeeded(
                monoloop_contracts::CanonicalToolOutput::Json(serde_json::json!({"ok": true})),
            ))
        })),
    );
    let resolved = ResolvedToolSet::from_registered(vec![registered]);
    let tx_id = TransactionId::generate();
    let exchange_id = ExchangeId::generate();
    let dispatcher = TransactionToolDispatcher::new(
        tx_id,
        SessionKey::new(
            ChannelId::try_new("llm").unwrap(),
            SessionId::try_new("s1").unwrap(),
        ),
        resolved.clone(),
        SharedToolCapacity::unlimited(),
        8,
        16,
    );

    let (spawner, mut spawn_rx) = TransactionTaskSpawner::channel(16);
    let pump = tokio::spawn(async move {
        let mut tasks = TaskSupervisor::new();
        while let Some(req) = spawn_rx.recv().await {
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        let _ = tasks.abort_and_drain().await;
    });

    let (publish_tx, mut publish_rx) = mpsc::channel::<EventPublisherCommand>(16);
    let collector = tokio::spawn(async move {
        let mut completed_ok = 0u32;
        while let Some(cmd) = publish_rx.recv().await {
            if let EventPublisherCommand::Publish(payload) = cmd {
                if let TransactionEventPayload::ToolLifecycle(
                    monoloop_contracts::ToolLifecycleEvent::Completed { result },
                ) = payload.as_ref()
                {
                    if matches!(
                        result.outcome,
                        monoloop_contracts::CanonicalToolResultOutcome::Succeeded(_)
                    ) {
                        completed_ok = completed_ok.saturating_add(1);
                    }
                }
            }
        }
        completed_ok
    });

    let runtime =
        HostToolRuntime::with_spawner(Arc::clone(&dispatcher), exchange_id, tx_id, spawner.clone());
    let cancel = Arc::new(StickyCancel::new());
    let report = run_supervised_tool_loop(
        &spawner,
        tx_id,
        ChannelId::try_new("llm").unwrap(),
        Some(SessionId::try_new("s1").unwrap()),
        exchange_id,
        vec![ready_tool_unit()],
        publish_tx,
        cancel,
        Arc::new(ResolvedToolRegistry::new(resolved)),
        Arc::new(runtime),
    )
    .await
    .expect("supervised non-empty loop");

    assert_eq!(report.tools_unavailable, 0, "registered tool must resolve");
    assert!(report.outbound_results >= 1);
    assert_eq!(report.tools_completed, 1);

    drop(spawner);
    let ok_count = collector.await.expect("collector");
    assert_eq!(ok_count, 1);
    pump.await.expect("spawn pump");
}

/// LAW 25: cancel before any Loop waiter must not report success/Drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_empty_loop_cancel_before_waiter_is_cancelled() {
    use super::event_publisher::EventPublisherCommand;
    use super::loop_dispatch::{run_supervised_empty_loop, LoopDispatchError};
    use super::task_spawner::TransactionTaskSpawner;
    use super::task_supervisor::TaskSupervisor;
    use crate::transaction::sticky_cancel::StickyCancel;
    use monoloop_contracts::{ChannelId, ExchangeId, TransactionId};
    use tokio::sync::mpsc;

    let (spawner, mut spawn_rx) = TransactionTaskSpawner::channel(8);
    let pump = tokio::spawn(async move {
        let mut tasks = TaskSupervisor::new();
        while let Some(req) = spawn_rx.recv().await {
            let id = tasks.spawn(req.class, req.future);
            let _ = req.reply.send(id);
        }
        let _ = tasks.abort_and_drain().await;
    });

    let (publish_tx, mut publish_rx) = mpsc::channel::<EventPublisherCommand>(8);
    let drain_pub = tokio::spawn(async move { while publish_rx.recv().await.is_some() {} });

    let cancel = Arc::new(StickyCancel::new());
    // Fire before run_supervised_empty_loop installs waiters.
    cancel.cancel();

    let err = run_supervised_empty_loop(
        &spawner,
        TransactionId::generate(),
        ChannelId::try_new("llm").unwrap(),
        None,
        ExchangeId::generate(),
        vec![ready_tool_unit()],
        publish_tx,
        Arc::clone(&cancel),
    )
    .await
    .expect_err("pre-cancel must not succeed");

    assert_eq!(err, LoopDispatchError::Cancelled);

    drop(spawner);
    let _ = drain_pub.await;
    pump.await.expect("spawn pump");
}
