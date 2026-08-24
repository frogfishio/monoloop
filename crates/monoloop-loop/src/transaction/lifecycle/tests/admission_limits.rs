//! Lifecycle unit tests (composed; see `mod.rs`).

use super::super::{StartedRuntime, TransactionRuntimeHandle};
use super::common::*;
use crate::transaction::bootstrap::{
    ControlHoldGate, FinalizerHoldGate, JoinOnlySpillInject, RuntimeBootstrap, RuntimeConfig,
    StartHoldGate, StoppedGate,
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
    assert_eq!(
        estimate_canonical_input_bytes(&plus).unwrap(),
        exact_bytes + 1
    );
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

/// D-023 / §9.2: unknown extension is rejected synchronously at admission
/// (not admitted then InvariantFailed asynchronously).
#[test]
fn unknown_extension_rejected_at_admission() {
    use monoloop_contracts::{ExtensionKey, VersionedExtension};

    let started = start_runtime(2, 2);
    let handle = started.handle.clone();
    let key = ExtensionKey::try_new("ns.unknown", 64).unwrap();
    let mut invocation = InvocationConfig::default();
    invocation.extensions.insert(
        key,
        VersionedExtension {
            version: 1,
            value: serde_json::json!({"x": 1}),
        },
    );
    let (delivery, receiver) =
        transaction_delivery(DeliveryLimits::try_new(32, 64 * 1024).unwrap()).unwrap();
    let err = handle
        .submit(TransactionSubmitRequest {
            channel_id: ChannelId::try_new("llm").unwrap(),
            session_id: Some(SessionId::try_new("ext-reject").unwrap()),
            input: user_text_input("hi").unwrap(),
            session_config: None,
            invocation_config: invocation,
            tools: vec![],
            delivery,
        })
        .expect_err("unknown extension must reject at admission");
    assert_eq!(err.kind, AdmissionErrorKind::InvalidConfiguration);
    assert!(
        err.message
            .to_ascii_lowercase()
            .contains("unknown extension")
            || err.message.contains("ns.unknown"),
        "expected unknown-extension message, got {:?}",
        err.message
    );
    assert_eq!(
        started.owner.ledger_len(),
        0,
        "failed admission must not leave a ledger row"
    );
    assert_rejected_silent(receiver);
    shutdown_owner(started);
}
