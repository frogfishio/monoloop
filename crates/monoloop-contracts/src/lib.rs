//! SPDX-License-Identifier: AGPL-3.0-or-later
//! Copyright (C) Alexander R. Croft
//!
//! Shared contracts for Monoloop product components.
//!
//! Product components share identities, dialect descriptors, canonical events,
//! configuration, transaction ports, and closed error families. This crate
//! intentionally contains no transport, dialect decoder, tool execution, UI,
//! or host-agent logic.

#![deny(missing_docs)]

mod canonical;
mod channel;
mod config;
mod dialect;
mod encoder;
mod error;
mod id;
mod input;
mod limits;
mod loop_types;
mod safe;
mod tool;
mod transaction;

pub use canonical::{
    BoundaryKind, CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, DiagnosticKind, FlowId,
    InterpretationEnd, InterpretationEndKind, InterpretationId, InterpreterOutputEvent, LaneId,
    ModelDiagnostic, ParagraphBoundary, ParagraphKind, SemanticBoundary, SourceTimeObservation,
    StructuralAtom, StructureKind, TextChannel, TextSentence, TokenCount, ToolActionEvent,
    ToolActionId, ToolExecutionState, ToolRequestState, ToolResultState, ToolTerminalOutcome,
    UnitId, UnitState, UsageObservation,
};
pub use channel::{
    send_and_retain_allowed, ChannelCapabilities, ChannelCapabilityError, ChannelDescriptor,
    ChannelKind, ExchangeMode, McpConfigurationCapability, McpReachability, SessionMode,
    ToolExecutionMode,
};
pub use config::{
    merge_effective_config, ChannelDefaults, ConfigError, ConfigOption, ContinuationPolicy,
    EffectiveConfig, ExtensionKey, InvocationConfig, OptionPolicy, ReasoningEffort, ResponseFormat,
    SessionConfig, VersionedExtension,
};
pub use dialect::{DialectBinding, DialectDescriptor, DialectFamily, DialectNegotiation};
pub use encoder::{
    ContinuationContext, EncodedExchange, EncodingError, ExchangeInputPolicy, InitialEncodeRequest,
    OutboundDialectEncoder, ToolContinuationEncodeRequest,
};
pub use error::{ConnectorError, ConnectorErrorKind, InterpreterError, InterpreterErrorKind};
pub use id::{
    validate_identity_string, ChannelId, ConnectionId, ExchangeId, ExternalSessionId,
    GrokSessionId, IdentityError, MonoloopRunId, RequestId, SessionId, SessionKey, ToolId,
    ToolName, TransactionId, MAX_IDENTITY_BYTES,
};
pub use input::{
    user_text_input, CanonicalAssistantToolCall, CanonicalInput, CanonicalMessage,
    InputValidationError, TextPart,
};
pub use limits::{
    ChannelLimits, ConnectorLimits, ExtensionLimits, InputLimits, InterpretationLimits,
    LimitsError, ToolLimits, TransactionLimits, TransportBufferLimits,
};
pub use loop_types::{
    LoopEnd, LoopEndKind, LoopError, LoopErrorKind, LoopId, LoopLimits, LoopOutputEvent, LoopScope,
    OutboundToolOutcome, OutboundToolResult, ToolExecutionId, ToolUnavailableReason,
};
pub use safe::{DiagnosticCode, SafeDiagnostic, SafeDiagnosticError};
pub use tool::{
    CanonicalToolError, CanonicalToolOutput, CanonicalToolResult, CanonicalToolResultOutcome,
    JsonSchema, ToolCall, ToolCallContext, ToolCancellationPolicy, ToolCompletion,
    ToolContractError, ToolLifecycleEvent, ToolOutputContract, ToolRuntimeError, ToolSpec,
    ToolStartError, ToolSuccessContract,
};
pub use transaction::{
    AdmissionError, AdmissionErrorKind, AdmissionReceipt, CancellationReason,
    CancellationReasonCode, CompletionCallback, CompletionDelivery, CompletionDeliveryError,
    EventDelivery, EventDeliveryError, EventDeliveryOutcome, FnCompletionCallback, FnEventSink,
    Shutdown, ShutdownDisposition, TerminationDisposition, TerminationMode, TerminationReason,
    TerminationReasonCode, TransactionDiagnostic, TransactionEnd, TransactionEndKind,
    TransactionEvent, TransactionEventPayload, TransactionEventSink, TransactionRequest,
    TransactionRuntime, TransactionSelector, TransactionUsage,
};

pub use bytes::Bytes;
