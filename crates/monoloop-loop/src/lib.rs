//! Component 03 — The Loop.
//!
//! Inner `LoopRuntime` (complete-unit tool reaction) plus the outer
//! `TransactionRuntime` composition layer.
//!
//! See `doc/THE_LOOP.md` and `doc/TRANSACTION_RUNTIME_IMPLEMENTATION.md`.

#![deny(missing_docs)]

mod registry;
mod runtime;
mod subscription;
mod tools;
mod transaction;

pub use registry::{
    EmptyToolRegistry, ResolveToolRequest, ToolDescriptorRef, ToolRegistry, ToolRegistryError,
    ToolResolution,
};
pub use runtime::{
    DefaultLoopRuntime, LoopCompletion, LoopControl, LoopHandle, LoopHealth, StartLoop,
};
pub use subscription::{
    CanonicalEventSubscription, DeliveredEvent, SubscriberId, SubscriptionGap,
    SubscriptionPublisher, SubscriptionStatus,
};
pub use tools::{
    NoToolRuntime, StartToolExecution, ToolExecutionHandle, ToolRuntime, ToolRuntimeError,
    ToolRuntimeTerminal,
};
pub use transaction::{
    dispatch_ready_tool, run_encoded_exchange, run_exchange, tool_definitions_from_resolved,
    validate_tool_completion, validate_tool_input, AcpPromptEncoder, AcpPromptWireShape,
    AsyncToolHandler, BoundedEventSender, CapabilityToken, CapacityManagers, ChannelBinding,
    ChannelRegistry, DefaultTransactionRuntime, DispatchOutcome, DispatchRequest,
    EmptyBytesEncoder, EncodedExchangeParams, EventQueueFull, EventSequencer, ExchangeFailure,
    ExchangeOutcome, ExchangeParams, FinalizationGuard, HeadlessPromptEncoder, HostToolRegistry,
    HostToolRuntime, ImmediateToolHandler, InputValidationFailure, LinkedToolExecutionHandle,
    LiveChannel, LostCompletionHandler, McpBindingState, McpGateway, McpGatewayHandle,
    McpInstallError, McpListenerShell, McpRouteTable, OpenAiChatCompletionsEncoder,
    OpenAiEncoderOptions, OutputValidationFailure, PanicOnStartHandler, PendingMcpBinding,
    QueuedEvent, RegisteredTool, RejectEncoder, ResolvedTool, ResolvedToolRegistry,
    ResolvedToolSet, RuntimeBootstrap, RuntimeConfig, RuntimeState, SharedToolCapacity,
    StartFailHandler, Startup, StartupError, TestTextEncoder, ToolExecutionCompletion,
    ToolExecutionControl, ToolHandler, TransactionMcpHandler, TransactionToolCapacity,
    TransactionToolDispatcher,
};

pub use monoloop_contracts::{
    CanonicalUnit, CanonicalUnitEvent, InterpretationEnd, InterpreterOutputEvent, LoopEnd,
    LoopEndKind, LoopError, LoopErrorKind, LoopId, LoopLimits, LoopOutputEvent, LoopScope,
    MonoloopRunId, OutboundToolOutcome, OutboundToolResult, ToolActionId, ToolRequestState,
    ToolUnavailableReason,
};
