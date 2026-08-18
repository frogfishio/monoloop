//! SPDX-License-Identifier: AGPL-3.0-or-later
//! Copyright (C) Alexander R. Croft
//!
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
    bound_diagnostics, dispatch_ready_tool, run_encoded_exchange, run_exchange,
    tool_definitions_from_resolved, validate_tool_completion, validate_tool_input,
    AcpPromptEncoder, AcpPromptWireShape, AsyncToolHandler, BoundedEventSender, CallbackService,
    CapabilityToken, CapacityManagers, ChannelBinding, ChannelRegistry, DefaultTransactionRuntime,
    DispatchOutcome, DispatchRequest, DispatcherLimits, EmptyBytesEncoder, EncodedExchangeParams,
    EventQueueFull, EventSequencer, ExchangeFailure, ExchangeOutcome, ExchangeParams,
    FinalizationGuard, HeadlessPromptEncoder, HostToolRegistry, HostToolRuntime,
    ImmediateToolHandler, InputValidationFailure, IsolatedKillableToolHandler,
    LinkedToolExecutionHandle, LiveChannel, LostCompletionHandler, McpBindingState, McpGateway,
    McpGatewayHandle, McpInstallError, McpListenerShell, McpRouteTable,
    OpenAiChatCompletionsEncoder, OpenAiEncoderOptions, OutputValidationFailure,
    PanicOnStartHandler, PendingMcpBinding, QueuedEvent, RegisteredTool, RejectEncoder,
    ResolvedTool, ResolvedToolRegistry, ResolvedToolSet, RuntimeBootstrap, RuntimeConfig,
    RuntimeState, SharedToolCapacity, StartFailHandler, Startup, StartupError, TestTextEncoder,
    ToolExecutionCompletion, ToolExecutionControl, ToolHandler, ToolKillHandle,
    TransactionMcpHandler, TransactionToolCapacity, TransactionToolDispatcher,
};

pub use monoloop_contracts::{
    CanonicalUnit, CanonicalUnitEvent, InterpretationEnd, InterpreterOutputEvent, LoopEnd,
    LoopEndKind, LoopError, LoopErrorKind, LoopId, LoopLimits, LoopOutputEvent, LoopScope,
    MonoloopRunId, OutboundToolOutcome, OutboundToolResult, ToolActionId, ToolRequestState,
    ToolUnavailableReason,
};
