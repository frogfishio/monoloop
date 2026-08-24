//! SPDX-License-Identifier: AGPL-3.0-or-later
//! Copyright (C) Alexander R. Croft
//!
//! Component 03 — The Loop.
//!
//! Inner `LoopRuntime` (complete-unit tool reaction) plus the outer
//! transaction composition layer (Runtime v2 lifecycle under `transaction::lifecycle`).

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
    DefaultLoopRuntime, LoopCompletion, LoopControl, LoopHandle, LoopHealth, LoopRunFuture,
    StartLoop,
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
    adapt_completion_callback, adapt_event_sink, build_completion, dispatch_ready_tool,
    dispatch_ready_tool_cancellable, tool_definitions_from_resolved, validate_tool_completion,
    validate_tool_input, AbortableAtYieldHandler, AcpPromptEncoder, AcpPromptWireShape,
    AsyncToolHandler, CapabilityToken, ChannelBinding, ChannelRegistry, DispatchOutcome,
    ControlHoldGate, DispatchRequest, DispatcherLimits, EmptyBytesEncoder, FinalizerHoldGate,
    HeadlessPromptEncoder, HostToolRegistry, HostToolRuntime,
    ImmediateToolHandler, InputValidationFailure, IsolatedKillableToolHandler, JoinOnlySpillInject,
    LedgerEntry, LifecycleLedger, LinkedToolExecutionHandle, LiveChannel, LostCompletionHandler,
    McpBindingState, McpGateway, McpGatewayHandle, McpGatewayLimits, McpInstallError,
    McpRequestOwner, McpRouteTable, OpenAiChatCompletionsEncoder, OpenAiEncoderOptions,
    OrphanToolPermitSet, OutputValidationFailure, OwnedProcessRegistry, PanicEncoder,
    PanicOnStartHandler, PendingMcpBinding, PreparedMcpGateway, ProcessIsolatedToolHandler,
    ProcessToolCommand, RegisteredTool, RejectEncoder, ReservationPool, ReservationPoolError,
    ResolvedTool, ResolvedToolRegistry, ResolvedToolSet, RuntimeBootstrap, RuntimeConfig,
    RuntimeOwner, RuntimeState, SharedToolCapacity, ShutdownTicket, SpawnReject, StartFailHandler,
    StartHoldGate, StartedRuntime, StartupError, StoppedGate, SupervisorCommand, TaskClass,
    TaskExit, TaskId, TaskSupervisor, TerminalDecision, TerminalProposal, TestTextEncoder,
    ToolExecutionCompletion, ToolExecutionControl, ToolHandler, ToolKillHandle,
    TransactionMcpHandler, TransactionPhase, TransactionReservations, TransactionRuntimeHandle,
    TransactionTaskSpawner, TransactionToolCapacity, TransactionToolDispatcher,
};

pub use monoloop_contracts::{
    estimate_event_bytes, transaction_delivery, AdmissionError, AdmissionErrorKind,
    AdmissionReceipt, BoundaryKind, CanonicalUnit, CanonicalUnitEvent, CleanupFailureCode,
    CleanupStatus, CompletionPublishResult, DeliveryConfigError, DeliveryLimits, EventEnqueueError,
    InterpretationEnd, InterpreterOutputEvent, LoopEnd, LoopEndKind, LoopError, LoopErrorKind,
    LoopId, LoopLimits, LoopOutputEvent, LoopScope, MonoloopRunId, OutboundToolOutcome,
    OutboundToolResult, ShutdownReport, ShutdownSnapshot, ShutdownWaitOutcome,
    TerminalEventDelivery, ToolActionId, ToolRequestState, ToolUnavailableReason,
    TransactionCompletion, TransactionDelivery, TransactionEndEvent, TransactionReceiver,
    TransactionSubmitRequest,
};
