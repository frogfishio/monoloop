//! SPDX-License-Identifier: AGPL-3.0-or-later
//! Copyright (C) Alexander R. Croft
//!
//! Component 03 — The Loop.
//!
//! Inner `LoopRuntime` (complete-unit tool reaction) plus the outer
//! transaction composition layer.
//!
//! Transaction lifecycle is being replaced by Runtime v2
//! (`doc/TRANSACTION_RUNTIME_V2_SPEC.md`). Public exports below reflect the
//! modules that compile during migration; full `TransactionRuntime` cutover is
//! M7.

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
    adapt_completion_callback, adapt_event_sink, begin_shutdown_placeholder, build_completion,
    rejecting, validate_tool_completion, validate_tool_input, AcpPromptEncoder, AcpPromptWireShape,
    AsyncToolHandler, CapacityManagers, ChannelBinding, ChannelRegistry, EmptyBytesEncoder,
    HeadlessPromptEncoder, HostCompletionAdapter, HostEventAdapter, HostToolRegistry,
    ImmediateToolHandler, InputValidationFailure, IsolatedKillableToolHandler, LedgerEntry,
    LifecycleLedger, LinkedToolExecutionHandle, LiveChannel, LostCompletionHandler,
    OpenAiChatCompletionsEncoder, OpenAiEncoderOptions, OutputValidationFailure,
    PanicOnStartHandler, RegisteredTool, RejectEncoder, ResolvedTool, ResolvedToolSet,
    RuntimeBootstrap, RuntimeConfig, RuntimeOwner, RuntimeState, SharedToolCapacity,
    ShutdownTicket, StartFailHandler, StartedRuntime, StartupError, SupervisorCommand, TaskClass,
    TaskId, TaskSupervisor, TerminalDecision, TestTextEncoder, ToolExecutionCompletion,
    ToolExecutionControl, ToolHandler, ToolKillHandle, TransactionCoordinator, TransactionPhase,
    TransactionReservations, TransactionRuntimeHandle, TransactionToolCapacity,
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
};
