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
    adapt_completion_callback, adapt_event_sink, build_completion, validate_tool_completion,
    validate_tool_input, AcpPromptEncoder, AcpPromptWireShape, AsyncToolHandler, CapacityManagers,
    ChannelBinding, ChannelRegistry, EmptyBytesEncoder, HeadlessPromptEncoder,
    HostCompletionAdapter, HostEventAdapter, HostToolRegistry, ImmediateToolHandler,
    InputValidationFailure, IsolatedKillableToolHandler, LedgerEntry, LifecycleLedger,
    LinkedToolExecutionHandle, LiveChannel, LostCompletionHandler, OpenAiChatCompletionsEncoder,
    OpenAiEncoderOptions, OutputValidationFailure, PanicOnStartHandler, ProcessIsolatedToolHandler,
    ProcessToolCommand, RegisteredTool, RejectEncoder, ReservationPool, ReservationPoolError,
    ResolvedTool, ResolvedToolSet, RuntimeBootstrap, RuntimeConfig, RuntimeOwner, RuntimeState,
    SharedToolCapacity, ShutdownTicket, StartFailHandler, StartedRuntime, StartupError, StoppedGate,
    SupervisorCommand, TaskClass, TaskExit, TaskId, TaskSupervisor, TerminalDecision,
    TerminalProposal, TestTextEncoder, ToolExecutionCompletion, ToolExecutionControl, ToolHandler,
    ToolKillHandle, TransactionPhase, TransactionReservations, TransactionRuntimeHandle,
    TransactionToolCapacity,
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
