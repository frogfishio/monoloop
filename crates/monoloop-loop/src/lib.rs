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

pub use registry::{EmptyToolRegistry, ResolveToolRequest, ToolRegistry, ToolResolution};
pub use runtime::{DefaultLoopRuntime, LoopCompletion, LoopControl, LoopHandle, LoopHealth, StartLoop};
pub use subscription::{
    CanonicalEventSubscription, DeliveredEvent, SubscriberId, SubscriptionGap, SubscriptionPublisher,
    SubscriptionStatus,
};
pub use tools::{NoToolRuntime, ToolRuntime};
pub use transaction::{
    CapacityManagers, ChannelBinding, ChannelRegistry, DefaultTransactionRuntime, EmptyBytesEncoder,
    EventSequencer, FinalizationGuard, HostToolRegistry, LiveChannel, McpListenerShell,
    RejectEncoder, ResolvedToolSet, RuntimeBootstrap, RuntimeConfig, RuntimeState, Startup,
    StartupError,
};

pub use monoloop_contracts::{
    CanonicalUnit, CanonicalUnitEvent, InterpretationEnd, InterpreterOutputEvent, LoopEnd,
    LoopEndKind, LoopError, LoopErrorKind, LoopId, LoopLimits, LoopOutputEvent, LoopScope,
    MonoloopRunId, OutboundToolOutcome, OutboundToolResult, ToolActionId, ToolRequestState,
    ToolUnavailableReason,
};
