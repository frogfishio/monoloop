//! Component 03 — The Loop.
//!
//! Consumes a lossless canonical subscription. Dispatches tools only on complete
//! `ToolRequestReady`. Initial composition uses EmptyToolRegistry / NoToolRuntime.
//!
//! See `doc/THE_LOOP.md`.

#![deny(missing_docs)]

mod registry;
mod runtime;
mod subscription;
mod tools;

pub use registry::{EmptyToolRegistry, ResolveToolRequest, ToolRegistry, ToolResolution};
pub use runtime::{DefaultLoopRuntime, LoopCompletion, LoopControl, LoopHandle, LoopHealth, StartLoop};
pub use subscription::{
    CanonicalEventSubscription, DeliveredEvent, SubscriberId, SubscriptionGap, SubscriptionPublisher,
    SubscriptionStatus,
};
pub use tools::{NoToolRuntime, ToolRuntime};

pub use monoloop_contracts::{
    CanonicalUnit, CanonicalUnitEvent, InterpretationEnd, InterpreterOutputEvent, LoopEnd,
    LoopEndKind, LoopError, LoopErrorKind, LoopId, LoopLimits, LoopOutputEvent, LoopScope,
    MonoloopRunId, OutboundToolOutcome, OutboundToolResult, ToolActionId, ToolRequestState,
    ToolUnavailableReason,
};
