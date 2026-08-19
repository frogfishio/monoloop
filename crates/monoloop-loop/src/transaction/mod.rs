//! Transaction runtime composition (Component 3 outer layer).
//!
//! Runtime v2 lifecycle lives in [`lifecycle`] (`doc/TRANSACTION_RUNTIME_V2_SPEC.md`).
//! The deleted v1 files (`runtime`, `admission`, `actor`, `finalization`,
//! `callback_service`, `executor_spawn`, `tool_join_vault`) are **not** restored.
//!
//! Modules that still depended on those files are deferred until their migration
//! stage (see `lifecycle/mod.rs` and the v2 migration plan). They remain on disk
//! but are not compiled, per "no additional deletions yet."

mod acp_encoder;
mod bootstrap;
mod capacity;
mod channel_registry;
mod error;
mod fake_support;
mod host_tools;
pub mod lifecycle;
mod openai_encoder;
mod resolved_tools;
mod state;
mod sticky_cancel;
mod tool_capacity;
mod tool_handler;
mod validation;

// Deferred until migration stages that replace v1 dependencies:
// mod active_registry;  // → lifecycle ledger (M2)
// mod dispatcher;       // → TaskSupervisor join ownership (M5)
// mod events;           // → lifecycle delivery + sequencing (M3)
// mod exchange;         // → supervised Connector ownership (M4)
// mod loop_adapters;    // → single Loop state machine (M3/M5)
// mod mcp;              // → TaskSupervisor registration (M5)
// mod spawn_gate;       // retired by owned executor (M2); delete at M7

pub use acp_encoder::{AcpPromptEncoder, AcpPromptWireShape, HeadlessPromptEncoder};
pub use bootstrap::{RuntimeBootstrap, RuntimeConfig};
pub use capacity::CapacityManagers;
pub use channel_registry::{ChannelBinding, ChannelRegistry, LiveChannel};
pub use error::StartupError;
pub use fake_support::{EmptyBytesEncoder, RejectEncoder, TestTextEncoder};
pub use host_tools::{HostToolRegistry, RegisteredTool};
pub use lifecycle::{
    adapt_completion_callback, adapt_event_sink, begin_shutdown_placeholder, build_completion,
    rejecting, HostCompletionAdapter, HostEventAdapter, LedgerEntry, LifecycleLedger, RuntimeOwner,
    ShutdownTicket, StartedRuntime, SupervisorCommand, TaskClass, TaskId, TaskSupervisor,
    TerminalDecision, TransactionCoordinator, TransactionPhase, TransactionReservations,
    TransactionRuntimeHandle,
};
pub use openai_encoder::{OpenAiChatCompletionsEncoder, OpenAiEncoderOptions};
pub use resolved_tools::{ResolvedTool, ResolvedToolSet};
pub use state::RuntimeState;
pub use tool_capacity::{SharedToolCapacity, TransactionToolCapacity};
pub use tool_handler::{
    AsyncToolHandler, ImmediateToolHandler, IsolatedKillableToolHandler, LinkedToolExecutionHandle,
    LostCompletionHandler, PanicOnStartHandler, StartFailHandler, ToolExecutionCompletion,
    ToolExecutionControl, ToolHandler, ToolKillHandle,
};
pub use validation::{
    validate_tool_completion, validate_tool_input, InputValidationFailure, OutputValidationFailure,
};
