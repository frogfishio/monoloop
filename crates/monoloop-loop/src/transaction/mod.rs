//! Transaction runtime composition (Component 3 outer layer).
//!
//! Runtime v2 lifecycle lives in [`lifecycle`] (`doc/TRANSACTION_RUNTIME_V2_SPEC.md`).
//! The deleted v1 files are **not** restored.
//!
//! Deferred on-disk modules (`exchange`, …) remain until their migration stage.

mod acp_encoder;
mod bootstrap;
mod channel_registry;
mod error;
mod fake_support;
mod host_tools;
pub mod lifecycle;
mod openai_encoder;
mod process_tool;
mod resolved_tools;
mod state;
mod sticky_cancel;
mod tool_capacity;
mod tool_handler;
mod validation;

mod dispatcher;
mod loop_adapters;

// Deferred until later migration stages (kept on disk, not compiled):
// mod active_registry;
// mod events;
// mod exchange;
mod mcp;
// mod spawn_gate;

pub use acp_encoder::{AcpPromptEncoder, AcpPromptWireShape, HeadlessPromptEncoder};
pub use bootstrap::{
    FinalizerHoldGate, RuntimeBootstrap, RuntimeConfig, StartHoldGate, StoppedGate,
};
pub use channel_registry::{ChannelBinding, ChannelRegistry, LiveChannel};
pub use dispatcher::{
    DispatchOutcome, DispatchRequest, DispatcherLimits, TransactionToolDispatcher,
};
pub use error::StartupError;
pub use fake_support::{EmptyBytesEncoder, PanicEncoder, RejectEncoder, TestTextEncoder};
pub use host_tools::{HostToolRegistry, RegisteredTool};
pub use loop_adapters::{
    dispatch_ready_tool, dispatch_ready_tool_cancellable, HostToolRuntime, ResolvedToolRegistry,
};
pub use mcp::{
    tool_definitions_from_resolved, CapabilityToken, McpBindingState, McpGateway, McpGatewayHandle,
    McpInstallError, McpRouteTable, PendingMcpBinding, TransactionMcpHandler,
};
pub use lifecycle::{
    adapt_completion_callback, adapt_event_sink, build_completion, HostCompletionAdapter,
    HostEventAdapter, LedgerEntry, LifecycleLedger, ReservationPool, ReservationPoolError,
    RuntimeOwner, ShutdownTicket, StartedRuntime, SupervisorCommand, TaskClass, TaskExit, TaskId,
    TaskSupervisor, TerminalDecision, TerminalProposal, TransactionPhase, TransactionReservations,
    TransactionRuntimeHandle, TransactionTaskSpawner,
};

pub use openai_encoder::{OpenAiChatCompletionsEncoder, OpenAiEncoderOptions};
pub use process_tool::{ProcessIsolatedToolHandler, ProcessToolCommand};
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
