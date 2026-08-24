//! Transaction runtime composition (Component 3 outer layer).
//!
//! Runtime v2 lifecycle lives in [`lifecycle`] (`doc/TRANSACTION_RUNTIME_V2_SPEC.md`).
//! Obsolete uncompiled v1 modules (`active_registry`, `events`, `exchange`,
//! `spawn_gate`) were deleted under D-054; do not restore them.

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
mod mcp;
mod owned_process_registry;

pub use acp_encoder::{AcpPromptEncoder, AcpPromptWireShape, HeadlessPromptEncoder};
pub use bootstrap::{
    ControlHoldGate, FinalizerHoldGate, JoinOnlySpillInject, RuntimeBootstrap, RuntimeConfig,
    StartHoldGate, StoppedGate,
};
pub use channel_registry::{ChannelBinding, ChannelRegistry, LiveChannel};
pub use dispatcher::{
    DispatchOutcome, DispatchRequest, DispatcherLimits, OrphanToolPermitSet,
    TransactionToolDispatcher,
};
pub use error::StartupError;
pub use fake_support::{EmptyBytesEncoder, PanicEncoder, RejectEncoder, TestTextEncoder};
pub use host_tools::{HostToolRegistry, RegisteredTool};
pub use lifecycle::{
    adapt_completion_callback, adapt_event_sink, build_completion, LedgerEntry, LifecycleLedger,
    ReservationPool, ReservationPoolError, RuntimeOwner, ShutdownTicket, SpawnReject,
    StartedRuntime, SupervisorCommand, TaskClass, TaskExit, TaskId, TaskSupervisor,
    TerminalDecision, TerminalProposal, TransactionPhase, TransactionReservations,
    TransactionRuntimeHandle, TransactionTaskSpawner,
};
pub use loop_adapters::{
    dispatch_ready_tool, dispatch_ready_tool_cancellable, HostToolRuntime, ResolvedToolRegistry,
};
pub use mcp::{
    tool_definitions_from_resolved, CapabilityToken, McpBindingState, McpGateway, McpGatewayHandle,
    McpGatewayLimits, McpInstallError, McpRequestOwner, McpRouteTable, PendingMcpBinding,
    PreparedMcpGateway, TransactionMcpHandler,
};

pub use openai_encoder::{OpenAiChatCompletionsEncoder, OpenAiEncoderOptions};
#[allow(unused_imports)] // public D-048 surface for hosts/tests
pub use owned_process_registry::OwnedProcessRegistry;
pub use process_tool::{ProcessIsolatedToolHandler, ProcessToolCommand};
pub use resolved_tools::{ResolvedTool, ResolvedToolSet};
pub use state::RuntimeState;
pub use tool_capacity::{SharedToolCapacity, TransactionToolCapacity};
pub use tool_handler::{
    AbortableAtYieldHandler, AsyncToolHandler, ImmediateToolHandler, IsolatedKillableToolHandler,
    LinkedToolExecutionHandle, LostCompletionHandler, PanicOnStartHandler, StartFailHandler,
    ToolExecutionCompletion, ToolExecutionControl, ToolHandler, ToolKillHandle,
};
pub use validation::{
    validate_tool_completion, validate_tool_input, InputValidationFailure, OutputValidationFailure,
};
