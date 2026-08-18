//! Transaction runtime composition (Component 3 outer layer).
//!
//! WP-03: bootstrap / registries / startup.
//! WP-04: admission, events, finalization, callbacks, no-I/O actor.
//! WP-06: linked tools, dispatcher, Loop adapters.

mod acp_encoder;
mod active_registry;
mod actor;
mod admission;
mod bootstrap;
mod callback_service;
mod capacity;
mod channel_registry;
mod dispatcher;
mod error;
mod events;
mod exchange;
mod executor_spawn;
mod fake_support;
mod finalization;
mod host_tools;
mod loop_adapters;
mod mcp;
mod openai_encoder;
mod resolved_tools;
mod runtime;
mod state;
mod sticky_cancel;
mod tool_capacity;
mod tool_handler;
mod validation;

pub use acp_encoder::{AcpPromptEncoder, AcpPromptWireShape, HeadlessPromptEncoder};
pub use bootstrap::{RuntimeBootstrap, RuntimeConfig};
pub use callback_service::CallbackService;
pub use capacity::CapacityManagers;
pub use channel_registry::{ChannelBinding, ChannelRegistry, LiveChannel};
pub use dispatcher::{
    DispatchOutcome, DispatchRequest, DispatcherLimits, TransactionToolDispatcher,
};
pub use error::StartupError;
pub use events::{BoundedEventSender, EventQueueFull, QueuedEvent};
pub use exchange::{
    run_encoded_exchange, run_exchange, EncodedExchangeParams, ExchangeFailure, ExchangeOutcome,
    ExchangeParams,
};
pub use fake_support::{EmptyBytesEncoder, RejectEncoder, TestTextEncoder};
pub use finalization::{bound_diagnostics, EventSequencer, FinalizationGuard};
pub use host_tools::{HostToolRegistry, RegisteredTool};
pub use loop_adapters::{dispatch_ready_tool, HostToolRuntime, ResolvedToolRegistry};
/// Back-compat alias: the MCP gateway owns the loopback listener.
pub use mcp::McpGateway as McpListenerShell;
pub use mcp::{
    tool_definitions_from_resolved, CapabilityToken, McpBindingState, McpGateway, McpGatewayHandle,
    McpInstallError, McpRouteTable, PendingMcpBinding, TransactionMcpHandler,
};
pub use openai_encoder::{OpenAiChatCompletionsEncoder, OpenAiEncoderOptions};
pub use resolved_tools::{ResolvedTool, ResolvedToolSet};
pub use runtime::{DefaultTransactionRuntime, Startup};
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
