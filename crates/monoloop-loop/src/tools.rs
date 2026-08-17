//! Abstract tool runtime + no-op runtime for empty registry.

use monoloop_contracts::ToolExecutionId;
use tokio::sync::oneshot;

/// Start request for a tool execution (never called with EmptyToolRegistry).
#[derive(Clone, Debug)]
pub struct StartToolExecution {
    /// Execution id allocated by The Loop.
    pub execution_id: ToolExecutionId,
    /// Tool action id from the canonical unit.
    pub tool_action_id: String,
    /// Tool name.
    pub tool_name: String,
    /// Complete payload.
    pub request_payload: String,
    /// Request generation that triggered dispatch.
    pub request_generation: u64,
}

/// Runtime error.
#[derive(Clone, Debug, thiserror::Error)]
#[error("tool runtime: {0}")]
pub struct ToolRuntimeError(pub String);

/// Terminal from a host/runtime execution mapped for Loop outbound emission.
#[derive(Clone, Debug)]
pub struct ToolRuntimeTerminal {
    /// Outcome for OutboundToolResult.
    pub outcome: monoloop_contracts::OutboundToolOutcome,
    /// Bounded payload.
    pub payload: String,
}

/// Handle for a running tool.
#[derive(Debug)]
pub struct ToolExecutionHandle {
    /// Execution id.
    pub execution_id: ToolExecutionId,
    /// Optional oneshot completion (present for real runtimes).
    pub completion: Option<oneshot::Receiver<ToolRuntimeTerminal>>,
}

/// Abstract tool runtime.
pub trait ToolRuntime: Send + Sync {
    /// Start a tool execution. Must not be called when registry always unavailable.
    fn start(&self, request: StartToolExecution) -> Result<ToolExecutionHandle, ToolRuntimeError>;
}

/// Runtime that asserts it is never started (pairs with EmptyToolRegistry).
#[derive(Clone, Debug, Default)]
pub struct NoToolRuntime;

impl NoToolRuntime {
    /// Create.
    pub fn new() -> Self {
        Self
    }
}

impl ToolRuntime for NoToolRuntime {
    fn start(&self, _request: StartToolExecution) -> Result<ToolExecutionHandle, ToolRuntimeError> {
        Err(ToolRuntimeError(
            "NoToolRuntime.start must never be called with EmptyToolRegistry".into(),
        ))
    }
}
