//! Abstract tool runtime + no-op runtime for empty registry.

use monoloop_contracts::ToolExecutionId;

/// Start request for a tool execution (never called with EmptyToolRegistry).
#[derive(Clone, Debug)]
pub struct StartToolExecution {
    /// Execution id allocated by The Loop.
    pub execution_id: ToolExecutionId,
    /// Tool name.
    pub tool_name: String,
    /// Complete payload.
    pub request_payload: String,
}

/// Runtime error.
#[derive(Clone, Debug, thiserror::Error)]
#[error("tool runtime: {0}")]
pub struct ToolRuntimeError(pub String);

/// Handle for a running tool (future).
#[derive(Debug)]
pub struct ToolExecutionHandle {
    /// Execution id.
    pub execution_id: ToolExecutionId,
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
