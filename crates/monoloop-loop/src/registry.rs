//! Abstract tool registry + empty implementation.

use monoloop_contracts::{ToolActionId, ToolUnavailableReason};
use std::future::Future;
use std::pin::Pin;

/// Request to resolve a complete tool by name.
#[derive(Clone, Debug)]
pub struct ResolveToolRequest {
    /// Tool action id.
    pub tool_action_id: ToolActionId,
    /// Complete tool name.
    pub tool_name: String,
    /// Complete request payload JSON.
    pub request_payload: String,
}

/// Opaque available tool reference (no concrete tool types).
#[derive(Clone, Debug)]
pub struct ToolDescriptorRef {
    /// Stable descriptor name.
    pub name: String,
}

/// Registry resolution result.
#[derive(Clone, Debug)]
pub enum ToolResolution {
    /// Tool is available (future runtime).
    Available(ToolDescriptorRef),
    /// Tool unavailable.
    Unavailable(ToolUnavailableReason),
}

/// Registry error.
#[derive(Clone, Debug, thiserror::Error)]
#[error("tool registry: {0}")]
pub struct ToolRegistryError(pub String);

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Abstract tool registry.
pub trait ToolRegistry: Send + Sync {
    /// Resolve a complete tool request.
    fn resolve<'a>(
        &'a self,
        request: ResolveToolRequest,
    ) -> BoxFuture<'a, Result<ToolResolution, ToolRegistryError>>;
}

/// Required first implementation: every request is unavailable.
#[derive(Clone, Debug, Default)]
pub struct EmptyToolRegistry;

impl EmptyToolRegistry {
    /// Create empty registry.
    pub fn new() -> Self {
        Self
    }
}

impl ToolRegistry for EmptyToolRegistry {
    fn resolve<'a>(
        &'a self,
        _request: ResolveToolRequest,
    ) -> BoxFuture<'a, Result<ToolResolution, ToolRegistryError>> {
        Box::pin(async {
            Ok(ToolResolution::Unavailable(
                ToolUnavailableReason::NoRegisteredTool,
            ))
        })
    }
}
