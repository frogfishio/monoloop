//! ToolRegistry / ToolRuntime adapters that delegate to TransactionToolDispatcher.

use super::dispatcher::{DispatchOutcome, DispatchRequest, TransactionToolDispatcher};
use crate::registry::{
    ResolveToolRequest, ToolDescriptorRef, ToolRegistry, ToolRegistryError, ToolResolution,
};
use crate::tools::{
    StartToolExecution, ToolExecutionHandle, ToolRuntime, ToolRuntimeError, ToolRuntimeTerminal,
};
use monoloop_contracts::{
    ExchangeId, OutboundToolOutcome, ToolActionId, ToolName, ToolUnavailableReason,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::oneshot;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Registry backed by the transaction-resolved allowlist.
pub struct ResolvedToolRegistry {
    tools: super::resolved_tools::ResolvedToolSet,
}

impl ResolvedToolRegistry {
    /// Construct from the admitted resolved set.
    pub fn new(tools: super::resolved_tools::ResolvedToolSet) -> Self {
        Self { tools }
    }
}

impl ToolRegistry for ResolvedToolRegistry {
    fn resolve<'a>(
        &'a self,
        request: ResolveToolRequest,
    ) -> BoxFuture<'a, Result<ToolResolution, ToolRegistryError>> {
        Box::pin(async move {
            let name = match ToolName::try_new(&request.tool_name) {
                Ok(n) => n,
                Err(_) => {
                    return Ok(ToolResolution::Unavailable(ToolUnavailableReason::NotFound));
                }
            };
            if self.tools.contains_name(&name) {
                Ok(ToolResolution::Available(ToolDescriptorRef {
                    name: request.tool_name,
                }))
            } else if self.tools.is_empty() {
                Ok(ToolResolution::Unavailable(
                    ToolUnavailableReason::NoRegisteredTool,
                ))
            } else {
                Ok(ToolResolution::Unavailable(ToolUnavailableReason::NotFound))
            }
        })
    }
}

/// Runtime that starts linked tools through the shared dispatcher.
pub struct HostToolRuntime {
    dispatcher: Arc<TransactionToolDispatcher>,
    exchange_id: ExchangeId,
}

impl HostToolRuntime {
    /// Construct for one transaction / exchange scope.
    pub fn new(dispatcher: Arc<TransactionToolDispatcher>, exchange_id: ExchangeId) -> Self {
        Self {
            dispatcher,
            exchange_id,
        }
    }
}

impl ToolRuntime for HostToolRuntime {
    fn start(&self, request: StartToolExecution) -> Result<ToolExecutionHandle, ToolRuntimeError> {
        let name = ToolName::try_new(&request.tool_name)
            .map_err(|_| ToolRuntimeError("invalid tool name".into()))?;
        let action_id = ToolActionId::new(request.tool_action_id.clone());
        let dispatcher = Arc::clone(&self.dispatcher);
        let exchange_id = self.exchange_id;
        let payload = request.request_payload;
        let provider_id = request.execution_id.as_str().to_string();
        let ordinal = request.request_generation as u32;
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let outcome = dispatcher
                .dispatch(DispatchRequest {
                    exchange_id,
                    tool_action_id: action_id,
                    tool_name: name,
                    provider_tool_call_id: provider_id,
                    request_ordinal: ordinal,
                    arguments_json: payload,
                })
                .await;
            let _ = tx.send(map_outcome(outcome));
        });

        Ok(ToolExecutionHandle {
            execution_id: request.execution_id,
            completion: Some(rx),
        })
    }
}

fn map_outcome(outcome: DispatchOutcome) -> ToolRuntimeTerminal {
    match outcome {
        DispatchOutcome::Canonical { result, .. } => {
            let payload = serde_json::to_string(&result.outcome)
                .unwrap_or_else(|_| "{\"error\":\"encode\"}".into());
            // Success and domain failure are both valid tool results for the model.
            ToolRuntimeTerminal {
                outcome: OutboundToolOutcome::Success,
                payload,
            }
        }
        DispatchOutcome::Rejected { code, message, .. } => ToolRuntimeTerminal {
            outcome: OutboundToolOutcome::DispatchRejected,
            payload: format!("{code}:{message}"),
        },
        DispatchOutcome::RuntimeFailed { code, .. } => ToolRuntimeTerminal {
            outcome: OutboundToolOutcome::ExecutionFailed,
            payload: code,
        },
    }
}

/// Dispatch one ready tool directly through the transaction dispatcher.
pub async fn dispatch_ready_tool(
    dispatcher: &Arc<TransactionToolDispatcher>,
    exchange_id: ExchangeId,
    tool_action_id: ToolActionId,
    tool_name: &str,
    provider_tool_call_id: &str,
    request_ordinal: u32,
    arguments_json: &str,
) -> DispatchOutcome {
    let name = match ToolName::try_new(tool_name) {
        Ok(n) => n,
        Err(_) => {
            return DispatchOutcome::Rejected {
                tool_action_id,
                code: "invalid_tool_name",
                message: "tool name failed identity validation".into(),
                lifecycle: vec![],
            };
        }
    };
    dispatcher
        .dispatch(DispatchRequest {
            exchange_id,
            tool_action_id,
            tool_name: name,
            provider_tool_call_id: provider_tool_call_id.into(),
            request_ordinal,
            arguments_json: arguments_json.into(),
        })
        .await
}
