//! ToolRegistry / ToolRuntime adapters that delegate to TransactionToolDispatcher.

use super::dispatcher::{DispatchOutcome, DispatchRequest, TransactionToolDispatcher};
use super::lifecycle::{TaskClass, TransactionTaskSpawner};
use crate::registry::{
    ResolveToolRequest, ToolDescriptorRef, ToolRegistry, ToolRegistryError, ToolResolution,
};
use crate::tools::{
    StartToolExecution, ToolExecutionHandle, ToolRuntime, ToolRuntimeError, ToolRuntimeTerminal,
};
use monoloop_contracts::{
    ExchangeId, OutboundToolOutcome, ToolActionId, ToolName, ToolUnavailableReason, TransactionId,
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
    spawner: TransactionTaskSpawner,
    transaction_id: TransactionId,
}

impl HostToolRuntime {
    /// Supervisor-owned tool workers (no ambient `tokio::spawn`; Law 23).
    pub fn with_spawner(
        dispatcher: Arc<TransactionToolDispatcher>,
        exchange_id: ExchangeId,
        transaction_id: TransactionId,
        spawner: TransactionTaskSpawner,
    ) -> Self {
        Self {
            dispatcher,
            exchange_id,
            spawner,
            transaction_id,
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
        let execution_id = request.execution_id.clone();
        let (tx, rx) = oneshot::channel();
        let work = async move {
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
        };

        let class = TaskClass::ToolWorker(self.transaction_id, execution_id.clone());
        self.spawner
            .try_spawn_owned(class, work)
            .map_err(|_| ToolRuntimeError("tool worker spawn capacity exceeded".into()))?;

        Ok(ToolExecutionHandle {
            execution_id: request.execution_id,
            completion: Some(rx),
        })
    }
}

fn map_outcome(outcome: DispatchOutcome) -> ToolRuntimeTerminal {
    match outcome {
        DispatchOutcome::Canonical { result, .. } => {
            // Round-trip the full CanonicalToolResult so lifecycle publish preserves
            // Succeeded vs DomainFailed (and tool_id / ordinal).
            let payload =
                serde_json::to_string(&result).unwrap_or_else(|_| "{\"error\":\"encode\"}".into());
            let outcome = match &result.outcome {
                monoloop_contracts::CanonicalToolResultOutcome::Succeeded(_) => {
                    OutboundToolOutcome::Success
                }
                monoloop_contracts::CanonicalToolResultOutcome::DomainFailed(_) => {
                    OutboundToolOutcome::ExecutionFailed
                }
            };
            ToolRuntimeTerminal { outcome, payload }
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
    dispatch_ready_tool_cancellable(
        dispatcher,
        exchange_id,
        tool_action_id,
        tool_name,
        provider_tool_call_id,
        request_ordinal,
        arguments_json,
        None,
    )
    .await
}

/// Dispatch with an actor-owned cancel signal so mid-dispatch cancel joins the worker (D-028).
#[allow(clippy::too_many_arguments)]
pub async fn dispatch_ready_tool_cancellable(
    dispatcher: &Arc<TransactionToolDispatcher>,
    exchange_id: ExchangeId,
    tool_action_id: ToolActionId,
    tool_name: &str,
    provider_tool_call_id: &str,
    request_ordinal: u32,
    arguments_json: &str,
    cancel: Option<std::sync::Arc<super::sticky_cancel::StickyCancel>>,
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
        .dispatch_with_cancel(
            DispatchRequest {
                exchange_id,
                tool_action_id,
                tool_name: name,
                provider_tool_call_id: provider_tool_call_id.into(),
                request_ordinal,
                arguments_json: arguments_json.into(),
            },
            cancel,
        )
        .await
}
