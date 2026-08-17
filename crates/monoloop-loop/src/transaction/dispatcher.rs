//! Single validated execution path for linked tools (MCP and model share this).

use super::resolved_tools::ResolvedToolSet;
use super::tool_capacity::{SharedToolCapacity, TransactionToolCapacity};
use super::validation::{
    validate_tool_completion, validate_tool_input, InputValidationFailure, DEFAULT_MAX_JSON_DEPTH,
};
use monoloop_contracts::{
    CanonicalToolError, CanonicalToolOutput, CanonicalToolResult, CanonicalToolResultOutcome,
    ExchangeId, SessionKey, ToolActionId, ToolCall, ToolCallContext, ToolCompletion, ToolId,
    ToolLifecycleEvent, ToolName, ToolRuntimeError, ToolStartError, TransactionId,
};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Request to dispatch one complete tool call through the linked handler path.
#[derive(Clone, Debug)]
pub struct DispatchRequest {
    /// Exchange owning this call (model path); MCP may synthesize one.
    pub exchange_id: ExchangeId,
    /// Internal action id.
    pub tool_action_id: ToolActionId,
    /// Public tool name as requested.
    pub tool_name: ToolName,
    /// Provider correlation id preserved exactly.
    pub provider_tool_call_id: String,
    /// Model-declared order.
    pub request_ordinal: u32,
    /// Complete JSON argument payload.
    pub arguments_json: String,
}

/// Outcome of a dispatch attempt.
#[derive(Clone, Debug)]
pub enum DispatchOutcome {
    /// Canonical success or declared domain failure (continuation product).
    Canonical {
        /// Validated result.
        result: CanonicalToolResult,
        /// Lifecycle events produced (Started + Completed).
        lifecycle: Vec<ToolLifecycleEvent>,
    },
    /// Invalid/disallowed arguments — rejected tool result, not transaction failure.
    Rejected {
        /// Action id.
        tool_action_id: ToolActionId,
        /// Safe reason code.
        code: &'static str,
        /// Safe message.
        message: String,
        /// Lifecycle (Started may be omitted when never accepted).
        lifecycle: Vec<ToolLifecycleEvent>,
    },
    /// Handler/runtime failure — selects `ToolExchangeFailed` when policy requires.
    RuntimeFailed {
        /// Action id.
        tool_action_id: ToolActionId,
        /// Tool id when known.
        tool_id: Option<ToolId>,
        /// Safe failure code.
        code: String,
        /// Lifecycle events (may include Started + RuntimeFailed).
        lifecycle: Vec<ToolLifecycleEvent>,
    },
}

/// Transaction-owned dispatcher: allowlist, validation, capacity, handler, output check.
pub struct TransactionToolDispatcher {
    transaction_id: TransactionId,
    session_key: SessionKey,
    tools: ResolvedToolSet,
    capacity: Arc<TransactionToolCapacity>,
    max_error_message_bytes: usize,
    max_json_depth: u32,
}

impl TransactionToolDispatcher {
    /// Build a dispatcher for one admitted transaction.
    pub fn new(
        transaction_id: TransactionId,
        session_key: SessionKey,
        tools: ResolvedToolSet,
        shared_capacity: Arc<SharedToolCapacity>,
        max_concurrent_tools: usize,
        max_queued_tools: usize,
    ) -> Arc<Self> {
        let capacity =
            TransactionToolCapacity::new(shared_capacity, max_concurrent_tools, max_queued_tools);
        for spec in tools.specs() {
            capacity.configure_tool(spec.id.clone(), spec.limits.max_concurrent);
        }
        Arc::new(Self {
            transaction_id,
            session_key,
            tools,
            capacity,
            max_error_message_bytes: 1024,
            max_json_depth: DEFAULT_MAX_JSON_DEPTH,
        })
    }

    /// Resolved tool set (encoder / MCP projection).
    pub fn tools(&self) -> &ResolvedToolSet {
        &self.tools
    }

    /// Transaction identity.
    pub fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    /// Session key.
    pub fn session_key(&self) -> &SessionKey {
        &self.session_key
    }

    /// Dispatch one call end-to-end.
    pub async fn dispatch(self: &Arc<Self>, request: DispatchRequest) -> DispatchOutcome {
        let action = request.tool_action_id.clone();

        // Allowlist by public name.
        let Some(resolved) = self.tools.get_by_name(&request.tool_name) else {
            return DispatchOutcome::Rejected {
                tool_action_id: action,
                code: "tool_not_allowed",
                message: "tool not in resolved set".into(),
                lifecycle: vec![],
            };
        };
        let tool_id = resolved.spec.id.clone();
        let spec = resolved.spec.clone();
        let handler = Arc::clone(&resolved.handler);

        if !self.capacity.try_enqueue() {
            return DispatchOutcome::Rejected {
                tool_action_id: action,
                code: "tool_queue_full",
                message: "per-transaction tool queue full".into(),
                lifecycle: vec![],
            };
        }

        // Input validation before capacity acquire for execution.
        let arguments = match validate_tool_input(
            &request.arguments_json,
            &spec.input_schema,
            spec.limits.max_input_bytes,
            self.max_json_depth,
        ) {
            Ok(v) => v,
            Err(f) => {
                self.capacity.dequeue();
                return reject_input(action, f);
            }
        };

        // Wait briefly for concurrency (bounded spin/yield); fail closed if not acquired.
        let permit = {
            let mut acquired = None;
            let deadline = Instant::now() + Duration::from_millis(50);
            while Instant::now() < deadline {
                if let Some(p) = self.capacity.try_acquire(&tool_id) {
                    acquired = Some(p);
                    break;
                }
                tokio::task::yield_now().await;
            }
            match acquired {
                Some(p) => p,
                None => {
                    // try_acquire dequeues only on success; still queued.
                    self.capacity.dequeue();
                    return DispatchOutcome::Rejected {
                        tool_action_id: action,
                        code: "tool_capacity_exceeded",
                        message: "tool concurrency capacity exceeded".into(),
                        lifecycle: vec![],
                    };
                }
            }
        };

        let mut lifecycle = vec![ToolLifecycleEvent::Started {
            tool_action_id: action.clone(),
            tool_id: tool_id.clone(),
            tool_name: request.tool_name.clone(),
            provider_tool_call_id: request.provider_tool_call_id.clone(),
            request_ordinal: request.request_ordinal,
        }];

        let call = ToolCall {
            tool_name: request.tool_name.clone(),
            tool_id: tool_id.clone(),
            provider_tool_call_id: request.provider_tool_call_id.clone(),
            arguments,
            request_ordinal: request.request_ordinal,
        };
        let context = ToolCallContext {
            transaction_id: self.transaction_id,
            session_key: self.session_key.clone(),
            exchange_id: Some(request.exchange_id),
            tool_action_id: action.clone(),
            tool_id: tool_id.clone(),
            deadline: Instant::now() + spec.limits.execution_deadline,
        };

        let start_result = catch_unwind(AssertUnwindSafe(|| handler.start(call, context)));
        let handle = match start_result {
            Ok(Ok(h)) => h,
            Ok(Err(ToolStartError::CapacityExceeded)) => {
                drop(permit);
                return DispatchOutcome::Rejected {
                    tool_action_id: action,
                    code: "tool_capacity_exceeded",
                    message: "handler capacity exceeded".into(),
                    lifecycle,
                };
            }
            Ok(Err(ToolStartError::Rejected(reason))) => {
                drop(permit);
                lifecycle.push(ToolLifecycleEvent::RuntimeFailed {
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    code: "tool_start_rejected".into(),
                });
                return DispatchOutcome::RuntimeFailed {
                    tool_action_id: action,
                    tool_id: Some(tool_id),
                    code: format!("start_rejected:{reason}"),
                    lifecycle,
                };
            }
            Err(_) => {
                drop(permit);
                lifecycle.push(ToolLifecycleEvent::RuntimeFailed {
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    code: "tool_panicked".into(),
                });
                return DispatchOutcome::RuntimeFailed {
                    tool_action_id: action,
                    tool_id: Some(tool_id),
                    code: "panicked".into(),
                    lifecycle,
                };
            }
        };

        // Bounded execution: deadline + cancel.
        let deadline = spec.limits.execution_deadline;
        let control = handle.control.clone();
        let completion = tokio::select! {
            biased;
            c = handle.completion.wait() => c,
            _ = tokio::time::sleep(deadline) => {
                control.cancel();
                ToolCompletion::RuntimeFailed(ToolRuntimeError::DeadlineExceeded)
            }
        };
        drop(permit);

        let validated = match validate_tool_completion(
            completion,
            &spec.output_contract,
            spec.limits.max_output_bytes,
            self.max_error_message_bytes,
            self.max_json_depth,
        ) {
            Ok(c) => c,
            Err(_) => {
                lifecycle.push(ToolLifecycleEvent::RuntimeFailed {
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    code: "output_contract_violated".into(),
                });
                return DispatchOutcome::RuntimeFailed {
                    tool_action_id: action,
                    tool_id: Some(tool_id),
                    code: "output_contract_violated".into(),
                    lifecycle,
                };
            }
        };

        match validated {
            ToolCompletion::Succeeded(output) => {
                let result = CanonicalToolResult {
                    transaction_id: self.transaction_id,
                    session_key: self.session_key.clone(),
                    exchange_id: request.exchange_id,
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    provider_tool_call_id: request.provider_tool_call_id,
                    request_ordinal: request.request_ordinal,
                    outcome: CanonicalToolResultOutcome::Succeeded(output),
                };
                lifecycle.push(ToolLifecycleEvent::Completed {
                    result: result.clone(),
                });
                DispatchOutcome::Canonical { result, lifecycle }
            }
            ToolCompletion::DomainFailed(err) => {
                let result = CanonicalToolResult {
                    transaction_id: self.transaction_id,
                    session_key: self.session_key.clone(),
                    exchange_id: request.exchange_id,
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    provider_tool_call_id: request.provider_tool_call_id,
                    request_ordinal: request.request_ordinal,
                    outcome: CanonicalToolResultOutcome::DomainFailed(err),
                };
                lifecycle.push(ToolLifecycleEvent::Completed {
                    result: result.clone(),
                });
                DispatchOutcome::Canonical { result, lifecycle }
            }
            ToolCompletion::RuntimeFailed(e) => {
                let code = match e {
                    ToolRuntimeError::Panicked => "panicked",
                    ToolRuntimeError::CompletionLost => "completion_lost",
                    ToolRuntimeError::OutputContractViolated => "output_contract_violated",
                    ToolRuntimeError::TerminationFailed => "termination_failed",
                    ToolRuntimeError::DeadlineExceeded => "deadline_exceeded",
                };
                lifecycle.push(ToolLifecycleEvent::RuntimeFailed {
                    tool_action_id: action.clone(),
                    tool_id: tool_id.clone(),
                    code: code.into(),
                });
                DispatchOutcome::RuntimeFailed {
                    tool_action_id: action,
                    tool_id: Some(tool_id),
                    code: code.into(),
                    lifecycle,
                }
            }
        }
    }
}

fn reject_input(action: ToolActionId, f: InputValidationFailure) -> DispatchOutcome {
    let (code, message) = match f {
        InputValidationFailure::OversizedInput => ("oversized_input", "tool input exceeds limit"),
        InputValidationFailure::InvalidJson => ("invalid_json", "tool arguments are not valid JSON"),
        InputValidationFailure::DepthExceeded => ("json_depth_exceeded", "tool arguments too deep"),
        InputValidationFailure::SchemaInvalid => {
            ("schema_invalid", "tool arguments fail input schema")
        }
    };
    // Rejected path still yields a domain-style rejection result shape for tests.
    let _ = CanonicalToolError::try_new(code, message, None, 256);
    let _ = CanonicalToolOutput::Text(String::new());
    DispatchOutcome::Rejected {
        tool_action_id: action,
        code,
        message: message.into(),
        lifecycle: vec![],
    }
}
