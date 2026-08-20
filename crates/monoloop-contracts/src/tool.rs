//! Canonical tool specification, call, result, and lifecycle contracts.

use crate::canonical::ToolActionId;
use crate::id::{ExchangeId, SessionKey, ToolId, ToolName, TransactionId};
use crate::limits::ToolLimits;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use thiserror::Error;

/// JSON Schema document for tool input/output (object root required).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JsonSchema {
    schema: serde_json::Value,
}

impl JsonSchema {
    /// Construct from a JSON value that must be an object.
    pub fn try_new(schema: serde_json::Value) -> Result<Self, ToolContractError> {
        if !schema.is_object() {
            return Err(ToolContractError::SchemaNotObject);
        }
        Ok(Self { schema })
    }

    /// Borrow the schema value.
    pub fn as_value(&self) -> &serde_json::Value {
        &self.schema
    }
}

/// Declared successful tool output shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ToolSuccessContract {
    /// JSON success body with schema.
    Json {
        /// Output schema.
        schema: JsonSchema,
    },
    /// Text success body with media type.
    Text {
        /// Bounded media type (e.g. `text/plain`).
        media_type: String,
    },
}

impl ToolSuccessContract {
    /// Construct JSON success contract.
    pub fn json(schema: JsonSchema) -> Self {
        Self::Json { schema }
    }

    /// Construct text success contract with validated media type.
    pub fn text(media_type: impl Into<String>) -> Result<Self, ToolContractError> {
        let media_type = media_type.into();
        if media_type.is_empty()
            || media_type.len() > 128
            || media_type.chars().any(|c| c.is_control())
        {
            return Err(ToolContractError::InvalidMediaType);
        }
        Ok(Self::Text { media_type })
    }
}

/// Output contract for a registered tool.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolOutputContract {
    /// Success shape.
    pub success: ToolSuccessContract,
    /// Optional domain-error data schema.
    pub error_data_schema: Option<JsonSchema>,
}

/// Structural execution / termination class for a registered tool (v2 §14).
///
/// Names describe real guarantees — not wishful “kill” labels on Tokio tasks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolExecutionClass {
    /// Cooperative cancel token; runtime cannot force stop. Failure to join
    /// leaves cleanup pending and can prevent `Stopped`.
    CooperativeInProcess {
        /// Grace period before cleanup-pending.
        grace: Duration,
    },
    /// Runtime owns a join handle and may `abort` at an await yield only.
    /// Not hard-killable.
    AbortableAtYield {
        /// Grace period before abort.
        grace: Duration,
    },
    /// Child process (or equivalent) isolation boundary with kill + wait.
    ProcessIsolated {
        /// Cooperative cancel grace before kill.
        grace: Duration,
        /// Hard cleanup deadline for kill/wait.
        kill_deadline: Duration,
    },
}

/// Immutable tool specification (no handler).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    /// Stable id for request selection.
    pub id: ToolId,
    /// Name exposed to models/MCP.
    pub name: ToolName,
    /// Bounded description.
    pub description: String,
    /// Input JSON schema.
    pub input_schema: JsonSchema,
    /// Output contract.
    pub output_contract: ToolOutputContract,
    /// Limits.
    pub limits: ToolLimits,
    /// Execution / termination class.
    pub execution_class: ToolExecutionClass,
}

impl ToolSpec {
    /// Maximum description bytes.
    pub const MAX_DESCRIPTION_BYTES: usize = 4 * 1024;

    /// Validate and construct a tool specification.
    pub fn try_new(
        id: ToolId,
        name: ToolName,
        description: impl Into<String>,
        input_schema: JsonSchema,
        output_contract: ToolOutputContract,
        limits: ToolLimits,
        execution_class: ToolExecutionClass,
    ) -> Result<Self, ToolContractError> {
        let description = description.into();
        if description.len() > Self::MAX_DESCRIPTION_BYTES {
            return Err(ToolContractError::DescriptionTooLong);
        }
        if description.chars().any(|c| c.is_control()) {
            return Err(ToolContractError::ControlCharacter);
        }
        if limits.max_concurrent == 0
            || limits.max_input_bytes == 0
            || limits.max_output_bytes == 0
            || limits.execution_deadline.is_zero()
        {
            return Err(ToolContractError::InvalidLimits);
        }
        match &execution_class {
            ToolExecutionClass::CooperativeInProcess { grace }
            | ToolExecutionClass::AbortableAtYield { grace } => {
                if grace.is_zero() {
                    return Err(ToolContractError::InvalidCancellationGrace);
                }
            }
            ToolExecutionClass::ProcessIsolated {
                grace,
                kill_deadline,
            } => {
                if grace.is_zero() || kill_deadline.is_zero() {
                    return Err(ToolContractError::InvalidCancellationGrace);
                }
            }
        }
        Ok(Self {
            id,
            name,
            description,
            input_schema,
            output_contract,
            limits,
            execution_class,
        })
    }
}

/// Provider-neutral tool call arguments at dispatch time.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    /// Tool name as requested.
    pub tool_name: ToolName,
    /// Resolved tool id.
    pub tool_id: ToolId,
    /// Provider correlation id (preserved exactly).
    pub provider_tool_call_id: String,
    /// JSON arguments.
    pub arguments: serde_json::Value,
    /// Model-declared order within the exchange.
    pub request_ordinal: u32,
}

/// Correlation context for a tool invocation (no prompts or secrets).
#[derive(Clone, Debug)]
pub struct ToolCallContext {
    /// Owning transaction.
    pub transaction_id: TransactionId,
    /// Session key.
    pub session_key: SessionKey,
    /// Exchange when known.
    pub exchange_id: Option<ExchangeId>,
    /// Internal tool action id.
    pub tool_action_id: ToolActionId,
    /// Tool id.
    pub tool_id: ToolId,
    /// Absolute deadline.
    pub deadline: Instant,
}

/// Canonical successful or domain-failed tool output body.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CanonicalToolOutput {
    /// JSON body.
    Json(serde_json::Value),
    /// Text body.
    Text(String),
}

/// Bounded public domain error from a tool.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanonicalToolError {
    /// Error code.
    pub code: String,
    /// Safe message.
    pub message: String,
    /// Optional data.
    pub data: Option<serde_json::Value>,
}

impl CanonicalToolError {
    /// Construct a bounded domain error.
    pub fn try_new(
        code: impl Into<String>,
        message: impl Into<String>,
        data: Option<serde_json::Value>,
        max_message_bytes: usize,
    ) -> Result<Self, ToolContractError> {
        let code = code.into();
        let message = message.into();
        if code.is_empty() || code.len() > 64 || code.chars().any(|c| c.is_control()) {
            return Err(ToolContractError::InvalidErrorCode);
        }
        if message.is_empty()
            || message.len() > max_message_bytes
            || message.chars().any(|c| c.is_control())
        {
            return Err(ToolContractError::InvalidErrorMessage);
        }
        Ok(Self {
            code,
            message,
            data,
        })
    }
}

/// Success or declared domain failure (not a runtime failure).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CanonicalToolResultOutcome {
    /// Validated success.
    Succeeded(CanonicalToolOutput),
    /// Declared domain failure.
    DomainFailed(CanonicalToolError),
}

/// Sole continuation/MCP success-domain product.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanonicalToolResult {
    /// Transaction.
    pub transaction_id: TransactionId,
    /// Session key.
    pub session_key: SessionKey,
    /// Exchange.
    pub exchange_id: ExchangeId,
    /// Internal action id.
    pub tool_action_id: ToolActionId,
    /// Tool id.
    pub tool_id: ToolId,
    /// Provider tool call id preserved exactly.
    pub provider_tool_call_id: String,
    /// Model-declared order.
    pub request_ordinal: u32,
    /// Outcome.
    pub outcome: CanonicalToolResultOutcome,
}

/// Host tool lifecycle event on the transaction stream (not dialect observation).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ToolLifecycleEvent {
    /// Dispatch accepted.
    Started {
        /// Action id.
        tool_action_id: ToolActionId,
        /// Tool id.
        tool_id: ToolId,
        /// Tool name.
        tool_name: ToolName,
        /// Provider call id.
        provider_tool_call_id: String,
        /// Ordinal.
        request_ordinal: u32,
    },
    /// Canonical result ready (success or domain failure).
    Completed {
        /// Result.
        result: CanonicalToolResult,
    },
    /// Runtime failure (selects ToolExchangeFailed when policy requires).
    RuntimeFailed {
        /// Action id.
        tool_action_id: ToolActionId,
        /// Tool id.
        tool_id: ToolId,
        /// Safe failure code.
        code: String,
    },
}

/// Tool contract construction error.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ToolContractError {
    /// Schema root must be object.
    #[error("JSON schema must be an object")]
    SchemaNotObject,
    /// Description too long.
    #[error("tool description exceeds maximum length")]
    DescriptionTooLong,
    /// Control character.
    #[error("tool string must not contain control characters")]
    ControlCharacter,
    /// Invalid limits.
    #[error("tool limits must be non-zero")]
    InvalidLimits,
    /// Invalid cancellation grace.
    #[error("cancellation grace must be non-zero")]
    InvalidCancellationGrace,
    /// Invalid media type.
    #[error("invalid media type")]
    InvalidMediaType,
    /// Invalid error code.
    #[error("invalid tool error code")]
    InvalidErrorCode,
    /// Invalid error message.
    #[error("invalid tool error message")]
    InvalidErrorMessage,
}

/// Failure starting a linked tool handler.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ToolStartError {
    /// Capacity exceeded.
    #[error("tool capacity exceeded")]
    CapacityExceeded,
    /// Handler rejected start.
    #[error("tool start rejected: {0}")]
    Rejected(&'static str),
}

/// Runtime failure from a tool implementation.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ToolRuntimeError {
    /// Panic caught.
    #[error("tool panicked")]
    Panicked,
    /// Lost completion.
    #[error("tool completion lost")]
    CompletionLost,
    /// Output contract violation.
    #[error("tool output contract violated")]
    OutputContractViolated,
    /// Termination mechanism failed.
    #[error("tool termination failed")]
    TerminationFailed,
    /// Deadline exceeded.
    #[error("tool deadline exceeded")]
    DeadlineExceeded,
}

/// Completion of a tool execution handle.
#[derive(Clone, Debug, PartialEq)]
pub enum ToolCompletion {
    /// Success output.
    Succeeded(CanonicalToolOutput),
    /// Domain failure.
    DomainFailed(CanonicalToolError),
    /// Runtime failure.
    RuntimeFailed(ToolRuntimeError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{ChannelId, SessionId};

    #[test]
    fn tool_spec_construction() {
        let schema = JsonSchema::try_new(serde_json::json!({
            "type": "object",
            "properties": { "q": { "type": "string" } }
        }))
        .unwrap();
        let out = ToolOutputContract {
            success: ToolSuccessContract::json(schema.clone()),
            error_data_schema: None,
        };
        let spec = ToolSpec::try_new(
            ToolId::try_new("search").unwrap(),
            ToolName::try_new("search").unwrap(),
            "Search the workspace",
            schema,
            out,
            ToolLimits::default(),
            ToolExecutionClass::AbortableAtYield {
                grace: Duration::from_secs(1),
            },
        )
        .unwrap();
        assert_eq!(spec.id.as_str(), "search");
    }

    #[test]
    fn schema_must_be_object() {
        assert!(JsonSchema::try_new(serde_json::json!([])).is_err());
    }

    #[test]
    fn lifecycle_result_serializes() {
        let tid = TransactionId::generate();
        let sk = SessionKey::new(
            ChannelId::try_new("ch").unwrap(),
            SessionId::try_new("s").unwrap(),
        );
        let result = CanonicalToolResult {
            transaction_id: tid,
            session_key: sk,
            exchange_id: ExchangeId::generate(),
            tool_action_id: ToolActionId::new("a1"),
            tool_id: ToolId::try_new("t").unwrap(),
            provider_tool_call_id: "p1".into(),
            request_ordinal: 0,
            outcome: CanonicalToolResultOutcome::Succeeded(CanonicalToolOutput::Text("ok".into())),
        };
        let ev = ToolLifecycleEvent::Completed { result };
        let json = serde_json::to_string(&ev).unwrap();
        let _back: ToolLifecycleEvent = serde_json::from_str(&json).unwrap();
    }
}
