//! Loop identities, limits, and provider-neutral outbound tool results.
//!
//! See `doc/THE_LOOP.md`. No concrete tools or dialect encoding.

use crate::id::{ConnectionId, ExternalSessionId, MonoloopRunId};
use crate::canonical::{InterpretationId, ToolActionId, UnitId};
use serde::{Deserialize, Serialize};

/// Loop instance identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LoopId(String);

impl LoopId {
    /// Create from an explicit value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Allocate a random id.
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable tool execution identity within one Loop incarnation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToolExecutionId(String);

impl ToolExecutionId {
    /// Create from an explicit value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Allocate a random id.
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Aggregate Loop bounds.
#[derive(Clone, Debug)]
pub struct LoopLimits {
    /// Maximum tracked tool actions.
    pub max_tool_actions: usize,
    /// Maximum concurrent tool executions.
    pub max_concurrent_executions: usize,
    /// Maximum queued ready requests.
    pub max_queued_ready: usize,
    /// Maximum output queue items.
    pub max_output_queue: usize,
    /// Maximum deduplication table entries.
    pub max_dedup_entries: usize,
}

impl Default for LoopLimits {
    fn default() -> Self {
        Self {
            max_tool_actions: 1024,
            max_concurrent_executions: 32,
            max_queued_ready: 256,
            max_output_queue: 4096,
            max_dedup_entries: 4096,
        }
    }
}

/// Explicit Loop admission scope (no ambient expansion).
#[derive(Clone, Debug)]
pub struct LoopScope {
    /// Owning run.
    pub monoloop_run_id: MonoloopRunId,
    /// Loop identity.
    pub loop_id: LoopId,
    /// Admitted interpretation ids (empty = accept any for initial test convenience
    /// only when `accept_all_interpretations` is true).
    pub accepted_interpretation_ids: Vec<InterpretationId>,
    /// Admitted connection ids.
    pub accepted_connection_ids: Vec<ConnectionId>,
    /// Admitted external session ids.
    pub accepted_external_session_ids: Vec<ExternalSessionId>,
    /// When true, skip interpretation membership checks (tests / single-source runs).
    pub accept_all_in_run: bool,
}

impl Default for LoopScope {
    fn default() -> Self {
        Self {
            monoloop_run_id: MonoloopRunId::generate(),
            loop_id: LoopId::generate(),
            accepted_interpretation_ids: Vec::new(),
            accepted_connection_ids: Vec::new(),
            accepted_external_session_ids: Vec::new(),
            accept_all_in_run: true,
        }
    }
}

impl LoopScope {
    /// Scope for a single interpretation/connection pair.
    pub fn single(
        run_id: MonoloopRunId,
        loop_id: LoopId,
        interpretation_id: InterpretationId,
        connection_id: ConnectionId,
        external_session_id: Option<ExternalSessionId>,
    ) -> Self {
        let mut sessions = Vec::new();
        if let Some(s) = external_session_id {
            sessions.push(s);
        }
        Self {
            monoloop_run_id: run_id,
            loop_id,
            accepted_interpretation_ids: vec![interpretation_id],
            accepted_connection_ids: vec![connection_id],
            accepted_external_session_ids: sessions,
            accept_all_in_run: false,
        }
    }
}

/// Why a tool was unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolUnavailableReason {
    /// Empty registry / no registered tool.
    NoRegisteredTool,
    /// Named tool not found.
    NotFound,
    /// Policy denied (future).
    Denied,
}

/// Terminal outcome of a loop-owned tool action (provider-neutral).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutboundToolOutcome {
    /// Tool succeeded (future runtime).
    Success,
    /// Tool unavailable at registry.
    ToolUnavailable,
    /// Dispatch rejected (limits/validation).
    DispatchRejected,
    /// Execution failed.
    ExecutionFailed,
    /// Cancelled.
    Cancelled,
    /// Execution lost.
    ExecutionLost,
}

/// Provider-neutral outbound tool result (Loop product; not dialect-encoded).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundToolResult {
    /// Result identity.
    pub outbound_result_id: String,
    /// Owning run.
    pub monoloop_run_id: MonoloopRunId,
    /// Loop identity.
    pub loop_id: LoopId,
    /// Source interpretation.
    pub source_interpretation_id: InterpretationId,
    /// Source connection.
    pub source_connection_id: ConnectionId,
    /// External session when present.
    pub external_session_id: Option<ExternalSessionId>,
    /// Tool action id.
    pub tool_action_id: ToolActionId,
    /// Request generation that triggered dispatch.
    pub request_generation: u64,
    /// Execution id when started.
    pub tool_execution_id: Option<ToolExecutionId>,
    /// Terminal outcome.
    pub outcome: OutboundToolOutcome,
    /// Complete result payload or safe error (bounded).
    pub payload: String,
    /// Canonical unit id for correlation.
    pub source_unit_id: UnitId,
}

/// Closed Loop output event vocabulary (initial).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoopOutputEvent {
    /// Registry resolution requested for a ready tool.
    ToolDispatchRequested {
        /// Tool action.
        tool_action_id: ToolActionId,
        /// Generation.
        request_generation: u64,
    },
    /// Tool unavailable (empty registry path).
    ToolUnavailable {
        /// Tool action.
        tool_action_id: ToolActionId,
        /// Reason.
        reason: ToolUnavailableReason,
    },
    /// Provider-neutral outbound result.
    OutboundToolResult(OutboundToolResult),
    /// Safe loop diagnostic.
    Diagnostic {
        /// Bounded message.
        message: String,
    },
    /// Loop ended.
    LoopEnded(LoopEnd),
}

/// Exactly one Loop terminal report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoopEnd {
    /// Owning run.
    pub monoloop_run_id: MonoloopRunId,
    /// Loop identity.
    pub loop_id: LoopId,
    /// Terminal kind.
    pub kind: LoopEndKind,
    /// Delivery events received.
    pub delivery_events_received: u64,
    /// Duplicate events ignored.
    pub duplicate_events: u64,
    /// Tool actions by terminal unavailable count.
    pub tools_unavailable: u64,
    /// Outbound results emitted.
    pub outbound_results_emitted: u64,
    /// Safe diagnostics.
    pub safe_diagnostics: Vec<String>,
}

/// Loop terminal kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoopEndKind {
    /// Source drained cleanly.
    Drained,
    /// Cancelled.
    Cancelled,
    /// Subscription gap/loss.
    SubscriptionLost,
    /// Output failed.
    OutputFailed,
    /// Invariant failed.
    InvariantFailed,
    /// Configuration failed.
    ConfigurationFailed,
}

/// Loop error classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoopErrorKind {
    /// Event out of scope.
    EventOutOfScope,
    /// Delivery sequence gap.
    DeliverySequenceGap,
    /// Unit identity conflict.
    UnitIdentityConflict,
    /// Tool request incomplete.
    ToolRequestIncomplete,
    /// Concurrency/queue limit.
    LimitExceeded,
    /// Cancelled.
    Cancelled,
    /// Invariant violation.
    InvariantViolation,
    /// Configuration invalid.
    ConfigurationInvalid,
    /// Output backpressure.
    OutputBackpressure,
}

/// Loop error with safe diagnostics.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("{kind:?}: {message}")]
pub struct LoopError {
    /// Closed family.
    pub kind: LoopErrorKind,
    /// Bounded message.
    pub message: String,
}

impl LoopError {
    /// Construct.
    pub fn new(kind: LoopErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Cancelled.
    pub fn cancelled() -> Self {
        Self::new(LoopErrorKind::Cancelled, "loop cancelled")
    }

    /// Gap.
    pub fn gap() -> Self {
        Self::new(LoopErrorKind::DeliverySequenceGap, "subscription gap detected")
    }

    /// Limit.
    pub fn limit(message: impl Into<String>) -> Self {
        Self::new(LoopErrorKind::LimitExceeded, message)
    }
}
