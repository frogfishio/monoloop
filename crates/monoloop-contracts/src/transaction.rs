//! Transaction request, events, terminal, sinks, and runtime port.

use crate::canonical::CanonicalUnitEvent;
use crate::config::{InvocationConfig, SessionConfig};
use crate::id::ToolId;
use crate::id::{ChannelId, SessionId, SessionKey, TransactionId};
use crate::input::CanonicalInput;
use crate::safe::SafeDiagnostic;
use crate::tool::ToolLifecycleEvent;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

/// Future returned by event delivery (no async_trait required).
pub type EventDelivery =
    Pin<Box<dyn Future<Output = Result<(), EventDeliveryError>> + Send + 'static>>;

/// Caller event sink (push-based).
pub trait TransactionEventSink: Send + Sync + 'static {
    /// Deliver one ordered event. Must return promptly with a future.
    fn deliver(&self, event: TransactionEvent) -> EventDelivery;
}

/// Future returned by completion callback.
pub type CompletionDelivery =
    Pin<Box<dyn Future<Output = Result<(), CompletionDeliveryError>> + Send + 'static>>;

/// One-shot completion callback.
pub trait CompletionCallback: Send + 'static {
    /// Invoke exactly once with the terminal result.
    fn call(self: Box<Self>, end: TransactionEnd) -> CompletionDelivery;
}

/// Closure adapter for [`TransactionEventSink`].
pub struct FnEventSink<F>(pub F);

impl<F> TransactionEventSink for FnEventSink<F>
where
    F: Fn(TransactionEvent) -> EventDelivery + Send + Sync + 'static,
{
    fn deliver(&self, event: TransactionEvent) -> EventDelivery {
        (self.0)(event)
    }
}

/// Closure adapter for [`CompletionCallback`].
pub struct FnCompletionCallback<F>(pub F);

impl<F> CompletionCallback for FnCompletionCallback<F>
where
    F: FnOnce(TransactionEnd) -> CompletionDelivery + Send + 'static,
{
    fn call(self: Box<Self>, end: TransactionEnd) -> CompletionDelivery {
        (self.0)(end)
    }
}

/// Transaction submission request (synchronous admission; async progress).
pub struct TransactionRequest {
    /// Explicit Channel selection.
    pub channel_id: ChannelId,
    /// Existing session when known; `None` for new external create or direct-LLM generate.
    pub session_id: Option<SessionId>,
    /// Canonical input messages.
    pub input: CanonicalInput,
    /// Optional external-agent session configuration.
    pub session_config: Option<SessionConfig>,
    /// Invocation configuration.
    pub invocation_config: InvocationConfig,
    /// Selected host tool ids (deduplicated at admission).
    pub tools: Vec<ToolId>,
    /// Required event sink.
    pub events: Arc<dyn TransactionEventSink>,
    /// Required completion callback.
    pub completion: Box<dyn CompletionCallback>,
}

/// Immediate admission receipt (no network performed).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionReceipt {
    /// Generated transaction id.
    pub transaction_id: TransactionId,
    /// Session id when already known (direct LLM or existing external).
    pub session_id: Option<SessionId>,
}

/// How to address an in-flight transaction for control.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TransactionSelector {
    /// By transaction id (valid during external session creation).
    Transaction(TransactionId),
    /// By established session key.
    Session(SessionKey),
}

/// Termination mode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminationMode {
    /// Cooperative cancellation.
    Cancel {
        /// Reason.
        reason: CancellationReason,
    },
    /// Forced terminate.
    ForceTerminate {
        /// Reason.
        reason: TerminationReason,
    },
}

/// Cancellation reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancellationReason {
    /// Closed code.
    pub code: CancellationReasonCode,
    /// Optional safe detail.
    pub detail: Option<SafeDiagnostic>,
}

/// Cancellation reason codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CancellationReasonCode {
    /// Caller requested cancel.
    CallerRequested,
    /// Runtime is shutting down.
    RuntimeShutdown,
}

/// Force-termination reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminationReason {
    /// Closed code.
    pub code: TerminationReasonCode,
    /// Optional safe detail.
    pub detail: Option<SafeDiagnostic>,
}

/// Termination reason codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TerminationReasonCode {
    /// Caller requested force.
    CallerRequested,
    /// Cancel grace expired.
    CancellationGraceExpired,
    /// Runtime is shutting down.
    RuntimeShutdown,
}

/// Immediate disposition of a terminate request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminationDisposition {
    /// Request accepted.
    Accepted,
    /// Already terminal or already requested.
    AlreadyRequested,
    /// Transaction already terminal.
    AlreadyTerminal,
    /// Unknown selector.
    NotFound,
}

/// Shutdown future type.
pub type Shutdown = Pin<Box<dyn Future<Output = ShutdownDisposition> + Send + 'static>>;

/// Shutdown summary counts.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ShutdownDisposition {
    /// Actors finalized normally.
    pub normally_finalized: u64,
    /// Supervisor claimed finalization after abort.
    pub supervisor_finalized: u64,
    /// Callback future failed.
    pub callback_failed: u64,
    /// Callback future aborted at deadline.
    pub callback_aborted: u64,
    /// Invariant failures during shutdown.
    pub invariant_failed: u64,
}

/// Public transaction runtime port (implementation in monoloop-loop).
pub trait TransactionRuntime: Send + Sync {
    /// Synchronously admit a transaction or return a typed error.
    fn submit(&self, request: TransactionRequest) -> Result<AdmissionReceipt, AdmissionError>;

    /// Request cancellation or forced termination.
    fn terminate(
        &self,
        selector: TransactionSelector,
        mode: TerminationMode,
    ) -> TerminationDisposition;

    /// Drain and stop the runtime.
    fn shutdown(&self, deadline: Duration) -> Shutdown;
}

/// Ordered transaction event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionEvent {
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Channel id.
    pub channel_id: ChannelId,
    /// Session id (established by this point for ordinary events).
    pub session_id: SessionId,
    /// Contiguous sequence starting at 1, including `Ended`.
    pub sequence: u64,
    /// Payload.
    pub payload: TransactionEventPayload,
}

/// Event payload variants.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TransactionEventPayload {
    /// External session identity established.
    SessionEstablished {
        /// Authoritative external id.
        external_session_id: crate::id::ExternalSessionId,
    },
    /// Complete canonical unit from Interpreter composition.
    CanonicalUnit(CanonicalUnitEvent),
    /// Host tool lifecycle.
    ToolLifecycle(ToolLifecycleEvent),
    /// Safe diagnostic.
    Diagnostic(TransactionDiagnostic),
    /// Terminal event (exactly once).
    Ended(TransactionEnd),
}

/// Bounded safe transaction diagnostic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionDiagnostic {
    /// Safe diagnostic.
    pub diagnostic: SafeDiagnostic,
}

/// Terminal transaction result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionEnd {
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Session when established.
    pub session_id: Option<SessionId>,
    /// Channel.
    pub channel_id: ChannelId,
    /// Terminal kind.
    pub kind: TransactionEndKind,
    /// Prior cause when terminal selection raced (optional).
    pub prior_terminal_cause: Option<TransactionEndKind>,
    /// Whether the terminal event was accepted by the sink.
    pub event_delivery: EventDeliveryOutcome,
    /// Number of events emitted including `Ended`.
    pub emitted_events: u64,
    /// Bounded usage facts.
    pub usage: TransactionUsage,
    /// Safe diagnostics.
    pub diagnostics: Vec<TransactionDiagnostic>,
}

/// Closed terminal kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransactionEndKind {
    /// Successful completion.
    Completed,
    /// Caller must continue (caller-controlled policy).
    ContinuationRequired,
    /// Cancelled.
    Cancelled,
    /// Force-terminated.
    Terminated,
    /// Runtime shutdown.
    RuntimeShutdown,
    /// Deadline exceeded.
    DeadlineExceeded,
    /// Channel open/attach failed.
    ChannelOpenFailed,
    /// Outbound encoding failed.
    EncodingFailed,
    /// Connector failed.
    ConnectorFailed,
    /// Interpretation failed.
    InterpretationFailed,
    /// Tool exchange failed.
    ToolExchangeFailed,
    /// Event delivery failed.
    EventDeliveryFailed,
    /// Resource limit exceeded.
    LimitExceeded,
    /// Internal invariant failed.
    InvariantFailed,
}

/// Terminal event delivery outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventDeliveryOutcome {
    /// Sink accepted.
    Accepted,
    /// Sink failed or timed out.
    Failed,
}

/// Bounded usage facts (unavailable is not zero).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransactionUsage {
    /// Provider input tokens when known.
    pub provider_input_tokens: Option<u64>,
    /// Provider output tokens when known.
    pub provider_output_tokens: Option<u64>,
    /// Number of provider exchanges.
    pub provider_exchanges: u32,
    /// Number of tool executions started.
    pub tools_started: u32,
    /// Number of tool executions completed (success or domain failure).
    pub tools_completed: u32,
}

/// Event delivery error (safe).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum EventDeliveryError {
    /// Sink rejected or failed.
    #[error("event delivery failed")]
    Failed,
    /// Delivery deadline exceeded.
    #[error("event delivery deadline exceeded")]
    DeadlineExceeded,
}

/// Completion callback delivery error (safe).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum CompletionDeliveryError {
    /// Callback failed.
    #[error("completion callback failed")]
    Failed,
    /// Callback deadline exceeded.
    #[error("completion callback deadline exceeded")]
    DeadlineExceeded,
}

/// Synchronous admission error.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{kind:?}: {message}")]
pub struct AdmissionError {
    /// Closed kind.
    pub kind: AdmissionErrorKind,
    /// Safe bounded message.
    pub message: String,
}

impl AdmissionError {
    /// Construct an admission error.
    pub fn new(kind: AdmissionErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

/// Admission error kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AdmissionErrorKind {
    /// Runtime not accepting.
    RuntimeShuttingDown,
    /// Unknown Channel id.
    UnknownChannel,
    /// Session already has an active transaction.
    SessionAlreadyActive,
    /// Unknown tool id.
    UnknownTool,
    /// Duplicate tool id in request.
    DuplicateTool,
    /// Invalid canonical input.
    InvalidInput,
    /// Invalid configuration merge.
    InvalidConfiguration,
    /// Capability mismatch for Channel/tools/session.
    CapabilityMismatch,
    /// Capacity exceeded.
    CapacityExceeded,
    /// Actor spawn failed.
    SpawnFailed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::user_text_input;

    #[test]
    fn end_kind_round_trip() {
        let kind = TransactionEndKind::Completed;
        let json = serde_json::to_string(&kind).unwrap();
        let back: TransactionEndKind = serde_json::from_str(&json).unwrap();
        assert_eq!(kind, back);
    }

    #[tokio::test]
    async fn sink_adapters_return_futures() {
        let sink = FnEventSink(|_e| Box::pin(async { Ok(()) }) as EventDelivery);
        let events: Arc<dyn TransactionEventSink> = Arc::new(sink);
        let end = TransactionEnd {
            transaction_id: TransactionId::generate(),
            session_id: None,
            channel_id: ChannelId::try_new("ch").unwrap(),
            kind: TransactionEndKind::Completed,
            prior_terminal_cause: None,
            event_delivery: EventDeliveryOutcome::Accepted,
            emitted_events: 1,
            usage: TransactionUsage::default(),
            diagnostics: vec![],
        };
        let ev = TransactionEvent {
            transaction_id: end.transaction_id,
            channel_id: end.channel_id.clone(),
            session_id: SessionId::try_new("s").unwrap(),
            sequence: 1,
            payload: TransactionEventPayload::Ended(end.clone()),
        };
        events.deliver(ev).await.unwrap();

        let cb: Box<dyn CompletionCallback> = Box::new(FnCompletionCallback(|_e| {
            Box::pin(async { Ok(()) }) as CompletionDelivery
        }));
        cb.call(end).await.unwrap();

        let _input = user_text_input("hello").unwrap();
    }
}
