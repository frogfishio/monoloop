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

/// Deprecated v1 sink-shaped submit request (host callbacks in the request).
///
/// **M7:** Core assemblers MUST use [`TransactionSubmitRequest`] +
/// [`crate::delivery::transaction_delivery`]. Host adapters
/// (`adapt_event_sink` / `adapt_completion_callback` in `monoloop-loop`) may
/// still drain push receivers into [`TransactionEventSink`] /
/// [`CompletionCallback`] **outside** the runtime.
#[deprecated(
    note = "use TransactionSubmitRequest with transaction_delivery(); sink/callback fields are not a core submit API (M7)"
)]
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
    /// Host event sink (v1; not accepted by `StartedRuntime` / `TransactionRuntimeHandle`).
    pub events: Arc<dyn TransactionEventSink>,
    /// Host completion callback (v1; not accepted by `StartedRuntime` / `TransactionRuntimeHandle`).
    pub completion: Box<dyn CompletionCallback>,
}

/// Runtime v2 submission request — concrete mailboxes, no host traits in-core.
pub struct TransactionSubmitRequest {
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
    /// Library-created delivery ports (caller holds the receiver half).
    pub delivery: crate::delivery::TransactionDelivery,
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
    /// Control queue was full — request not enqueued (Law 22 fail-closed; D-039).
    ///
    /// This is **not** [`Self::AlreadyTerminal`]: the transaction may still be live.
    ControlCapacityExceeded,
    /// Control queue closed (runtime stopping / stopped) — request not enqueued.
    RuntimeClosed,
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

/// Deprecated v1 runtime port (sink-shaped [`TransactionRequest`] submit).
///
/// **M7:** Production code uses `StartedRuntime` /
/// `TransactionRuntimeHandle::submit(TransactionSubmitRequest)` in
/// `monoloop-loop`. No live v2 type implements this trait.
#[deprecated(
    note = "use StartedRuntime / TransactionRuntimeHandle with TransactionSubmitRequest (M7)"
)]
pub trait TransactionRuntime: Send + Sync {
    /// Synchronously admit a transaction or return a typed error.
    #[allow(deprecated)]
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
///
/// Live assistant text arrives only as [`Self::CanonicalUnit`] (complete units).
/// There is **no** token / delta stream on this port.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TransactionEventPayload {
    /// External session identity established.
    SessionEstablished {
        /// Authoritative external id.
        external_session_id: crate::id::ExternalSessionId,
    },
    /// Complete canonical unit from Interpreter composition (not a token delta).
    CanonicalUnit(CanonicalUnitEvent),
    /// Host tool lifecycle.
    ToolLifecycle(ToolLifecycleEvent),
    /// Safe diagnostic.
    Diagnostic(TransactionDiagnostic),
    /// Terminal event (exactly once) — legacy v1 shape with embedded delivery.
    Ended(TransactionEnd),
    /// Terminal event body without self-referential delivery (Runtime v2).
    EndedEvent(TransactionEndEvent),
}

/// Bounded safe transaction diagnostic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionDiagnostic {
    /// Safe diagnostic.
    pub diagnostic: SafeDiagnostic,
}

/// Terminal transaction result.
///
/// **Legacy (v1):** embeds `event_delivery` inside the terminal event itself.
/// Runtime v2 publishes [`TransactionEndEvent`] on the event stream and reports
/// delivery/cleanup on [`TransactionCompletion`] instead.
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

/// Terminal event body for the v2 event stream (no self-referential delivery).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionEndEvent {
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Session when established.
    pub session_id: Option<SessionId>,
    /// Channel.
    pub channel_id: ChannelId,
    /// Terminal kind.
    pub kind: TransactionEndKind,
    /// Number of events emitted including this terminal event.
    pub emitted_events: u64,
    /// Bounded usage facts.
    pub usage: TransactionUsage,
    /// Safe diagnostics.
    pub diagnostics: Vec<TransactionDiagnostic>,
}

/// Outcome of attempting to enqueue the terminal `Ended` event (v2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TerminalEventDelivery {
    /// Terminal event was accepted by the event mailbox.
    Published,
    /// Event receiver was dropped / channel closed.
    QueueClosed,
    /// Terminal-event budget elapsed before enqueue.
    DeadlineExceeded,
    /// Item or byte capacity rejected the terminal event.
    LimitExceeded,
    /// No publisher / Seal was ever attempted (e.g. shutdown before Start).
    ///
    /// Spec §6.4 / D-041: never-attempted is **not** [`Self::Published`].
    NotAttempted,
}

/// Status of owned cleanup after completion publication (v2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CleanupStatus {
    /// All owned tasks/processes have been observed finished.
    Complete,
    /// Completion was published while owned work remains.
    Pending {
        /// Owned Tokio tasks still registered.
        owned_tasks: u32,
        /// Owned child processes still registered.
        owned_processes: u32,
        /// Cooperative in-process tools still outstanding.
        cooperative_tools: u32,
    },
    /// Cleanup failed with a closed code.
    Failed {
        /// Stable cleanup failure code.
        code: CleanupFailureCode,
    },
}

/// Closed cleanup failure codes (v2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CleanupFailureCode {
    /// Join observed a panic.
    TaskPanicked,
    /// Process reap failed.
    ProcessReapFailed,
    /// Internal ownership invariant broken.
    InvariantFailed,
}

/// One-shot completion mailbox payload (v2).
///
/// Separates terminal event data from terminal-event delivery and cleanup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionCompletion {
    /// Terminal event body (also published on the event stream when possible).
    pub end: TransactionEndEvent,
    /// Result of the terminal event enqueue attempt, or [`TerminalEventDelivery::NotAttempted`]
    /// when Seal / `Ended` was never issued.
    pub terminal_event_delivery: TerminalEventDelivery,
    /// Whether owned cleanup is complete.
    pub cleanup: CleanupStatus,
}

/// Wait outcome for [`crate`] runtime owner shutdown (v2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShutdownWaitOutcome {
    /// Stopped invariants hold; shutdown generation is complete.
    Stopped(ShutdownReport),
    /// Wait deadline elapsed; runtime remains `Quiescing` and retains ownership.
    TimedOut(ShutdownSnapshot),
}

/// Final shutdown report when the runtime reaches `Stopped` (v2).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ShutdownReport {
    /// Admitted transactions that received a completion publication attempt.
    pub completions_published: u64,
    /// Completions where the host had dropped its receiver.
    pub completions_receiver_dropped: u64,
    /// Completions that hit an invariant on the sender.
    pub completions_invariant_failed: u64,
    /// Transactions terminated because of runtime shutdown.
    pub runtime_shutdown_terminals: u64,
}

/// Point-in-time shutdown progress while still `Quiescing` (v2).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ShutdownSnapshot {
    /// Shutdown generation id (shared by concurrent waiters).
    pub generation: u64,
    /// Ledger entries still present.
    pub ledger_entries: u32,
    /// Owned Tokio tasks still registered.
    pub owned_tasks: u32,
    /// Owned child processes still registered.
    pub owned_processes: u32,
    /// Outstanding MCP routes.
    pub mcp_routes: u32,
    /// Completion publications attempted so far in this generation.
    pub completions_published: u64,
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
