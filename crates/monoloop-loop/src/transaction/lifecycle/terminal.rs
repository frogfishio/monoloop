//! Terminal decision, proposals, and completion construction (v2 §13).

use monoloop_contracts::{
    CleanupStatus, TerminalEventDelivery, TransactionCompletion, TransactionEndEvent,
    TransactionEndKind, TransactionId,
};

/// Immutable terminal decision selected once by the supervisor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalDecision {
    /// Selected end kind.
    pub kind: TransactionEndKind,
}

impl TerminalDecision {
    /// Construct a decision.
    pub fn new(kind: TransactionEndKind) -> Self {
        Self { kind }
    }
}

/// Coordinator proposal — supervisor accepts or upgrades (Cancel→Terminated).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalProposal {
    /// Proposed primary cause.
    pub kind: TransactionEndKind,
}

impl TerminalProposal {
    /// Construct a proposal.
    pub fn new(kind: TransactionEndKind) -> Self {
        Self { kind }
    }
}

/// Build a completion mailbox payload.
pub fn build_completion(
    end: TransactionEndEvent,
    terminal_event_delivery: TerminalEventDelivery,
    cleanup: CleanupStatus,
) -> TransactionCompletion {
    TransactionCompletion {
        end,
        terminal_event_delivery,
        cleanup,
    }
}

/// Build a terminal event body.
pub fn end_event(
    transaction_id: TransactionId,
    channel_id: monoloop_contracts::ChannelId,
    session_id: Option<monoloop_contracts::SessionId>,
    kind: TransactionEndKind,
    emitted_events: u64,
) -> TransactionEndEvent {
    TransactionEndEvent {
        transaction_id,
        session_id,
        channel_id,
        kind,
        emitted_events,
        usage: monoloop_contracts::TransactionUsage::default(),
        diagnostics: Vec::new(),
    }
}
