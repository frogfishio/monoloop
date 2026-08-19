//! Terminal decision and completion construction (M3 scaffold).

use monoloop_contracts::{
    CleanupStatus, TerminalEventDelivery, TransactionCompletion, TransactionEndEvent,
    TransactionEndKind,
};

/// Immutable terminal decision selected once by the supervisor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalDecision {
    /// Selected end kind.
    pub kind: TransactionEndKind,
}

/// Build a completion mailbox payload from a terminal event and delivery result.
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
