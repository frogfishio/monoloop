//! Lifecycle ledger types (M2 scaffold).

use monoloop_contracts::{
    ChannelId, SessionKey, TransactionDelivery, TransactionId, TransactionUsage,
};

/// Continuous representation of an admitted transaction until completion publish.
#[derive(Debug, Default)]
pub struct LifecycleLedger {
    _entries: usize,
}

/// Per-transaction ledger row (fields land fully in M2).
#[derive(Debug)]
pub struct LedgerEntry {
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Channel.
    pub channel_id: ChannelId,
    /// Session when known.
    pub session_key: Option<SessionKey>,
    /// Current phase.
    pub phase: TransactionPhase,
    /// Delivery ports (taken at completion publish).
    pub delivery: Option<TransactionDelivery>,
    /// Usage facts.
    pub usage: TransactionUsage,
}

/// Ledger phase machine (v2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TransactionPhase {
    /// Admitted; supervisor has not started work.
    Queued,
    /// External session create/load in progress.
    EstablishingSession,
    /// Provider/tool work running.
    Running,
    /// Cancellation in progress.
    Cancelling,
    /// Terminal selected; publishing.
    Finalizing,
    /// Completion published; cleanup already done.
    CompletionPublished,
    /// Completion published; owned cleanup remains.
    CleanupPending,
}

impl LifecycleLedger {
    /// Empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of entries (scaffold).
    pub fn len(&self) -> usize {
        self._entries
    }

    /// Whether the ledger is empty.
    pub fn is_empty(&self) -> bool {
        self._entries == 0
    }
}
