//! Lifecycle ledger — continuous representation of admitted transactions (v2 §8).

use super::capacity::TransactionReservations;
use super::terminal::TerminalDecision;
use crate::transaction::sticky_cancel::StickyCancel;
use monoloop_contracts::{
    CanonicalInput, ChannelId, InvocationConfig, SessionConfig, SessionKey, ToolId,
    TransactionCompletionSender, TransactionDelivery, TransactionId, TransactionUsage,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Resource controls for cooperative cancel / shutdown wakeups.
#[derive(Debug, Clone)]
pub struct ResourceControls {
    /// Sticky cancel / shutdown signal for the coordinator (flag before notify).
    pub cancel: Arc<StickyCancel>,
}

impl Default for ResourceControls {
    fn default() -> Self {
        Self {
            cancel: Arc::new(StickyCancel::new()),
        }
    }
}

/// Per-transaction ledger row.
#[derive(Debug)]
pub struct LedgerEntry {
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Channel.
    pub channel_id: ChannelId,
    /// Session when known at admission or after claim.
    pub session_key: Option<SessionKey>,
    /// Current phase.
    pub phase: TransactionPhase,
    /// Immutable terminal decision once selected.
    pub terminal: Option<TerminalDecision>,
    /// Last allocated event sequence (0 = none yet).
    pub event_sequence: u64,
    /// Full delivery ports at admit; taken at Start (split into publisher + completion).
    pub delivery: Option<TransactionDelivery>,
    /// Completion sender retained until Seal + publish.
    pub completion_tx: Option<TransactionCompletionSender>,
    /// Command sender to this transaction's event publisher (for Seal).
    pub publisher_cmd_tx:
        Option<tokio::sync::mpsc::Sender<super::event_publisher::EventPublisherCommand>>,
    /// Canonical input captured at admission.
    pub input: CanonicalInput,
    /// Invocation configuration.
    pub invocation_config: InvocationConfig,
    /// Optional session configuration.
    pub session_config: Option<SessionConfig>,
    /// Selected tool ids.
    pub tools: Vec<ToolId>,
    /// RAII reservations.
    pub reservations: Option<TransactionReservations>,
    /// Cancel / control knobs.
    pub resources: ResourceControls,
    /// Usage facts.
    pub usage: TransactionUsage,
    /// Bounded diagnostics count.
    pub diagnostic_count: u32,
}

/// Ledger phase machine (v2 §8.2).
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

/// Source of truth from admission through completion publication.
#[derive(Debug, Default)]
pub struct LifecycleLedger {
    by_transaction: HashMap<TransactionId, LedgerEntry>,
    by_session: HashMap<SessionKey, TransactionId>,
}

impl LifecycleLedger {
    /// Empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.by_transaction.len()
    }

    /// Whether empty.
    pub fn is_empty(&self) -> bool {
        self.by_transaction.is_empty()
    }

    /// Snapshot of all transaction ids (for shutdown).
    pub fn transaction_ids(&self) -> Vec<TransactionId> {
        self.by_transaction.keys().copied().collect()
    }

    /// Lookup by id.
    pub fn get(&self, id: &TransactionId) -> Option<&LedgerEntry> {
        self.by_transaction.get(id)
    }

    /// Mutable lookup by id.
    pub fn get_mut(&mut self, id: &TransactionId) -> Option<&mut LedgerEntry> {
        self.by_transaction.get_mut(id)
    }

    /// Whether a session key is already active.
    pub fn session_active(&self, key: &SessionKey) -> bool {
        self.by_session.contains_key(key)
    }

    /// Resolve the active transaction for a session key.
    pub fn transaction_for_session(&self, key: &SessionKey) -> Option<TransactionId> {
        self.by_session.get(key).copied()
    }

    /// Count distinct active sessions on a channel (D-015 / ChannelLimits).
    pub fn distinct_sessions_on_channel(&self, channel: &ChannelId) -> usize {
        self.by_session
            .keys()
            .filter(|k| k.channel_id == *channel)
            .count()
    }

    /// Insert a complete Queued entry. Returns `Err` if id or session collides
    /// or if `max_distinct_sessions` would be exceeded for a new SessionKey.
    pub fn insert_queued(
        &mut self,
        entry: LedgerEntry,
        max_distinct_sessions: Option<usize>,
    ) -> Result<(), LedgerInsertError> {
        if self.by_transaction.contains_key(&entry.transaction_id) {
            return Err(LedgerInsertError::DuplicateTransaction);
        }
        if let Some(ref key) = entry.session_key {
            if self.by_session.contains_key(key) {
                return Err(LedgerInsertError::SessionAlreadyActive);
            }
            if let Some(max) = max_distinct_sessions {
                if self.distinct_sessions_on_channel(&key.channel_id) >= max {
                    return Err(LedgerInsertError::DistinctSessionsExceeded);
                }
            }
        }
        if let Some(ref key) = entry.session_key {
            self.by_session.insert(key.clone(), entry.transaction_id);
        }
        self.by_transaction.insert(entry.transaction_id, entry);
        Ok(())
    }

    /// Remove an entry and drop its reservations (via Drop).
    pub fn remove(&mut self, id: &TransactionId) -> Option<LedgerEntry> {
        let entry = self.by_transaction.remove(id)?;
        if let Some(ref key) = entry.session_key {
            if self.by_session.get(key) == Some(id) {
                self.by_session.remove(key);
            }
        }
        Some(entry)
    }

    /// Bind session key after external session claim (supervisor only).
    ///
    /// When `max_distinct_sessions` is `Some`, a net-new session on the channel
    /// that would exceed the bound fails with `DistinctSessionsExceeded`.
    /// Replacing an existing key on the same channel does not consume an extra
    /// distinct slot.
    pub fn bind_session(
        &mut self,
        id: &TransactionId,
        key: SessionKey,
        max_distinct_sessions: Option<usize>,
    ) -> Result<(), LedgerInsertError> {
        if self.by_session.contains_key(&key) {
            return Err(LedgerInsertError::SessionAlreadyActive);
        }
        let replacing_same_channel = self
            .by_transaction
            .get(id)
            .ok_or(LedgerInsertError::UnknownTransaction)?
            .session_key
            .as_ref()
            .is_some_and(|old| old.channel_id == key.channel_id);
        if let Some(max) = max_distinct_sessions {
            if !replacing_same_channel
                && self.distinct_sessions_on_channel(&key.channel_id) >= max
            {
                return Err(LedgerInsertError::DistinctSessionsExceeded);
            }
        }
        let entry = self
            .by_transaction
            .get_mut(id)
            .ok_or(LedgerInsertError::UnknownTransaction)?;
        if let Some(ref old) = entry.session_key {
            self.by_session.remove(old);
        }
        entry.session_key = Some(key.clone());
        self.by_session.insert(key, *id);
        Ok(())
    }
}

/// Ledger install / bind failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LedgerInsertError {
    /// Transaction id already present.
    DuplicateTransaction,
    /// Session key already has an active transaction.
    SessionAlreadyActive,
    /// Channel `max_distinct_sessions` would be exceeded.
    DistinctSessionsExceeded,
    /// Unknown transaction id.
    UnknownTransaction,
}
