//! Dual-index active transaction registry (TransactionId + SessionKey).

use super::finalization::FinalizationGuard;
use monoloop_contracts::{ChannelId, SessionKey, TransactionId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Control message for an in-flight transaction actor.
#[derive(Debug)]
pub enum ControlMessage {
    /// Cooperative cancel.
    Cancel,
    /// Forced terminate.
    ForceTerminate,
}

/// One active transaction entry.
pub struct ActiveTransaction {
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Session key when known (None while creating external session).
    pub session_key: Option<SessionKey>,
    /// Channel id for capacity release / diagnostics.
    #[allow(dead_code)]
    pub channel_id: ChannelId,
    /// Finalization guard (actor + shutdown supervisor).
    pub guard: Arc<FinalizationGuard>,
    /// Control sender (capacity 1).
    pub control_tx: mpsc::Sender<ControlMessage>,
    /// Join handle for actor (+ delivery reaper).
    pub actor_join: tokio::task::JoinHandle<()>,
    /// Once-only capacity release (actor finalize and/or shutdown supervisor).
    pub release_capacity: Arc<dyn Fn() + Send + Sync>,
}

/// Registry of admitted transactions.
#[derive(Default)]
pub struct ActiveTransactionRegistry {
    by_tx: HashMap<TransactionId, ActiveTransaction>,
    by_session: HashMap<SessionKey, TransactionId>,
}

impl ActiveTransactionRegistry {
    /// Create empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of active transactions.
    pub fn len(&self) -> usize {
        self.by_tx.len()
    }

    /// Whether empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.by_tx.is_empty()
    }

    /// True if session key is already active.
    pub fn session_active(&self, key: &SessionKey) -> bool {
        self.by_session.contains_key(key)
    }

    /// Install a new active transaction. Fails if SessionKey already active.
    pub fn insert(
        &mut self,
        entry: ActiveTransaction,
    ) -> Result<(), monoloop_contracts::AdmissionErrorKind> {
        if let Some(ref sk) = entry.session_key {
            if self.by_session.contains_key(sk) {
                return Err(monoloop_contracts::AdmissionErrorKind::SessionAlreadyActive);
            }
        }
        if self.by_tx.contains_key(&entry.transaction_id) {
            return Err(monoloop_contracts::AdmissionErrorKind::SpawnFailed);
        }
        if let Some(ref sk) = entry.session_key {
            self.by_session.insert(sk.clone(), entry.transaction_id);
        }
        self.by_tx.insert(entry.transaction_id, entry);
        Ok(())
    }

    /// Claim a SessionKey for a provisional transaction.
    pub fn claim_session(
        &mut self,
        transaction_id: TransactionId,
        key: SessionKey,
    ) -> Result<(), ClaimSessionError> {
        if self.by_session.contains_key(&key) {
            return Err(ClaimSessionError::Collision);
        }
        let entry = self
            .by_tx
            .get_mut(&transaction_id)
            .ok_or(ClaimSessionError::UnknownTransaction)?;
        if entry.session_key.is_some() {
            return Err(ClaimSessionError::AlreadyClaimed);
        }
        entry.session_key = Some(key.clone());
        entry.guard.set_session_id(key.session_id.clone());
        self.by_session.insert(key, transaction_id);
        Ok(())
    }

    /// Get control sender by transaction id.
    pub fn control_tx(&self, id: &TransactionId) -> Option<mpsc::Sender<ControlMessage>> {
        self.by_tx.get(id).map(|e| e.control_tx.clone())
    }

    /// Get control sender by session key.
    pub fn control_tx_by_session(&self, key: &SessionKey) -> Option<mpsc::Sender<ControlMessage>> {
        let id = self.by_session.get(key)?;
        self.control_tx(id)
    }

    /// Remove and return entry by transaction id.
    pub fn remove(&mut self, id: &TransactionId) -> Option<ActiveTransaction> {
        let entry = self.by_tx.remove(id)?;
        if let Some(ref sk) = entry.session_key {
            self.by_session.remove(sk);
        }
        Some(entry)
    }

    /// Snapshot transaction ids.
    #[allow(dead_code)]
    pub fn transaction_ids(&self) -> Vec<TransactionId> {
        self.by_tx.keys().copied().collect()
    }

    /// Drain all entries (shutdown).
    pub fn drain_all(&mut self) -> Vec<ActiveTransaction> {
        self.by_session.clear();
        self.by_tx.drain().map(|(_, v)| v).collect()
    }

    /// Guard if still active.
    #[allow(dead_code)]
    pub fn guard(&self, id: &TransactionId) -> Option<Arc<FinalizationGuard>> {
        self.by_tx.get(id).map(|e| Arc::clone(&e.guard))
    }
}

/// Session key claim failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimSessionError {
    /// Another transaction holds the key.
    Collision,
    /// Unknown transaction.
    UnknownTransaction,
    /// Already has a session key.
    AlreadyClaimed,
}
