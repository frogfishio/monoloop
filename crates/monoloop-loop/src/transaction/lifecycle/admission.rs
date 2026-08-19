//! Synchronous admission with start-queue rollback (v2 §9).

use super::capacity::ReservationPool;
use super::ledger::{LedgerEntry, LedgerInsertError, ResourceControls, TransactionPhase};
use super::supervisor::{RuntimeShared, StartCommand, STATE_ACCEPTING};
use monoloop_contracts::{
    AdmissionError, AdmissionErrorKind, AdmissionReceipt, ChannelId, SessionKey, TransactionId,
    TransactionSubmitRequest, TransactionUsage,
};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Channel presence check used by admission (live map or registry ids).
pub(crate) trait ChannelIndex {
    fn contains(&self, id: &ChannelId) -> bool;
}

impl ChannelIndex for HashMap<ChannelId, ()> {
    fn contains(&self, id: &ChannelId) -> bool {
        self.contains_key(id)
    }
}

impl ChannelIndex for HashMap<ChannelId, super::super::channel_registry::LiveChannel> {
    fn contains(&self, id: &ChannelId) -> bool {
        self.contains_key(id)
    }
}

/// Perform synchronous admission (no spawn, no executor wait).
pub(crate) fn admit(
    shared: &Arc<RuntimeShared>,
    pool: &Arc<ReservationPool>,
    channels: &impl ChannelIndex,
    tools: &super::super::host_tools::HostToolRegistry,
    max_tools: usize,
    request: TransactionSubmitRequest,
) -> Result<AdmissionReceipt, AdmissionError> {
    // 1. Runtime accepting?
    if shared.state.load(Ordering::SeqCst) != STATE_ACCEPTING {
        return Err(AdmissionError::new(
            AdmissionErrorKind::RuntimeShuttingDown,
            "runtime not accepting",
        ));
    }

    // 2. Validate channel + tools (input already constructed via CanonicalInput::try_new).
    if !channels.contains(&request.channel_id) {
        return Err(AdmissionError::new(
            AdmissionErrorKind::UnknownChannel,
            "unknown channel",
        ));
    }
    if request.tools.len() > max_tools {
        return Err(AdmissionError::new(
            AdmissionErrorKind::InvalidConfiguration,
            "too many tools",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for tool_id in &request.tools {
        if !seen.insert(tool_id.clone()) {
            return Err(AdmissionError::new(
                AdmissionErrorKind::DuplicateTool,
                "duplicate tool id",
            ));
        }
        if tools.get(tool_id).is_none() {
            return Err(AdmissionError::new(
                AdmissionErrorKind::UnknownTool,
                "unknown tool id",
            ));
        }
    }

    // 5. Allocate ids / session key when known.
    let transaction_id = TransactionId::generate();
    let session_key = request.session_id.as_ref().map(|sid| SessionKey {
        channel_id: request.channel_id.clone(),
        session_id: sid.clone(),
    });

    // 6–7. RAII reservations (fail closed, no wait).
    let reservations = pool.try_reserve(&request.channel_id).ok_or_else(|| {
        AdmissionError::new(AdmissionErrorKind::CapacityExceeded, "capacity exceeded")
    })?;

    // 8–12. Ledger critical section + start command.
    let mut ledger = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());

    // 9. Recheck state under lock.
    if shared.state.load(Ordering::SeqCst) != STATE_ACCEPTING {
        drop(ledger);
        drop(reservations);
        return Err(AdmissionError::new(
            AdmissionErrorKind::RuntimeShuttingDown,
            "runtime not accepting",
        ));
    }

    // 10. Duplicate session.
    if let Some(ref key) = session_key {
        if ledger.session_active(key) {
            drop(ledger);
            drop(reservations);
            return Err(AdmissionError::new(
                AdmissionErrorKind::SessionAlreadyActive,
                "session already active",
            ));
        }
    }

    let entry = LedgerEntry {
        transaction_id,
        channel_id: request.channel_id.clone(),
        session_key: session_key.clone(),
        phase: TransactionPhase::Queued,
        terminal: None,
        event_sequence: 0,
        delivery: Some(request.delivery),
        reservations: Some(reservations),
        resources: ResourceControls::default(),
        usage: TransactionUsage::default(),
        diagnostic_count: 0,
    };

    if let Err(e) = ledger.insert_queued(entry) {
        drop(ledger);
        let kind = match e {
            LedgerInsertError::SessionAlreadyActive => AdmissionErrorKind::SessionAlreadyActive,
            LedgerInsertError::DuplicateTransaction => AdmissionErrorKind::CapacityExceeded,
            LedgerInsertError::UnknownTransaction => AdmissionErrorKind::CapacityExceeded,
        };
        return Err(AdmissionError::new(kind, "ledger insert failed"));
    }

    // 12. try_send Start on the dedicated start queue while rollback remains possible.
    match shared
        .start_tx
        .try_send(StartCommand::Start(transaction_id))
    {
        Ok(()) => {
            drop(ledger);
            Ok(AdmissionReceipt {
                transaction_id,
                session_id: request.session_id,
            })
        }
        Err(_) => {
            // 13. Rollback ledger + reservations (Drop of entry).
            let _ = ledger.remove(&transaction_id);
            drop(ledger);
            Err(AdmissionError::new(
                AdmissionErrorKind::SpawnFailed,
                "supervisor start queue full",
            ))
        }
    }
}
