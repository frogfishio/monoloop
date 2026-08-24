//! Synchronous admission with start-queue rollback (v2 §9).

use super::capacity::ReservationPool;
use super::ledger::{LedgerEntry, LedgerInsertError, ResourceControls, TransactionPhase};
use super::supervisor::{RuntimeShared, StartCommand, STATE_ACCEPTING};
use monoloop_contracts::{
    estimate_canonical_input_bytes, AdmissionError, AdmissionErrorKind, AdmissionReceipt,
    CanonicalMessage, ChannelId, McpConfigurationCapability, SessionKey, ToolExecutionMode,
    TransactionId, TransactionSubmitRequest, TransactionUsage,
};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Channel presence + capability checks used by admission (live map or registry ids).
pub(crate) trait ChannelIndex {
    fn contains(&self, id: &ChannelId) -> bool;

    /// CreationOnly + McpGateway Channels reject tool-enabled existing-session reuse (D-014).
    fn rejects_creation_only_tool_reuse(&self, id: &ChannelId) -> bool {
        let _ = id;
        false
    }

    /// `ChannelLimits.max_distinct_sessions` when known (`None` = do not enforce).
    fn max_distinct_sessions(&self, id: &ChannelId) -> Option<usize> {
        let _ = id;
        None
    }
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

    fn rejects_creation_only_tool_reuse(&self, id: &ChannelId) -> bool {
        self.get(id).is_some_and(|live| {
            live.binding.tool_mode == ToolExecutionMode::McpGateway
                && live.binding.capabilities.mcp_configuration
                    == McpConfigurationCapability::CreationOnly
        })
    }

    fn max_distinct_sessions(&self, id: &ChannelId) -> Option<usize> {
        self.get(id)
            .map(|live| live.binding.limits.max_distinct_sessions)
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

    // 2. Validate channel + tools + input bounds (D-035).
    // CanonicalInput::try_new already applied InputLimits; TransactionLimits are
    // enforced here so hosts cannot bypass max_input_bytes with roomy construction.
    if !channels.contains(&request.channel_id) {
        return Err(AdmissionError::new(
            AdmissionErrorKind::UnknownChannel,
            "unknown channel",
        ));
    }
    let limits = &shared.transaction_limits;
    let messages = request.input.messages();
    if messages.len() > limits.max_messages {
        return Err(AdmissionError::new(
            AdmissionErrorKind::InvalidInput,
            "message count exceeds max_messages",
        ));
    }
    for msg in messages {
        let parts = match msg {
            CanonicalMessage::System { content, .. }
            | CanonicalMessage::User { content, .. }
            | CanonicalMessage::Assistant { content, .. }
            | CanonicalMessage::Tool { content, .. } => content.len(),
        };
        if parts > limits.max_content_parts {
            return Err(AdmissionError::new(
                AdmissionErrorKind::InvalidInput,
                "content parts exceed max_content_parts",
            ));
        }
    }
    match estimate_canonical_input_bytes(&request.input) {
        Ok(bytes) if bytes > limits.max_input_bytes => {
            return Err(AdmissionError::new(
                AdmissionErrorKind::InvalidInput,
                "canonical input exceeds max_input_bytes",
            ));
        }
        Ok(_) => {}
        Err(_) => {
            return Err(AdmissionError::new(
                AdmissionErrorKind::InvalidInput,
                "canonical input byte estimate failed",
            ));
        }
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

    // D-014: CreationOnly cannot install MCP on an existing session — reject
    // tool-enabled reuse at admission (CapabilityMismatch), not mid-coordinator.
    if request.session_id.is_some()
        && !request.tools.is_empty()
        && channels.rejects_creation_only_tool_reuse(&request.channel_id)
    {
        return Err(AdmissionError::new(
            AdmissionErrorKind::CapabilityMismatch,
            "CreationOnly rejects tool-enabled existing-session reuse",
        ));
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

    let max_distinct = channels.max_distinct_sessions(&request.channel_id);

    let entry = LedgerEntry {
        transaction_id,
        channel_id: request.channel_id.clone(),
        session_key: session_key.clone(),
        phase: TransactionPhase::Queued,
        terminal: None,
        pending_worker_proposal: None,
        event_sequence: 0,
        delivery: Some(request.delivery),
        completion_tx: None,
        publisher_cmd_tx: None,
        publisher_seal_tx: None,
        input: request.input,
        invocation_config: request.invocation_config,
        session_config: request.session_config,
        tools: request.tools,
        reservations: Some(reservations),
        resources: ResourceControls::default(),
        usage: TransactionUsage::default(),
        diagnostic_count: 0,
    };

    if let Err(e) = ledger.insert_queued(entry, max_distinct) {
        drop(ledger);
        let kind = match e {
            LedgerInsertError::SessionAlreadyActive => AdmissionErrorKind::SessionAlreadyActive,
            LedgerInsertError::DistinctSessionsExceeded => AdmissionErrorKind::CapacityExceeded,
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
