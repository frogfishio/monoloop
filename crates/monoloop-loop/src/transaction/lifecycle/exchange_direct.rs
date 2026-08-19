//! DirectLlm exchange helper for M3.
//!
//! Concurrent Fake open/pump/interpret is deferred to M4 (`ExchangeContext` /
//! TaskSpawner). M3 returns a deterministic synthetic completed text unit so
//! EventPublisher sequencing and Seal can be proven end-to-end.

use monoloop_connector::Connector;
use monoloop_contracts::{
    CanonicalUnit, CanonicalUnitEvent, CanonicalUnitSnapshot, ConnectionId, FlowId,
    InterpretationId, LaneId, OutboundDialectEncoder, TextChannel, TextSentence,
    TransactionEndKind, TransactionId, UnitId, UnitState,
};
use monoloop_interpreter::InterpreterFactory;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Outcome of one DirectLlm exchange.
pub struct DirectExchangeOutcome {
    /// Complete unit lifecycle events observed.
    pub units: Vec<CanonicalUnitEvent>,
    /// Mapped terminal proposal kind.
    pub terminal: TransactionEndKind,
}

/// Run one DirectLlm exchange (M3 synthetic path).
#[allow(clippy::too_many_arguments)]
pub async fn run_direct_llm_exchange(
    _transaction_id: TransactionId,
    _connector: &dyn Connector,
    _encoder: &dyn OutboundDialectEncoder,
    _interpreter: &dyn InterpreterFactory,
    _endpoint_ref: &str,
    _credential_ref: Option<&str>,
    input: &monoloop_contracts::CanonicalInput,
    _config: &monoloop_contracts::EffectiveConfig,
    cancel: Arc<Notify>,
    _deadline: Duration,
) -> DirectExchangeOutcome {
    // Cooperative cancel probe (does not wait for a future notify).
    tokio::select! {
        biased;
        _ = cancel.notified() => {
            return DirectExchangeOutcome {
                units: vec![],
                terminal: TransactionEndKind::Cancelled,
            };
        }
        _ = std::future::ready(()) => {}
    }

    let content = "ok.".to_string();
    let _ = input;
    let unit_id = UnitId::new(uuid::Uuid::new_v4().to_string());
    let snapshot = CanonicalUnitSnapshot {
        unit_id: unit_id.clone(),
        unit_generation: 1,
        unit_state: UnitState::Complete,
        interpretation_id: InterpretationId::generate(),
        connection_id: ConnectionId::generate(),
        external_session_id: None,
        flow_id: FlowId::main(),
        lane_id: LaneId::response(),
        lane_ordinal: 1,
        causal_parent_id: None,
        source_time: None,
        source_step: None,
        unit: CanonicalUnit::Text(TextSentence {
            sentence_id: unit_id,
            channel: TextChannel::PublicResponse,
            paragraph_id: None,
            sentence_ordinal: 1,
            content,
        }),
    };

    DirectExchangeOutcome {
        units: vec![CanonicalUnitEvent::Completed(snapshot)],
        terminal: TransactionEndKind::Completed,
    }
}
