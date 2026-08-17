//! Transaction actor: session establish + one provider exchange + finalization.

use super::active_registry::{ActiveTransactionRegistry, ClaimSessionError, ControlMessage};
use super::events::QueuedEvent;
use super::exchange::{run_exchange, ExchangeFailure, ExchangeParams};
use super::finalization::{build_transaction_end, FinalizationGuard};
use super::resolved_tools::ResolvedToolSet;
use monoloop_connector::{Connector, SessionAdapter};
use monoloop_contracts::{
    CanonicalUnitEvent, ChannelId, ChannelKind, EffectiveConfig, EventDeliveryOutcome,
    ExternalSessionId, InterpretationLimits, OutboundDialectEncoder, SessionId, SessionKey,
    ToolExecutionMode, TransactionEndKind, TransactionEvent, TransactionEventPayload,
    TransactionId,
};
use monoloop_interpreter::InterpreterFactory;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// Inputs for the transaction actor.
pub struct ActorSpawn {
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Channel id.
    pub channel_id: ChannelId,
    /// Channel kind (session topology).
    #[allow(dead_code)]
    pub channel_kind: ChannelKind,
    /// Tool execution mode (controls Loop fan-out; MCP/None skip).
    pub tool_mode: ToolExecutionMode,
    /// Session key if known at admission.
    pub session_key: Option<SessionKey>,
    /// Provisional external create (no session yet).
    pub provisional_external: bool,
    /// Session adapter for external Channels.
    pub sessions: Option<Arc<dyn SessionAdapter>>,
    /// Connector for open/send/receive.
    pub connector: Arc<dyn Connector>,
    /// Outbound encoder.
    pub encoder: Arc<dyn OutboundDialectEncoder>,
    /// Interpreter factory.
    pub interpreter: Arc<dyn InterpreterFactory>,
    /// Endpoint ref for open.
    pub endpoint_ref: String,
    /// Credential ref.
    pub credential_ref: Option<String>,
    /// Canonical input.
    pub input: monoloop_contracts::CanonicalInput,
    /// Effective configuration.
    pub effective: EffectiveConfig,
    /// Resolved tools (specs for encoder).
    pub tools: ResolvedToolSet,
    /// Finalization guard.
    pub guard: Arc<FinalizationGuard>,
    /// Control receiver (capacity 1).
    pub control_rx: mpsc::Receiver<ControlMessage>,
    /// Event queue to delivery task.
    pub event_tx: mpsc::Sender<QueuedEvent>,
    /// Delivery failure signal.
    pub delivery_fail_rx: mpsc::Receiver<()>,
    /// Shared registry.
    pub registry: Arc<Mutex<ActiveTransactionRegistry>>,
    /// Capacity release on exit.
    pub release_capacity: Arc<dyn Fn() + Send + Sync>,
    /// Transaction deadline.
    pub deadline: Duration,
}

/// Spawn the actor task.
pub fn spawn_actor(spawn: ActorSpawn) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_actor(spawn).await;
    })
}

struct ActorResult {
    kind: TransactionEndKind,
    prior: Option<TransactionEndKind>,
    delivery: EventDeliveryOutcome,
    session_key: Option<SessionKey>,
}

async fn run_actor(spawn: ActorSpawn) {
    let ActorSpawn {
        transaction_id,
        channel_id,
        channel_kind: _,
        tool_mode,
        mut session_key,
        provisional_external,
        sessions,
        connector,
        encoder,
        interpreter,
        endpoint_ref,
        credential_ref,
        input,
        effective,
        tools,
        guard,
        mut control_rx,
        event_tx,
        mut delivery_fail_rx,
        registry,
        release_capacity,
        deadline,
    } = spawn;

    let mut terminal_kind = TransactionEndKind::Completed;
    let mut attachment: Option<Arc<monoloop_connector::SessionAttachment>> = None;

    let work = async {
        // --- EstablishingSession (provisional external) ---
        if provisional_external {
            // WP-05: synthetic session id when no real attach race; still claim SessionKey.
            // If a SessionAdapter is present, prefer begin_attach for real lifecycle.
            if let Some(ref adapter) = sessions {
                let req = monoloop_connector::SessionAttachRequest {
                    transaction_id,
                    channel_id: channel_id.clone(),
                    requested_session_id: None,
                    session_config: effective.session.clone(),
                    initial_mcp: None,
                    deadline: std::time::Instant::now() + deadline,
                };
                let pending = adapter
                    .begin_attach(req)
                    .map_err(|_| TransactionEndKind::ChannelOpenFailed)?;
                let att = tokio::select! {
                    biased;
                    ctrl = control_rx.recv() => {
                        let _ = pending.control.cancel();
                        return Err(match ctrl {
                            Some(ControlMessage::ForceTerminate) => TransactionEndKind::Terminated,
                            _ => TransactionEndKind::Cancelled,
                        });
                    }
                    r = pending.completion => {
                        r.map_err(|_| TransactionEndKind::ChannelOpenFailed)?
                    }
                };
                let sid = SessionId::from_external(&att.external_session_id);
                let key = SessionKey::new(channel_id.clone(), sid.clone());
                {
                    let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
                    match reg.claim_session(transaction_id, key.clone()) {
                        Ok(()) => {}
                        Err(ClaimSessionError::Collision) => {
                            return Err(TransactionEndKind::InvariantFailed);
                        }
                        Err(_) => return Err(TransactionEndKind::InvariantFailed),
                    }
                }
                session_key = Some(key);
                if !emit_unit_or_session(
                    &event_tx,
                    &guard,
                    transaction_id,
                    &channel_id,
                    &session_key,
                    TransactionEventPayload::SessionEstablished {
                        external_session_id: att.external_session_id.clone(),
                    },
                )
                .await
                {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
                attachment = Some(att);
            } else {
                let sid = SessionId::generate();
                let key = SessionKey::new(channel_id.clone(), sid.clone());
                {
                    let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
                    match reg.claim_session(transaction_id, key.clone()) {
                        Ok(()) => {}
                        Err(ClaimSessionError::Collision) => {
                            return Err(TransactionEndKind::InvariantFailed);
                        }
                        Err(_) => return Err(TransactionEndKind::InvariantFailed),
                    }
                }
                session_key = Some(key);
                let ext = ExternalSessionId::try_new(sid.as_str())
                    .map_err(|_| TransactionEndKind::InvariantFailed)?;
                if !emit_unit_or_session(
                    &event_tx,
                    &guard,
                    transaction_id,
                    &channel_id,
                    &session_key,
                    TransactionEventPayload::SessionEstablished {
                        external_session_id: ext,
                    },
                )
                .await
                {
                    return Err(TransactionEndKind::EventDeliveryFailed);
                }
            }
        }

        // ActivatingTools: no-op for ModelToolCalls/None; MCP pending WP-07.
        let _ = tool_mode;

        // --- One provider exchange ---
        let tool_specs: Vec<_> = tools.specs().into_iter().cloned().collect();
        let outcome = tokio::select! {
            biased;
            ctrl = control_rx.recv() => {
                return Err(match ctrl {
                    Some(ControlMessage::ForceTerminate) => TransactionEndKind::Terminated,
                    _ => TransactionEndKind::Cancelled,
                });
            }
            r = run_exchange(ExchangeParams {
                transaction_id,
                connector: connector.as_ref(),
                encoder: encoder.as_ref(),
                interpreter: interpreter.as_ref(),
                endpoint_ref: &endpoint_ref,
                credential_ref: credential_ref.as_deref(),
                session_attachment: attachment.clone(),
                input: &input,
                config: &effective,
                tools: &tool_specs,
                interpretation_limits: InterpretationLimits::default(),
                deadline,
            }) => r,
        };

        let outcome = outcome.map_err(map_exchange_failure)?;

        // Publish canonical units (no raw bytes ever enter actor queues).
        for unit in outcome.units {
            if !emit_canonical_unit(
                &event_tx,
                &guard,
                transaction_id,
                &channel_id,
                &session_key,
                unit,
            )
            .await
            {
                return Err(TransactionEndKind::EventDeliveryFailed);
            }
        }

        if let Some(fail) = outcome.failure {
            return Err(map_exchange_failure(fail));
        }

        Ok::<(), TransactionEndKind>(())
    };

    // Race work against deadline / delivery failure only (control is selected inside work).
    let cancelled = tokio::select! {
        biased;
        fail = delivery_fail_rx.recv() => {
            if fail.is_some() {
                terminal_kind = TransactionEndKind::EventDeliveryFailed;
            }
            true
        }
        _ = tokio::time::sleep(deadline) => {
            terminal_kind = TransactionEndKind::DeadlineExceeded;
            true
        }
        work_res = work => {
            match work_res {
                Ok(()) => {
                    terminal_kind = TransactionEndKind::Completed;
                }
                Err(k) => terminal_kind = k,
            }
            false
        }
    };
    let _ = cancelled;

    let result = ActorResult {
        kind: terminal_kind,
        prior: None,
        delivery: EventDeliveryOutcome::Accepted,
        session_key: session_key.clone(),
    };

    finalize_and_cleanup(
        transaction_id,
        channel_id,
        guard,
        event_tx,
        registry,
        release_capacity,
        result,
    )
    .await;
}

fn map_exchange_failure(f: ExchangeFailure) -> TransactionEndKind {
    match f {
        ExchangeFailure::ChannelOpenFailed => TransactionEndKind::ChannelOpenFailed,
        ExchangeFailure::EncodingFailed => TransactionEndKind::EncodingFailed,
        ExchangeFailure::ConnectorFailed => TransactionEndKind::ConnectorFailed,
        ExchangeFailure::InterpretationFailed => TransactionEndKind::InterpretationFailed,
        ExchangeFailure::Cancelled => TransactionEndKind::Cancelled,
        ExchangeFailure::Terminated => TransactionEndKind::Terminated,
    }
}

async fn emit_unit_or_session(
    event_tx: &mpsc::Sender<QueuedEvent>,
    guard: &FinalizationGuard,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    session_key: &Option<SessionKey>,
    payload: TransactionEventPayload,
) -> bool {
    let seq = guard.sequencer().allocate();
    let session_id = session_key
        .as_ref()
        .map(|k| k.session_id.clone())
        .unwrap_or_else(SessionId::generate);
    let event = TransactionEvent {
        transaction_id,
        channel_id: channel_id.clone(),
        session_id,
        sequence: seq,
        payload,
    };
    event_tx
        .send(QueuedEvent { event, ack: None })
        .await
        .is_ok()
}

async fn emit_canonical_unit(
    event_tx: &mpsc::Sender<QueuedEvent>,
    guard: &FinalizationGuard,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    session_key: &Option<SessionKey>,
    unit: CanonicalUnitEvent,
) -> bool {
    emit_unit_or_session(
        event_tx,
        guard,
        transaction_id,
        channel_id,
        session_key,
        TransactionEventPayload::CanonicalUnit(unit),
    )
    .await
}

async fn finalize_and_cleanup(
    transaction_id: TransactionId,
    channel_id: ChannelId,
    guard: Arc<FinalizationGuard>,
    event_tx: mpsc::Sender<QueuedEvent>,
    registry: Arc<Mutex<ActiveTransactionRegistry>>,
    release_capacity: Arc<dyn Fn() + Send + Sync>,
    result: ActorResult,
) {
    let Some(payload) = guard.try_claim() else {
        release_capacity();
        let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
        let _ = reg.remove(&transaction_id);
        return;
    };

    let session_for_event = payload
        .session_id
        .clone()
        .or_else(|| result.session_key.as_ref().map(|k| k.session_id.clone()))
        .unwrap_or_else(SessionId::generate);

    let seq = guard.sequencer().allocate();
    let mut kind = result.kind;
    let mut prior = result.prior;
    let mut delivery = result.delivery;

    let end_preview = build_transaction_end(&payload, kind, prior, delivery, seq);
    let event = TransactionEvent {
        transaction_id: payload.transaction_id,
        channel_id: channel_id.clone(),
        session_id: session_for_event,
        sequence: seq,
        payload: TransactionEventPayload::Ended(end_preview),
    };

    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    let send_ok = event_tx
        .send(QueuedEvent {
            event,
            ack: Some(ack_tx),
        })
        .await
        .is_ok();

    if !send_ok {
        delivery = EventDeliveryOutcome::Failed;
        prior = Some(kind);
        kind = TransactionEndKind::EventDeliveryFailed;
    } else {
        match tokio::time::timeout(Duration::from_secs(5), ack_rx).await {
            Ok(Ok(Ok(()))) => {}
            _ => {
                delivery = EventDeliveryOutcome::Failed;
                prior = Some(kind);
                kind = TransactionEndKind::EventDeliveryFailed;
            }
        }
    }

    let end = build_transaction_end(&payload, kind, prior, delivery, seq);
    drop(event_tx);

    {
        let mut reg = registry.lock().unwrap_or_else(|e| e.into_inner());
        let _ = reg.remove(&transaction_id);
    }
    release_capacity();

    guard.mark_callback_scheduled();
    let fut = payload.callback.call(end);
    let _ = tokio::time::timeout(Duration::from_secs(5), fut).await;
}
