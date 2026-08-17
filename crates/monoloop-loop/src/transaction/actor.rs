//! WP-04 no-I/O transaction actor: lifecycle, events, terminal, callback.

use super::active_registry::{ActiveTransactionRegistry, ClaimSessionError, ControlMessage};
use super::events::QueuedEvent;
use super::finalization::{build_transaction_end, FinalizationGuard};
use monoloop_connector::SessionAdapter;
use monoloop_contracts::{
    ChannelId, ChannelKind, EventDeliveryOutcome, ExternalSessionId, SessionId, SessionKey,
    TransactionEndKind, TransactionEvent, TransactionEventPayload, TransactionId,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// Inputs for the no-I/O actor.
pub struct ActorSpawn {
    /// Transaction id.
    pub transaction_id: TransactionId,
    /// Channel id.
    pub channel_id: ChannelId,
    /// Channel kind (WP-05 exchange path).
    #[allow(dead_code)]
    pub channel_kind: ChannelKind,
    /// Session key if known at admission.
    pub session_key: Option<SessionKey>,
    /// Provisional external create (no session yet).
    pub provisional_external: bool,
    /// Session adapter (WP-05 attach path).
    #[allow(dead_code)]
    pub sessions: Option<Arc<dyn SessionAdapter>>,
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
        mut session_key,
        provisional_external,
        sessions: _,
        guard,
        mut control_rx,
        event_tx,
        mut delivery_fail_rx,
        registry,
        release_capacity,
        deadline,
    } = spawn;

    let mut emitted = 0u64;
    let mut terminal_kind = TransactionEndKind::Completed;

    let setup = async {
        if provisional_external {
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
            let seq = guard.sequencer().allocate();
            emitted = seq;
            let sid_ev = session_key
                .as_ref()
                .map(|k| k.session_id.clone())
                .unwrap_or(sid);
            let event = TransactionEvent {
                transaction_id,
                channel_id: channel_id.clone(),
                session_id: sid_ev,
                sequence: seq,
                payload: TransactionEventPayload::SessionEstablished {
                    external_session_id: ext,
                },
            };
            if event_tx
                .send(QueuedEvent { event, ack: None })
                .await
                .is_err()
            {
                return Err(TransactionEndKind::EventDeliveryFailed);
            }
        }
        Ok::<(), TransactionEndKind>(())
    };

    tokio::select! {
        biased;
        ctrl = control_rx.recv() => {
            terminal_kind = match ctrl {
                Some(ControlMessage::ForceTerminate) => TransactionEndKind::Terminated,
                Some(ControlMessage::Cancel) | None => TransactionEndKind::Cancelled,
            };
        }
        _ = tokio::time::sleep(deadline) => {
            terminal_kind = TransactionEndKind::DeadlineExceeded;
        }
        fail = delivery_fail_rx.recv() => {
            if fail.is_some() {
                terminal_kind = TransactionEndKind::EventDeliveryFailed;
            }
        }
        work_res = setup => {
            if let Err(k) = work_res {
                terminal_kind = k;
            }
        }
    }

    let _ = emitted;
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
