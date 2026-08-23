//! Per-transaction event publisher (v2 §12 / §22.6 / D-047).
//!
//! Sole allocator of ordinary and terminal event sequences for one transaction.
//! Waits for caller mailbox capacity under the transaction deadline — never
//! silently drops ordinary events after the coordinator has handed them off.

use super::session_identity::ensure_session;
use crate::transaction::sticky_cancel::StickyCancel;
use monoloop_contracts::{
    ChannelId, EventEnqueueError, ExternalSessionId, SessionId, TerminalEventDelivery,
    TransactionEndEvent, TransactionEvent, TransactionEventPayload, TransactionEventSender,
    TransactionId,
};
use std::sync::Arc;
use std::time::Instant as StdInstant;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep_until, Instant as TokioInstant};

/// Commands from the coordinator (Publish) or supervisor (Seal).
pub enum EventPublisherCommand {
    /// Establish an external session before ordinary events (§22.6).
    ///
    /// When this is the first successful enqueue, publishes `SessionEstablished`
    /// at sequence 1. Ignored if a session was already established or sequences
    /// have already advanced (cannot satisfy seq-1).
    EstablishExternal(ExternalSessionId),
    /// Publish one ordinary payload (sequence allocated after capacity reserved).
    Publish(Box<TransactionEventPayload>),
    /// Allocate terminal sequence, enqueue EndedEvent, reply with result.
    Seal {
        /// Terminal event body (emitted_events filled by publisher).
        terminal: TransactionEndEvent,
        /// Reply with publication result + last committed sequence.
        reply: oneshot::Sender<TerminalPublicationResult>,
    },
}

/// Result of Seal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalPublicationResult {
    /// How the terminal event was (or was not) delivered.
    pub delivery: TerminalEventDelivery,
    /// Last committed sequence (includes terminal if published).
    pub last_sequence: u64,
}

/// Run the publisher until Seal completes (or the command channel closes).
///
/// Ordinary / establish publishes wait for host mailbox capacity until `cancel`
/// or `deadline`. Failures are sticky: later ordinary events are refused, and
/// Seal reports the sticky failure instead of inventing `Published` for a
/// silently truncated stream (D-047).
pub async fn run_event_publisher(
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    event_tx: TransactionEventSender,
    mut cmd_rx: mpsc::Receiver<EventPublisherCommand>,
    _cancel: Arc<StickyCancel>,
    deadline: StdInstant,
) -> TerminalPublicationResult {
    let mut next_seq: u64 = 1;
    let mut last_committed: u64 = 0;
    let mut session = session_id;
    let mut session_established = false;
    // Sticky ordinary/establish publication failure (D-047).
    let mut sticky_fail: Option<TerminalEventDelivery> = None;

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            EventPublisherCommand::EstablishExternal(_) if sticky_fail.is_some() => {}
            EventPublisherCommand::EstablishExternal(external) => {
                match establish_external_waiting(
                    &event_tx,
                    transaction_id,
                    &channel_id,
                    external,
                    &mut session,
                    &mut next_seq,
                    &mut last_committed,
                    &mut session_established,
                    deadline,
                )
                .await
                {
                    Ok(()) => {}
                    Err(fail) => {
                        sticky_fail = Some(fail);
                    }
                }
            }
            EventPublisherCommand::Publish(_) if sticky_fail.is_some() => {
                // Refuse ordinary events after a sticky publish failure so Seal
                // cannot paper over a truncated stream with Completed.
            }
            EventPublisherCommand::Publish(payload) => {
                // §22.6: if EstablishExternal was required but never committed
                // seq 1, do not publish ordinary events afterward.
                // (Only applies when establish was attempted and sticky-failed;
                // DirectLlm paths never send EstablishExternal.)
                let payload = *payload;
                let sid = ensure_session(&mut session, None, transaction_id);
                let seq = next_seq;
                let event = TransactionEvent {
                    transaction_id,
                    channel_id: channel_id.clone(),
                    session_id: sid,
                    sequence: seq,
                    payload,
                };
                match enqueue_waiting(&event_tx, event, deadline).await {
                    Ok(()) => {
                        last_committed = seq;
                        next_seq = seq.saturating_add(1);
                    }
                    Err(EnqueueWaitError::Failed(fail)) => {
                        sticky_fail = Some(fail);
                    }
                }
            }
            EventPublisherCommand::Seal {
                mut terminal,
                reply,
            } => {
                if let Some(fail) = sticky_fail {
                    let result = TerminalPublicationResult {
                        delivery: fail,
                        last_sequence: last_committed,
                    };
                    let _ = reply.send(result.clone());
                    return result;
                }
                let sid = ensure_session(&mut session, terminal.session_id.clone(), transaction_id);
                let seq = next_seq;
                terminal.emitted_events = seq;
                terminal.session_id = Some(sid.clone());
                let event = TransactionEvent {
                    transaction_id,
                    channel_id: channel_id.clone(),
                    session_id: sid,
                    sequence: seq,
                    payload: TransactionEventPayload::EndedEvent(terminal),
                };
                // Terminal: cancel is already set by accept_terminal before Seal.
                // Do not treat cancel as publication failure — wait only on
                // deadline / mailbox capacity (D-047).
                let delivery = match enqueue_seal(&event_tx, event, deadline).await {
                    Ok(()) => {
                        last_committed = seq;
                        TerminalEventDelivery::Published
                    }
                    Err(fail) => fail,
                };
                let result = TerminalPublicationResult {
                    delivery,
                    last_sequence: last_committed,
                };
                let _ = reply.send(result.clone());
                return result;
            }
        }
    }

    TerminalPublicationResult {
        delivery: sticky_fail.unwrap_or(TerminalEventDelivery::QueueClosed),
        last_sequence: last_committed,
    }
}

enum EnqueueWaitError {
    Failed(TerminalEventDelivery),
}

fn tokio_deadline_from(deadline: StdInstant) -> TokioInstant {
    let now = StdInstant::now();
    if deadline > now {
        TokioInstant::now() + deadline.saturating_duration_since(now)
    } else {
        TokioInstant::now()
    }
}

fn map_send_err(err: EventEnqueueError) -> TerminalEventDelivery {
    match err {
        EventEnqueueError::Closed => TerminalEventDelivery::QueueClosed,
        EventEnqueueError::EventTooLarge
        | EventEnqueueError::ByteCapacityExceeded
        | EventEnqueueError::ItemCapacityExceeded => TerminalEventDelivery::LimitExceeded,
    }
}

/// Wait for mailbox capacity under the transaction deadline.
///
/// Does **not** abort on StickyCancel: accept_terminal cancels before Seal, and
/// pending ordinary `Publish` commands already accepted onto the publisher
/// command queue must still be delivered (D-047 lossless).
async fn enqueue_waiting(
    event_tx: &TransactionEventSender,
    event: TransactionEvent,
    deadline: StdInstant,
) -> Result<(), EnqueueWaitError> {
    let tokio_deadline = tokio_deadline_from(deadline);
    tokio::select! {
        biased;
        _ = sleep_until(tokio_deadline) => Err(EnqueueWaitError::Failed(
            TerminalEventDelivery::DeadlineExceeded,
        )),
        res = event_tx.send(event) => match res {
            Ok(()) => Ok(()),
            Err(e) => Err(EnqueueWaitError::Failed(map_send_err(e))),
        },
    }
}

/// Seal publication: ignore cancel (already set) and wait under deadline only.
async fn enqueue_seal(
    event_tx: &TransactionEventSender,
    event: TransactionEvent,
    deadline: StdInstant,
) -> Result<(), TerminalEventDelivery> {
    let tokio_deadline = tokio_deadline_from(deadline);
    tokio::select! {
        biased;
        _ = sleep_until(tokio_deadline) => Err(TerminalEventDelivery::DeadlineExceeded),
        res = event_tx.send(event) => match res {
            Ok(()) => Ok(()),
            Err(e) => Err(map_send_err(e)),
        },
    }
}

#[allow(clippy::too_many_arguments)] // publisher state machine locals
async fn establish_external_waiting(
    event_tx: &TransactionEventSender,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    external: ExternalSessionId,
    session: &mut Option<SessionId>,
    next_seq: &mut u64,
    last_committed: &mut u64,
    session_established: &mut bool,
    deadline: StdInstant,
) -> Result<(), TerminalEventDelivery> {
    if *session_established {
        return Ok(());
    }
    // §22.6: SessionEstablished must be sequence 1 for new external sessions.
    if *next_seq != 1 {
        return Ok(());
    }
    let sid = SessionId::from_external(&external);
    let seq = *next_seq;
    let event = TransactionEvent {
        transaction_id,
        channel_id: channel_id.clone(),
        session_id: sid.clone(),
        sequence: seq,
        payload: TransactionEventPayload::SessionEstablished {
            external_session_id: external,
        },
    };
    match enqueue_waiting(event_tx, event, deadline).await {
        Ok(()) => {
            let _ = ensure_session(session, Some(sid), transaction_id);
            *last_committed = seq;
            *next_seq = seq.saturating_add(1);
            *session_established = true;
            Ok(())
        }
        Err(EnqueueWaitError::Failed(fail)) => Err(fail),
    }
}
