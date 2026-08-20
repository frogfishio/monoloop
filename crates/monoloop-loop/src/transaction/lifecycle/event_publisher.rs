//! Per-transaction event publisher (v2 §12 / §22.6).
//!
//! Sole allocator of ordinary and terminal event sequences for one transaction.
//! Waits for caller mailbox capacity here — never in the global supervisor loop.

use super::session_identity::ensure_session;
use monoloop_contracts::{
    ChannelId, EventEnqueueError, ExternalSessionId, SessionId, TerminalEventDelivery,
    TransactionEndEvent, TransactionEvent, TransactionEventPayload, TransactionEventSender,
    TransactionId,
};
use tokio::sync::{mpsc, oneshot};

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
pub async fn run_event_publisher(
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    event_tx: TransactionEventSender,
    mut cmd_rx: mpsc::Receiver<EventPublisherCommand>,
) -> TerminalPublicationResult {
    let mut next_seq: u64 = 1;
    let mut last_committed: u64 = 0;
    let mut sealed = false;
    let mut session = session_id;
    let mut session_established = false;

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            EventPublisherCommand::EstablishExternal(external) if sealed => {}
            EventPublisherCommand::EstablishExternal(external) => {
                match try_establish_external(
                    &event_tx,
                    transaction_id,
                    &channel_id,
                    external,
                    &mut session,
                    &mut next_seq,
                    &mut last_committed,
                    &mut session_established,
                ) {
                    Ok(()) => {}
                    Err(EventEnqueueError::Closed) => {
                        return TerminalPublicationResult {
                            delivery: TerminalEventDelivery::QueueClosed,
                            last_sequence: last_committed,
                        };
                    }
                    Err(EventEnqueueError::ItemCapacityExceeded)
                    | Err(EventEnqueueError::ByteCapacityExceeded)
                    | Err(EventEnqueueError::EventTooLarge) => {
                        // Do not consume sequence.
                    }
                }
            }
            EventPublisherCommand::Publish(_) if sealed => {}
            EventPublisherCommand::Publish(payload) => {
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
                // try_send: never block the publisher on a non-draining host
                // (supervisor must stay responsive). Spec allows waiting; M3
                // prefers fail-closed limit over stalling lifecycle.
                match event_tx.try_send(event) {
                    Ok(()) => {
                        last_committed = seq;
                        next_seq = seq.saturating_add(1);
                    }
                    Err(EventEnqueueError::Closed) => {
                        return TerminalPublicationResult {
                            delivery: TerminalEventDelivery::QueueClosed,
                            last_sequence: last_committed,
                        };
                    }
                    Err(EventEnqueueError::ItemCapacityExceeded)
                    | Err(EventEnqueueError::ByteCapacityExceeded)
                    | Err(EventEnqueueError::EventTooLarge) => {
                        // Do not consume sequence.
                    }
                }
            }
            EventPublisherCommand::Seal {
                mut terminal,
                reply,
            } => {
                sealed = true;
                debug_assert!(sealed);
                let sid = ensure_session(&mut session, terminal.session_id.clone(), transaction_id);
                let seq = next_seq;
                terminal.emitted_events = seq;
                // Envelope and EndedEvent payload must carry the same SessionId.
                terminal.session_id = Some(sid.clone());
                let event = TransactionEvent {
                    transaction_id,
                    channel_id: channel_id.clone(),
                    session_id: sid,
                    sequence: seq,
                    payload: TransactionEventPayload::EndedEvent(terminal),
                };
                let delivery = match event_tx.try_send(event) {
                    Ok(()) => {
                        last_committed = seq;
                        TerminalEventDelivery::Published
                    }
                    Err(EventEnqueueError::Closed) => TerminalEventDelivery::QueueClosed,
                    Err(EventEnqueueError::ItemCapacityExceeded)
                    | Err(EventEnqueueError::ByteCapacityExceeded)
                    | Err(EventEnqueueError::EventTooLarge) => TerminalEventDelivery::LimitExceeded,
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
        delivery: TerminalEventDelivery::QueueClosed,
        last_sequence: last_committed,
    }
}

#[allow(clippy::too_many_arguments)] // publisher state machine locals
fn try_establish_external(
    event_tx: &TransactionEventSender,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    external: ExternalSessionId,
    session: &mut Option<SessionId>,
    next_seq: &mut u64,
    last_committed: &mut u64,
    session_established: &mut bool,
) -> Result<(), EventEnqueueError> {
    if *session_established {
        return Ok(());
    }
    // §22.6: SessionEstablished must be sequence 1 for new external sessions.
    if *next_seq != 1 {
        return Ok(());
    }
    // Commit identity only after enqueue succeeds (§22.6 fail-closed).
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
    match event_tx.try_send(event) {
        Ok(()) => {
            let _ = ensure_session(session, Some(sid), transaction_id);
            *last_committed = seq;
            *next_seq = seq.saturating_add(1);
            *session_established = true;
            Ok(())
        }
        Err(e) => Err(e),
    }
}
