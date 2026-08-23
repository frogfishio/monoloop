//! Per-transaction event publisher (v2 §12 / §22.6 / D-047).
//!
//! Sole allocator of ordinary and terminal event sequences for one transaction.
//! Waits for caller mailbox capacity under the transaction deadline — never
//! silently drops ordinary events after the coordinator has handed them off.
//!
//! Terminal Seal uses a **dedicated** channel so a full ordinary-command queue
//! cannot discard Seal or allow ordinary delivery after finalization (D-047).

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

/// Ordinary / establish commands from the coordinator (not Seal).
pub enum EventPublisherCommand {
    /// Establish an external session before ordinary events (§22.6).
    EstablishExternal(ExternalSessionId),
    /// Publish one ordinary payload (sequence allocated after capacity reserved).
    Publish(Box<TransactionEventPayload>),
}

/// Terminal Seal on the dedicated priority channel (D-047).
pub struct SealCommand {
    /// Terminal event body (emitted_events filled by publisher).
    pub terminal: TransactionEndEvent,
    /// Reply with publication result + last committed sequence.
    pub reply: oneshot::Sender<TerminalPublicationResult>,
    /// Authoritative absolute deadline for terminal `Ended` enqueue
    /// (`terminal_event_delivery_deadline`). Shared with the Finalizer wait so
    /// completion cannot publish before a later successful terminal delivery.
    pub deadline: StdInstant,
}

/// Result of Seal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalPublicationResult {
    /// How the terminal event was (or was not) delivered.
    pub delivery: TerminalEventDelivery,
    /// Last committed sequence (includes terminal if published).
    pub last_sequence: u64,
}

/// Run the publisher until Seal completes (or both channels close).
///
/// Seal on `seal_rx` is preferred (biased) over ordinary `cmd_rx`. While an
/// ordinary publish waits for host mailbox capacity, an arriving Seal preempts
/// that wait so no ordinary event can publish after finalization begins.
#[allow(clippy::too_many_arguments)] // publisher state machine ports
pub async fn run_event_publisher(
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    event_tx: TransactionEventSender,
    mut cmd_rx: mpsc::Receiver<EventPublisherCommand>,
    mut seal_rx: mpsc::Receiver<SealCommand>,
    _cancel: Arc<StickyCancel>,
    deadline: StdInstant,
) -> TerminalPublicationResult {
    let mut next_seq: u64 = 1;
    let mut last_committed: u64 = 0;
    let mut session = session_id;
    let mut session_established = false;
    let mut sticky_fail: Option<TerminalEventDelivery> = None;
    let mut cmd_closed = false;
    let mut seal_closed = false;

    loop {
        if let Some(fail) = sticky_fail {
            return match seal_rx.recv().await {
                Some(cmd) => reply_sticky(cmd, fail, last_committed),
                None => TerminalPublicationResult {
                    delivery: fail,
                    last_sequence: last_committed,
                },
            };
        }

        tokio::select! {
            biased;
            seal = seal_rx.recv(), if !seal_closed => {
                match seal {
                    Some(cmd) => {
                        return finish_seal(
                            cmd,
                            transaction_id,
                            &channel_id,
                            &event_tx,
                            &mut session,
                            &mut next_seq,
                            &mut last_committed,
                            None,
                        )
                        .await;
                    }
                    None => {
                        seal_closed = true;
                        if cmd_closed {
                            return TerminalPublicationResult {
                                delivery: TerminalEventDelivery::QueueClosed,
                                last_sequence: last_committed,
                            };
                        }
                    }
                }
            }
            cmd = cmd_rx.recv(), if !cmd_closed => {
                match cmd {
                    Some(EventPublisherCommand::EstablishExternal(external)) => {
                        match establish_external(
                            &event_tx,
                            transaction_id,
                            &channel_id,
                            external,
                            &mut session,
                            &mut next_seq,
                            &mut last_committed,
                            &mut session_established,
                            deadline,
                            &mut seal_rx,
                        )
                        .await
                        {
                            Ok(()) => {}
                            Err(WaitEnd::Sealed(cmd)) => {
                                return finish_seal(
                                    cmd,
                                    transaction_id,
                                    &channel_id,
                                    &event_tx,
                                    &mut session,
                                    &mut next_seq,
                                    &mut last_committed,
                                    None,
                                )
                                .await;
                            }
                            Err(WaitEnd::Failed(fail)) => sticky_fail = Some(fail),
                        }
                    }
                    Some(EventPublisherCommand::Publish(payload)) => {
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
                        match wait_send_or_seal(&event_tx, event, deadline, &mut seal_rx).await {
                            Ok(()) => {
                                last_committed = seq;
                                next_seq = seq.saturating_add(1);
                            }
                            Err(WaitEnd::Sealed(cmd)) => {
                                return finish_seal(
                                    cmd,
                                    transaction_id,
                                    &channel_id,
                                    &event_tx,
                                    &mut session,
                                    &mut next_seq,
                                    &mut last_committed,
                                    None,
                                )
                                .await;
                            }
                            Err(WaitEnd::Failed(fail)) => sticky_fail = Some(fail),
                        }
                    }
                    None => {
                        cmd_closed = true;
                        if seal_closed {
                            return TerminalPublicationResult {
                                delivery: TerminalEventDelivery::QueueClosed,
                                last_sequence: last_committed,
                            };
                        }
                    }
                }
            }
        }
    }
}

enum WaitEnd {
    Sealed(SealCommand),
    Failed(TerminalEventDelivery),
}

fn reply_sticky(
    cmd: SealCommand,
    fail: TerminalEventDelivery,
    last_committed: u64,
) -> TerminalPublicationResult {
    let result = TerminalPublicationResult {
        delivery: fail,
        last_sequence: last_committed,
    };
    let _ = cmd.reply.send(result.clone());
    result
}

#[allow(clippy::too_many_arguments)] // publisher locals
async fn finish_seal(
    cmd: SealCommand,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    event_tx: &TransactionEventSender,
    session: &mut Option<SessionId>,
    next_seq: &mut u64,
    last_committed: &mut u64,
    sticky_fail: Option<TerminalEventDelivery>,
) -> TerminalPublicationResult {
    if let Some(fail) = sticky_fail {
        return reply_sticky(cmd, fail, *last_committed);
    }
    let SealCommand {
        mut terminal,
        reply,
        deadline,
    } = cmd;
    let sid = ensure_session(session, terminal.session_id.clone(), transaction_id);
    let seq = *next_seq;
    terminal.emitted_events = seq;
    terminal.session_id = Some(sid.clone());
    let event = TransactionEvent {
        transaction_id,
        channel_id: channel_id.clone(),
        session_id: sid,
        sequence: seq,
        payload: TransactionEventPayload::EndedEvent(terminal),
    };
    // Use the Seal-carried terminal deadline — never the transaction deadline —
    // so Finalizer and publisher share one authoritative budget.
    let delivery = match enqueue_seal(event_tx, event, deadline).await {
        Ok(()) => {
            *last_committed = seq;
            TerminalEventDelivery::Published
        }
        Err(fail) => fail,
    };
    let result = TerminalPublicationResult {
        delivery,
        last_sequence: *last_committed,
    };
    let _ = reply.send(result.clone());
    result
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

/// Ordinary enqueue; a real Seal preempts without committing the event.
/// Closing the seal channel without a Seal does **not** fail the ordinary wait
/// (tests may drop the seal sender after draining ordinary commands).
async fn wait_send_or_seal(
    event_tx: &TransactionEventSender,
    event: TransactionEvent,
    deadline: StdInstant,
    seal_rx: &mut mpsc::Receiver<SealCommand>,
) -> Result<(), WaitEnd> {
    let tokio_deadline = tokio_deadline_from(deadline);
    let send_fut = event_tx.send(event);
    tokio::pin!(send_fut);
    let mut seal_alive = true;
    loop {
        tokio::select! {
            biased;
            seal = seal_rx.recv(), if seal_alive => match seal {
                Some(cmd) => return Err(WaitEnd::Sealed(cmd)),
                None => {
                    seal_alive = false;
                }
            },
            _ = sleep_until(tokio_deadline) => {
                return Err(WaitEnd::Failed(TerminalEventDelivery::DeadlineExceeded));
            }
            res = &mut send_fut => {
                return match res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(WaitEnd::Failed(map_send_err(e))),
                };
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn establish_external(
    event_tx: &TransactionEventSender,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    external: ExternalSessionId,
    session: &mut Option<SessionId>,
    next_seq: &mut u64,
    last_committed: &mut u64,
    session_established: &mut bool,
    deadline: StdInstant,
    seal_rx: &mut mpsc::Receiver<SealCommand>,
) -> Result<(), WaitEnd> {
    if *session_established || *next_seq != 1 {
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
    match wait_send_or_seal(event_tx, event, deadline, seal_rx).await {
        Ok(()) => {
            let _ = ensure_session(session, Some(sid), transaction_id);
            *last_committed = seq;
            *next_seq = seq.saturating_add(1);
            *session_established = true;
            Ok(())
        }
        Err(WaitEnd::Sealed(cmd)) => Err(WaitEnd::Sealed(cmd)),
        Err(WaitEnd::Failed(fail)) => Err(WaitEnd::Failed(fail)),
    }
}

/// Seal publication: wait under deadline only.
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
