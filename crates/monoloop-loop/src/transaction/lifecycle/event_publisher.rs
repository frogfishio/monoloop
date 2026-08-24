//! Per-transaction event publisher (v2 §12 / §22.6 / D-047).
//!
//! Sole allocator of ordinary and terminal event sequences for one transaction.
//! Waits for caller mailbox capacity under the transaction deadline — never
//! silently drops ordinary events after the coordinator has handed them off.
//!
//! Terminal Seal uses a **dedicated** channel so a full ordinary-command queue
//! cannot discard Seal. Seal is an **ordering fence**:
//! 1. close ordinary admission ([`OrdinaryCmdAdmit::close`]);
//! 2. finish in-flight ordinary under the Seal deadline (commit or sticky-fail);
//! 3. asynchronously `recv` the ordinary channel to exhaustion (Disconnected);
//! 4. then attempt `Ended`.

use super::session_identity::ensure_session;
use crate::transaction::sticky_cancel::StickyCancel;
use monoloop_contracts::{
    ChannelId, EventEnqueueError, ExternalSessionId, SessionId, TerminalEventDelivery,
    TransactionEndEvent, TransactionEvent, TransactionEventPayload, TransactionEventSender,
    TransactionId,
};
use std::sync::{Arc, Mutex};
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

/// Closeable ordinary-command admission into the event publisher.
///
/// Cloneable handle. [`Self::close`] drops the gate's Sender so no *new*
/// `send` can start. In-flight `send` futures that already cloned a Sender may
/// still complete; the publisher then drains them with `recv` until
/// Disconnected after Seal.
#[derive(Clone, Debug)]
pub struct OrdinaryCmdAdmit {
    tx: Arc<Mutex<Option<mpsc::Sender<EventPublisherCommand>>>>,
}

impl OrdinaryCmdAdmit {
    /// Create an admit gate and the publisher's receiver.
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<EventPublisherCommand>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (
            Self {
                tx: Arc::new(Mutex::new(Some(tx))),
            },
            rx,
        )
    }

    /// Linearization point for Seal: refuse new ordinary admissions.
    pub fn close(&self) {
        let _ = self.tx.lock().unwrap_or_else(|e| e.into_inner()).take();
    }

    /// Whether admission is still open.
    pub fn is_open(&self) -> bool {
        self.tx.lock().unwrap_or_else(|e| e.into_inner()).is_some()
    }

    /// Enqueue an ordinary command; fails if closed or the publisher dropped.
    pub async fn send(&self, cmd: EventPublisherCommand) -> Result<(), ()> {
        let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let Some(tx) = tx else {
            return Err(());
        };
        tx.send(cmd).await.map_err(|_| ())
    }

    /// Non-blocking enqueue (tests).
    pub fn try_send(
        &self,
        cmd: EventPublisherCommand,
    ) -> Result<(), mpsc::error::TrySendError<EventPublisherCommand>> {
        let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let Some(tx) = tx else {
            return Err(mpsc::error::TrySendError::Closed(cmd));
        };
        tx.try_send(cmd)
    }

    /// Test hook: clone the live Sender (proving pre-close admission), signal
    /// `holding`, then await capacity. Callers MUST await `holding` before Seal
    /// so the send is known to have crossed the admission linearization point
    /// while the ordinary queue is still full.
    #[cfg(test)]
    pub async fn send_after_pre_fence_hold(
        &self,
        cmd: EventPublisherCommand,
        holding: oneshot::Sender<()>,
    ) -> Result<(), ()> {
        let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let Some(tx) = tx else {
            return Err(());
        };
        let _ = holding.send(());
        tx.send(cmd).await.map_err(|_| ())
    }
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
/// Seal arrives on a dedicated channel so a full ordinary-command queue cannot
/// discard it. Arrival establishes a fence: close [`OrdinaryCmdAdmit`], finish
/// in-flight ordinary under the Seal deadline, `recv` to Disconnected, then
/// `Ended`. Failure to deliver the backlog sticky-fails terminal delivery.
#[allow(clippy::too_many_arguments)] // publisher state machine ports
pub async fn run_event_publisher(
    transaction_id: TransactionId,
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    event_tx: TransactionEventSender,
    mut cmd_rx: mpsc::Receiver<EventPublisherCommand>,
    admit: OrdinaryCmdAdmit,
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
                Some(cmd) => {
                    admit.close();
                    let _ = drain_to_disconnect(&mut cmd_rx, cmd.deadline).await;
                    reply_sticky(cmd, fail, last_committed)
                }
                None => {
                    admit.close();
                    TerminalPublicationResult {
                        delivery: fail,
                        last_sequence: last_committed,
                    }
                }
            };
        }

        tokio::select! {
            biased;
            seal = seal_rx.recv(), if !seal_closed => {
                match seal {
                    Some(cmd) => {
                        admit.close();
                        return fence_drain_and_seal(
                            cmd,
                            transaction_id,
                            &channel_id,
                            &event_tx,
                            &mut cmd_rx,
                            &mut session,
                            &mut next_seq,
                            &mut last_committed,
                            &mut session_established,
                            None,
                        )
                        .await;
                    }
                    None => {
                        seal_closed = true;
                        // Drop gate Sender so cmd_rx can disconnect once external
                        // admits are gone (publisher itself holds an Admit clone).
                        admit.close();
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
                            Err(WaitEnd::Sealed { cmd, ordinary }) => {
                                admit.close();
                                let fail =
                                    apply_ordinary_outcome(ordinary, &mut next_seq, &mut last_committed);
                                return fence_drain_and_seal(
                                    cmd,
                                    transaction_id,
                                    &channel_id,
                                    &event_tx,
                                    &mut cmd_rx,
                                    &mut session,
                                    &mut next_seq,
                                    &mut last_committed,
                                    &mut session_established,
                                    fail,
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
                            Err(WaitEnd::Sealed { cmd, ordinary }) => {
                                admit.close();
                                let fail =
                                    apply_ordinary_outcome(ordinary, &mut next_seq, &mut last_committed);
                                return fence_drain_and_seal(
                                    cmd,
                                    transaction_id,
                                    &channel_id,
                                    &event_tx,
                                    &mut cmd_rx,
                                    &mut session,
                                    &mut next_seq,
                                    &mut last_committed,
                                    &mut session_established,
                                    fail,
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

/// Outcome of the ordinary event that was in flight when Seal arrived.
enum OrdinarySealOutcome {
    /// Already committed to the host mailbox at `seq`.
    Committed { seq: u64 },
    /// Could not deliver under the Seal deadline / send error.
    Failed(TerminalEventDelivery),
}

enum WaitEnd {
    Sealed {
        cmd: SealCommand,
        ordinary: OrdinarySealOutcome,
    },
    Failed(TerminalEventDelivery),
}

fn apply_ordinary_outcome(
    ordinary: OrdinarySealOutcome,
    next_seq: &mut u64,
    last_committed: &mut u64,
) -> Option<TerminalEventDelivery> {
    match ordinary {
        OrdinarySealOutcome::Committed { seq } => {
            *last_committed = seq;
            *next_seq = seq.saturating_add(1);
            None
        }
        OrdinarySealOutcome::Failed(f) => Some(f),
    }
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

/// Discard remaining ordinary commands after sticky failure (admission already closed).
async fn drain_to_disconnect(
    cmd_rx: &mut mpsc::Receiver<EventPublisherCommand>,
    deadline: StdInstant,
) -> Result<(), TerminalEventDelivery> {
    let tokio_deadline = tokio_deadline_from(deadline);
    loop {
        tokio::select! {
            biased;
            _ = sleep_until(tokio_deadline) => {
                return Err(TerminalEventDelivery::DeadlineExceeded);
            }
            cmd = cmd_rx.recv() => {
                if cmd.is_none() {
                    return Ok(());
                }
                // Discard — sticky fail already decided.
            }
        }
    }
}

/// After admission is closed: publish every remaining ordinary command (including
/// those that complete from pre-fence parked `send`s) under the Seal deadline,
/// then publish `Ended`.
#[allow(clippy::too_many_arguments)]
async fn fence_drain_and_seal(
    cmd: SealCommand,
    transaction_id: TransactionId,
    channel_id: &ChannelId,
    event_tx: &TransactionEventSender,
    cmd_rx: &mut mpsc::Receiver<EventPublisherCommand>,
    session: &mut Option<SessionId>,
    next_seq: &mut u64,
    last_committed: &mut u64,
    session_established: &mut bool,
    mut sticky_fail: Option<TerminalEventDelivery>,
) -> TerminalPublicationResult {
    let seal_deadline = cmd.deadline;
    let tokio_deadline = tokio_deadline_from(seal_deadline);

    // Async drain to Disconnected — not try_recv-until-Empty (parked send race).
    while sticky_fail.is_none() {
        let next = tokio::select! {
            biased;
            _ = sleep_until(tokio_deadline) => {
                sticky_fail = Some(TerminalEventDelivery::DeadlineExceeded);
                break;
            }
            cmd = cmd_rx.recv() => cmd,
        };
        match next {
            None => break,
            Some(EventPublisherCommand::EstablishExternal(external)) => {
                if *session_established || *next_seq != 1 {
                    continue;
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
                match enqueue_under_deadline(event_tx, event, seal_deadline).await {
                    Ok(()) => {
                        let _ = ensure_session(session, Some(sid), transaction_id);
                        *last_committed = seq;
                        *next_seq = seq.saturating_add(1);
                        *session_established = true;
                    }
                    Err(fail) => sticky_fail = Some(fail),
                }
            }
            Some(EventPublisherCommand::Publish(payload)) => {
                let sid = ensure_session(session, None, transaction_id);
                let seq = *next_seq;
                let event = TransactionEvent {
                    transaction_id,
                    channel_id: channel_id.clone(),
                    session_id: sid,
                    sequence: seq,
                    payload: *payload,
                };
                match enqueue_under_deadline(event_tx, event, seal_deadline).await {
                    Ok(()) => {
                        *last_committed = seq;
                        *next_seq = seq.saturating_add(1);
                    }
                    Err(fail) => sticky_fail = Some(fail),
                }
            }
        }
    }
    if sticky_fail.is_some() {
        let _ = drain_to_disconnect(cmd_rx, seal_deadline).await;
    }

    finish_seal(
        cmd,
        transaction_id,
        channel_id,
        event_tx,
        session,
        next_seq,
        last_committed,
        sticky_fail,
    )
    .await
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
    let delivery = match enqueue_under_deadline(event_tx, event, deadline).await {
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

/// Ordinary enqueue under `deadline`. On Seal, do **not** drop the in-flight
/// event: finish it under the Seal budget, then return the fence.
async fn wait_send_or_seal(
    event_tx: &TransactionEventSender,
    event: TransactionEvent,
    deadline: StdInstant,
    seal_rx: &mut mpsc::Receiver<SealCommand>,
) -> Result<(), WaitEnd> {
    let seq = event.sequence;
    let tokio_deadline = tokio_deadline_from(deadline);
    let send_fut = event_tx.send(event.clone());
    tokio::pin!(send_fut);
    let mut seal_alive = true;
    loop {
        tokio::select! {
            biased;
            seal = seal_rx.recv(), if seal_alive => match seal {
                Some(cmd) => {
                    let seal_deadline = tokio_deadline_from(cmd.deadline);
                    tokio::select! {
                        biased;
                        _ = sleep_until(seal_deadline) => {
                            return Err(WaitEnd::Sealed {
                                cmd,
                                ordinary: OrdinarySealOutcome::Failed(
                                    TerminalEventDelivery::DeadlineExceeded,
                                ),
                            });
                        }
                        res = &mut send_fut => {
                            let ordinary = match res {
                                Ok(()) => OrdinarySealOutcome::Committed { seq },
                                Err(e) => OrdinarySealOutcome::Failed(map_send_err(e)),
                            };
                            return Err(WaitEnd::Sealed { cmd, ordinary });
                        }
                    }
                }
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
        Err(WaitEnd::Sealed { cmd, ordinary }) => {
            // Mirror commit into session identity when the send already landed.
            if matches!(ordinary, OrdinarySealOutcome::Committed { .. }) {
                let _ = ensure_session(session, Some(sid), transaction_id);
                *session_established = true;
            }
            Err(WaitEnd::Sealed { cmd, ordinary })
        }
        Err(WaitEnd::Failed(fail)) => Err(WaitEnd::Failed(fail)),
    }
}

/// Enqueue one event, waiting only until `deadline`.
async fn enqueue_under_deadline(
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
