//! Runtime-owned event delivery task (ordered, backpressured).

use super::executor_spawn::try_spawn;
use super::finalization::EventSequencer;
use monoloop_contracts::{
    ChannelId, EventDeliveryError, SessionId, TransactionEvent, TransactionEventPayload,
    TransactionEventSink, TransactionId,
};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Mutex};

/// Queued event for delivery.
pub struct QueuedEvent {
    /// Event to deliver.
    pub event: TransactionEvent,
    /// Optional oneshot for terminal delivery ack.
    pub ack: Option<tokio::sync::oneshot::Sender<Result<(), EventDeliveryError>>>,
    /// Approximate serialized size for byte-queue accounting (D-015).
    pub approx_bytes: usize,
}

impl QueuedEvent {
    /// Build a queued event with a conservative byte estimate.
    pub fn new(
        event: TransactionEvent,
        ack: Option<tokio::sync::oneshot::Sender<Result<(), EventDeliveryError>>>,
    ) -> Self {
        let approx_bytes = estimate_event_bytes(&event);
        Self {
            event,
            ack,
            approx_bytes,
        }
    }
}

fn estimate_event_bytes(event: &TransactionEvent) -> usize {
    // Prefer exact JSON size when cheap; fall back to a floor for accounting.
    serde_json::to_vec(event)
        .map(|b| b.len().max(64))
        .unwrap_or(256)
}

/// Bounded event sender: item capacity + byte budget (D-015).
#[derive(Clone)]
pub struct BoundedEventSender {
    tx: mpsc::Sender<QueuedEvent>,
    queued_bytes: Arc<AtomicUsize>,
    max_bytes: usize,
}

impl BoundedEventSender {
    /// Wrap an mpsc sender with a shared byte counter.
    pub fn new(tx: mpsc::Sender<QueuedEvent>, max_bytes: usize) -> Self {
        Self {
            tx,
            queued_bytes: Arc::new(AtomicUsize::new(0)),
            max_bytes: max_bytes.max(1),
        }
    }

    /// Try to enqueue; fails closed when item or byte budget is exceeded.
    ///
    /// Byte reservation is cancellation-safe (D-027): if the caller cancels while
    /// awaiting item capacity, the Drop guard restores the reserved bytes so a
    /// later terminal event can still be queued.
    pub async fn send(&self, item: QueuedEvent) -> Result<(), EventQueueFull> {
        let bytes = item.approx_bytes;
        loop {
            let cur = self.queued_bytes.load(Ordering::SeqCst);
            if cur.saturating_add(bytes) > self.max_bytes {
                return Err(EventQueueFull::Bytes);
            }
            if self
                .queued_bytes
                .compare_exchange(cur, cur + bytes, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        // Holds the reservation until send completes or this future is dropped.
        let mut reservation = ByteReservation {
            counter: &self.queued_bytes,
            bytes,
            released: false,
        };
        match self.tx.send(item).await {
            Ok(()) => {
                reservation.released = true;
                Ok(())
            }
            Err(_) => {
                reservation.release();
                Err(EventQueueFull::Closed)
            }
        }
    }

    /// Shared counter for the delivery task.
    pub fn byte_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.queued_bytes)
    }
}

/// Actor-owned publisher: sole ordinary allocator of public event sequences (D-036).
///
/// Child tasks must not call [`EventSequencer::allocate`] directly. All ordinary
/// and terminal publishes go through this type so allocate+enqueue stay ordered.
#[derive(Clone)]
pub struct OrderedEventPublisher {
    order: Arc<Mutex<()>>,
    event_tx: BoundedEventSender,
    sequencer: Arc<EventSequencer>,
}

impl OrderedEventPublisher {
    /// Create a publisher bound to one transaction's sequencer and queue.
    pub fn new(event_tx: BoundedEventSender, sequencer: Arc<EventSequencer>) -> Self {
        Self {
            order: Arc::new(Mutex::new(())),
            event_tx,
            sequencer,
        }
    }

    /// Sequencer handle (finalization accounting only; do not allocate here).
    pub fn sequencer(&self) -> &Arc<EventSequencer> {
        &self.sequencer
    }

    /// Publish one ordinary event; returns its sequence number.
    pub async fn publish(
        &self,
        transaction_id: TransactionId,
        channel_id: ChannelId,
        session_id: SessionId,
        payload: TransactionEventPayload,
    ) -> Result<u64, EventQueueFull> {
        self.publish_inner(transaction_id, channel_id, session_id, payload, None)
            .await
    }

    /// Publish terminal `Ended` with delivery ack.
    pub async fn publish_terminal(
        &self,
        transaction_id: TransactionId,
        channel_id: ChannelId,
        session_id: SessionId,
        payload: TransactionEventPayload,
        ack: tokio::sync::oneshot::Sender<Result<(), EventDeliveryError>>,
    ) -> Result<u64, EventQueueFull> {
        self.publish_inner(transaction_id, channel_id, session_id, payload, Some(ack))
            .await
    }

    async fn publish_inner(
        &self,
        transaction_id: TransactionId,
        channel_id: ChannelId,
        session_id: SessionId,
        payload: TransactionEventPayload,
        ack: Option<tokio::sync::oneshot::Sender<Result<(), EventDeliveryError>>>,
    ) -> Result<u64, EventQueueFull> {
        // Serialize producers so delivery order matches sequence (D-036).
        let _guard = self.order.lock().await;
        let seq = self.sequencer.peek_next();
        let event = TransactionEvent {
            transaction_id,
            channel_id,
            session_id,
            sequence: seq,
            payload,
        };
        self.event_tx.send(QueuedEvent::new(event, ack)).await?;
        let got = self.sequencer.allocate();
        debug_assert_eq!(got, seq);
        Ok(seq)
    }
}

/// Event queue rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventQueueFull {
    /// Byte budget exceeded.
    Bytes,
    /// Receiver closed.
    Closed,
}

/// RAII guard that restores reserved event-queue bytes on cancel/drop (D-027).
struct ByteReservation<'a> {
    counter: &'a AtomicUsize,
    bytes: usize,
    released: bool,
}

impl ByteReservation<'_> {
    fn release(&mut self) {
        if !self.released {
            self.counter.fetch_sub(self.bytes, Ordering::SeqCst);
            self.released = true;
        }
    }
}

impl Drop for ByteReservation<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

/// Spawn the sequential delivery task for one transaction on `executor` (D-032).
pub fn spawn_delivery_task(
    executor: &Handle,
    mut rx: mpsc::Receiver<QueuedEvent>,
    sink: Arc<dyn TransactionEventSink>,
    on_fail: mpsc::Sender<()>,
    byte_counter: Arc<AtomicUsize>,
    deliver_deadline: Duration,
) -> Result<tokio::task::JoinHandle<()>, ()> {
    let executor_child = executor.clone();
    try_spawn(executor, async move {
        while let Some(item) = rx.recv().await {
            let bytes = item.approx_bytes;
            // D-021: host sink panics (invoke or poll) must not kill delivery.
            let result =
                deliver_isolated(&executor_child, &sink, item.event, deliver_deadline).await;
            byte_counter.fetch_sub(
                bytes.min(byte_counter.load(Ordering::SeqCst)),
                Ordering::SeqCst,
            );
            let ok = result.is_ok();
            if let Some(ack) = item.ack {
                let _ = ack.send(if ok {
                    Ok(())
                } else {
                    Err(EventDeliveryError::Failed)
                });
            }
            if !ok {
                let _ = on_fail.try_send(());
                while let Some(rest) = rx.recv().await {
                    byte_counter.fetch_sub(
                        rest.approx_bytes.min(byte_counter.load(Ordering::SeqCst)),
                        Ordering::SeqCst,
                    );
                    if let Some(ack) = rest.ack {
                        let _ = ack.send(Err(EventDeliveryError::Failed));
                    }
                }
                break;
            }
        }
    })
}

/// Invoke sink.deliver and await its future with panic + deadline isolation (D-021).
async fn deliver_isolated(
    executor: &Handle,
    sink: &Arc<dyn TransactionEventSink>,
    event: TransactionEvent,
    deadline: Duration,
) -> Result<(), EventDeliveryError> {
    let deliver_fut = catch_unwind(AssertUnwindSafe(|| sink.deliver(event)));
    let fut = match deliver_fut {
        Ok(f) => f,
        Err(_) => return Err(EventDeliveryError::Failed),
    };
    // Owned child task: Future::poll panics become JoinError, not delivery-task death.
    let handle = match try_spawn(executor, fut) {
        Ok(h) => h,
        Err(()) => return Err(EventDeliveryError::Failed),
    };
    let abort = handle.abort_handle();
    match tokio::time::timeout(deadline, handle).await {
        Ok(Ok(r)) => r,
        Ok(Err(_)) => Err(EventDeliveryError::Failed),
        Err(_) => {
            abort.abort();
            Err(EventDeliveryError::Failed)
        }
    }
}
