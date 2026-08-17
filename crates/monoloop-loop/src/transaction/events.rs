//! Runtime-owned event delivery task (ordered, backpressured).

use monoloop_contracts::{EventDeliveryError, TransactionEvent, TransactionEventSink};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

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
        match self.tx.send(item).await {
            Ok(()) => Ok(()),
            Err(_) => {
                self.queued_bytes.fetch_sub(bytes, Ordering::SeqCst);
                Err(EventQueueFull::Closed)
            }
        }
    }

    /// Shared counter for the delivery task.
    pub fn byte_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.queued_bytes)
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

/// Spawn the sequential delivery task for one transaction.
pub fn spawn_delivery_task(
    mut rx: mpsc::Receiver<QueuedEvent>,
    sink: Arc<dyn TransactionEventSink>,
    on_fail: mpsc::Sender<()>,
    byte_counter: Arc<AtomicUsize>,
    deliver_deadline: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            let bytes = item.approx_bytes;
            // D-021: host sink panics must not kill delivery without failure signal.
            let deliver_fut = catch_unwind(AssertUnwindSafe(|| sink.deliver(item.event)));
            let result = match deliver_fut {
                Ok(fut) => {
                    // Bound individual delivery waits (terminal path uses ack deadline separately).
                    match tokio::time::timeout(deliver_deadline, fut).await {
                        Ok(r) => r,
                        Err(_) => Err(EventDeliveryError::Failed),
                    }
                }
                Err(_) => Err(EventDeliveryError::Failed),
            };
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
