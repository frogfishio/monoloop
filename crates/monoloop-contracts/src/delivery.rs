//! Concrete bounded delivery mailboxes (Transaction Runtime v2).
//!
//! The core runtime publishes only into these library-created channels. It does
//! not invoke host [`crate::transaction::TransactionEventSink`] or
//! [`crate::transaction::CompletionCallback`] traits.

use crate::transaction::{TransactionCompletion, TransactionEvent};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

/// Validated event-queue capacities for [`transaction_delivery`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveryLimits {
    /// Maximum queued event items (nonzero).
    pub max_event_items: usize,
    /// Maximum queued event payload bytes (nonzero).
    pub max_event_bytes: usize,
}

impl DeliveryLimits {
    /// Construct limits; returns an error when either capacity is zero.
    pub fn try_new(
        max_event_items: usize,
        max_event_bytes: usize,
    ) -> Result<Self, DeliveryConfigError> {
        if max_event_items == 0 {
            return Err(DeliveryConfigError::ZeroItemCapacity);
        }
        if max_event_bytes == 0 {
            return Err(DeliveryConfigError::ZeroByteCapacity);
        }
        Ok(Self {
            max_event_items,
            max_event_bytes,
        })
    }
}

/// Invalid delivery-port construction.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum DeliveryConfigError {
    /// Item capacity was zero.
    #[error("delivery item capacity must be nonzero")]
    ZeroItemCapacity,
    /// Byte capacity was zero.
    #[error("delivery byte capacity must be nonzero")]
    ZeroByteCapacity,
}

/// Result of a one-shot completion publication attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CompletionPublishResult {
    /// Receiver existed and accepted the value.
    Published,
    /// Host dropped its completion receiver.
    ReceiverDropped,
    /// Sender was absent or already consumed (invariant).
    InvariantFailed,
}

/// RAII permit for one queued event's byte reservation (D-046).
///
/// Released when the queued item is received or dropped with the channel.
struct QueuedBytePermit {
    queued_bytes: Arc<AtomicU64>,
    nbytes: u64,
}

impl Drop for QueuedBytePermit {
    fn drop(&mut self) {
        self.queued_bytes.fetch_sub(self.nbytes, Ordering::SeqCst);
    }
}

/// Internal mailbox item: event + byte reservation owned by the queue entry.
struct QueuedDeliveryEvent {
    event: TransactionEvent,
    /// `None` only if already taken (should not happen in normal flow).
    permit: Option<QueuedBytePermit>,
}

/// Runtime-held half of the event mailbox.
#[derive(Debug, Clone)]
pub struct TransactionEventSender {
    tx: mpsc::Sender<QueuedDeliveryEvent>,
    /// Approximate bytes currently queued (best-effort accounting).
    queued_bytes: Arc<AtomicU64>,
    max_event_bytes: usize,
}

/// Host-held half of the event mailbox.
#[derive(Debug)]
pub struct TransactionEventReceiver {
    rx: mpsc::Receiver<QueuedDeliveryEvent>,
}

/// Runtime-held one-shot completion sender (consumed exactly once).
#[derive(Debug)]
pub struct TransactionCompletionSender {
    tx: Option<oneshot::Sender<TransactionCompletion>>,
}

/// Host-held one-shot completion receiver.
#[derive(Debug)]
pub struct TransactionCompletionReceiver {
    rx: oneshot::Receiver<TransactionCompletion>,
}

/// Ports installed at admission and owned by the lifecycle ledger until publish.
#[derive(Debug)]
pub struct TransactionDelivery {
    /// Bounded event publisher.
    pub event_tx: TransactionEventSender,
    /// One-shot completion publisher.
    pub completion_tx: TransactionCompletionSender,
}

/// Host receivers created together with [`TransactionDelivery`].
#[derive(Debug)]
pub struct TransactionReceiver {
    /// Ordered event stream.
    pub events: TransactionEventReceiver,
    /// Exactly-one completion publication.
    pub completion: TransactionCompletionReceiver,
}

/// Create paired delivery ports with validated capacities.
pub fn transaction_delivery(
    limits: DeliveryLimits,
) -> Result<(TransactionDelivery, TransactionReceiver), DeliveryConfigError> {
    let limits = DeliveryLimits::try_new(limits.max_event_items, limits.max_event_bytes)?;
    let (event_tx, event_rx) = mpsc::channel(limits.max_event_items);
    let (completion_tx, completion_rx) = oneshot::channel();
    Ok((
        TransactionDelivery {
            event_tx: TransactionEventSender {
                tx: event_tx,
                queued_bytes: Arc::new(AtomicU64::new(0)),
                max_event_bytes: limits.max_event_bytes,
            },
            completion_tx: TransactionCompletionSender {
                tx: Some(completion_tx),
            },
        },
        TransactionReceiver {
            events: TransactionEventReceiver { rx: event_rx },
            completion: TransactionCompletionReceiver { rx: completion_rx },
        },
    ))
}

impl TransactionEventSender {
    /// Maximum event payload bytes accepted by this mailbox.
    pub fn max_event_bytes(&self) -> usize {
        self.max_event_bytes
    }

    /// Best-effort queued byte count.
    pub fn queued_bytes(&self) -> u64 {
        self.queued_bytes.load(Ordering::SeqCst)
    }

    /// Try to enqueue without waiting.
    pub fn try_send(&self, event: TransactionEvent) -> Result<(), EventEnqueueError> {
        let nbytes = estimate_event_bytes(&event);
        if nbytes > self.max_event_bytes {
            return Err(EventEnqueueError::EventTooLarge);
        }
        loop {
            let cur = self.queued_bytes.load(Ordering::SeqCst);
            let next = cur.saturating_add(nbytes as u64);
            if next > self.max_event_bytes as u64 {
                return Err(EventEnqueueError::ByteCapacityExceeded);
            }
            if self
                .queued_bytes
                .compare_exchange(cur, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let item = QueuedDeliveryEvent {
                    event,
                    permit: Some(QueuedBytePermit {
                        queued_bytes: Arc::clone(&self.queued_bytes),
                        nbytes: nbytes as u64,
                    }),
                };
                match self.tx.try_send(item) {
                    Ok(()) => return Ok(()),
                    // Dropping the returned item releases the RAII permit.
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        return Err(EventEnqueueError::ItemCapacityExceeded);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Err(EventEnqueueError::Closed);
                    }
                }
            }
        }
    }

    /// Enqueue, waiting until item capacity is available or the receiver is dropped.
    ///
    /// Byte capacity is reserved before awaiting the mpsc slot. If concurrent
    /// queued bytes would exceed the limit, fails closed immediately (no wait).
    pub async fn send(&self, event: TransactionEvent) -> Result<(), EventEnqueueError> {
        let nbytes = estimate_event_bytes(&event);
        if nbytes > self.max_event_bytes {
            return Err(EventEnqueueError::EventTooLarge);
        }
        loop {
            let cur = self.queued_bytes.load(Ordering::SeqCst);
            let next = cur.saturating_add(nbytes as u64);
            if next > self.max_event_bytes as u64 {
                return Err(EventEnqueueError::ByteCapacityExceeded);
            }
            if self
                .queued_bytes
                .compare_exchange(cur, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
        let item = QueuedDeliveryEvent {
            event,
            permit: Some(QueuedBytePermit {
                queued_bytes: Arc::clone(&self.queued_bytes),
                nbytes: nbytes as u64,
            }),
        };
        match self.tx.send(item).await {
            Ok(()) => Ok(()),
            // Closed: Drop of `item` (or the oneshot Err payload) releases permit.
            Err(mpsc::error::SendError(_)) => Err(EventEnqueueError::Closed),
        }
    }
}

impl TransactionEventReceiver {
    /// Receive the next event, or `None` when the sender has been dropped.
    ///
    /// Releases the queued byte reservation for the received item (D-046).
    pub async fn recv(&mut self) -> Option<TransactionEvent> {
        self.rx.recv().await.map(|item| {
            let QueuedDeliveryEvent { event, permit } = item;
            drop(permit);
            event
        })
    }

    /// Non-blocking receive; releases byte reservation on success (D-046).
    pub fn try_recv(&mut self) -> Result<TransactionEvent, mpsc::error::TryRecvError> {
        self.rx.try_recv().map(|item| {
            let QueuedDeliveryEvent { event, permit } = item;
            drop(permit);
            event
        })
    }
}

impl TransactionCompletionSender {
    /// Publish completion exactly once (non-blocking).
    pub fn send(mut self, completion: TransactionCompletion) -> CompletionPublishResult {
        match self.tx.take() {
            Some(tx) => {
                if tx.send(completion).is_ok() {
                    CompletionPublishResult::Published
                } else {
                    CompletionPublishResult::ReceiverDropped
                }
            }
            None => CompletionPublishResult::InvariantFailed,
        }
    }
}

impl TransactionCompletionReceiver {
    /// Await the single completion publication.
    pub async fn recv(self) -> Result<TransactionCompletion, oneshot::error::RecvError> {
        self.rx.await
    }
}

/// Event enqueue failure (fail closed).
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum EventEnqueueError {
    /// Item capacity exhausted.
    #[error("event item capacity exceeded")]
    ItemCapacityExceeded,
    /// Byte capacity exhausted.
    #[error("event byte capacity exceeded")]
    ByteCapacityExceeded,
    /// Single event exceeds the byte budget alone.
    #[error("event exceeds delivery byte capacity")]
    EventTooLarge,
    /// Receiver dropped / channel closed.
    #[error("event delivery channel closed")]
    Closed,
}

/// Conservative byte estimate for queue accounting (not wire encoding).
pub fn estimate_event_bytes(event: &TransactionEvent) -> usize {
    // Structural overhead + serialized payload when cheap; fall back to a floor.
    match serde_json::to_vec(event) {
        Ok(buf) => buf.len().max(64),
        Err(_) => 256,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{ChannelId, SessionId, TransactionId};
    use crate::transaction::{
        CleanupStatus, TerminalEventDelivery, TransactionDiagnostic, TransactionEndEvent,
        TransactionEndKind, TransactionEventPayload, TransactionUsage,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    fn sample_completion() -> TransactionCompletion {
        TransactionCompletion {
            end: TransactionEndEvent {
                transaction_id: TransactionId::generate(),
                session_id: None,
                channel_id: ChannelId::try_new("ch").unwrap(),
                kind: TransactionEndKind::Completed,
                emitted_events: 1,
                usage: TransactionUsage::default(),
                diagnostics: Vec::<TransactionDiagnostic>::new(),
            },
            terminal_event_delivery: TerminalEventDelivery::Published,
            cleanup: CleanupStatus::Complete,
        }
    }

    #[test]
    fn rejects_zero_capacities() {
        assert!(DeliveryLimits::try_new(0, 1024).is_err());
        assert!(DeliveryLimits::try_new(8, 0).is_err());
    }

    #[tokio::test]
    async fn completion_publish_and_receive() {
        let (delivery, mut receiver) =
            transaction_delivery(DeliveryLimits::try_new(8, 64 * 1024).unwrap()).unwrap();
        let expected = sample_completion();
        let result = delivery.completion_tx.send(expected.clone());
        assert_eq!(result, CompletionPublishResult::Published);
        let got = receiver.completion.recv().await.unwrap();
        assert_eq!(got, expected);
        let _ = &mut receiver.events;
    }

    #[tokio::test]
    async fn completion_receiver_dropped_is_observable() {
        let (delivery, receiver) =
            transaction_delivery(DeliveryLimits::try_new(4, 4096).unwrap()).unwrap();
        drop(receiver);
        let result = delivery.completion_tx.send(sample_completion());
        assert_eq!(result, CompletionPublishResult::ReceiverDropped);
    }

    fn sample_event(sequence: u64) -> TransactionEvent {
        TransactionEvent {
            transaction_id: TransactionId::generate(),
            channel_id: ChannelId::try_new("ch").unwrap(),
            session_id: SessionId::try_new("s").unwrap(),
            sequence,
            payload: TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: crate::safe::SafeDiagnostic::try_new_default("internal", Some("x"))
                    .unwrap(),
            }),
        }
    }

    #[tokio::test]
    async fn event_try_send_item_capacity() {
        let (delivery, _receiver) =
            transaction_delivery(DeliveryLimits::try_new(1, 1024 * 1024).unwrap()).unwrap();
        let ev = sample_event(1);
        delivery.event_tx.try_send(ev.clone()).unwrap();
        let err = delivery.event_tx.try_send(ev).unwrap_err();
        assert_eq!(err, EventEnqueueError::ItemCapacityExceeded);
    }

    /// D-046: byte budget is recovered after receive so cumulative lifetime
    /// volume may exceed the limit while concurrent queued bytes stay within it.
    #[tokio::test]
    async fn event_byte_capacity_recovered_after_receive() {
        let ev = sample_event(1);
        let one = estimate_event_bytes(&ev);
        let (delivery, mut receiver) =
            transaction_delivery(DeliveryLimits::try_new(4, one).unwrap()).unwrap();

        for i in 0..8u64 {
            let next = sample_event(i + 1);
            delivery
                .event_tx
                .try_send(next)
                .unwrap_or_else(|e| panic!("cycle {i} must succeed after receive release: {e:?}"));
            let _ = receiver
                .events
                .recv()
                .await
                .expect("receive releases bytes");
            assert_eq!(
                delivery.event_tx.queued_bytes(),
                0,
                "queued bytes must be zero after receive"
            );
        }
    }

    /// D-046: exact concurrent byte capacity succeeds; plus-one fails closed.
    #[tokio::test]
    async fn event_byte_capacity_exact_and_plus_one() {
        let ev = sample_event(1);
        let one = estimate_event_bytes(&ev);
        let (delivery, mut receiver) =
            transaction_delivery(DeliveryLimits::try_new(8, one * 2).unwrap()).unwrap();
        let a = sample_event(1);
        let b = sample_event(2);
        let c = sample_event(3);
        assert_eq!(estimate_event_bytes(&a), one);
        assert_eq!(estimate_event_bytes(&b), one);
        assert_eq!(estimate_event_bytes(&c), one);

        delivery.event_tx.try_send(a).unwrap();
        delivery.event_tx.try_send(b).unwrap();
        assert_eq!(delivery.event_tx.queued_bytes(), (one * 2) as u64);
        let err = delivery.event_tx.try_send(c).unwrap_err();
        assert_eq!(err, EventEnqueueError::ByteCapacityExceeded);

        let _ = receiver.events.recv().await.unwrap();
        assert_eq!(delivery.event_tx.queued_bytes(), one as u64);
        delivery.event_tx.try_send(sample_event(4)).unwrap();
        assert_eq!(delivery.event_tx.queued_bytes(), (one * 2) as u64);
    }

    /// D-046: dropping the receiver releases every outstanding byte reservation.
    #[tokio::test]
    async fn event_byte_capacity_released_on_receiver_drop() {
        let ev = sample_event(1);
        let one = estimate_event_bytes(&ev);
        let (delivery, receiver) =
            transaction_delivery(DeliveryLimits::try_new(4, one * 3).unwrap()).unwrap();
        delivery.event_tx.try_send(sample_event(1)).unwrap();
        delivery.event_tx.try_send(sample_event(2)).unwrap();
        assert!(delivery.event_tx.queued_bytes() > 0);
        drop(receiver);
        // Allow Drop of queued items to run.
        tokio::task::yield_now().await;
        assert_eq!(
            delivery.event_tx.queued_bytes(),
            0,
            "receiver drop must release all queued byte permits"
        );
        // Channel closed — further sends fail closed, not byte-capacity.
        let err = delivery.event_tx.try_send(sample_event(3)).unwrap_err();
        assert_eq!(err, EventEnqueueError::Closed);
    }

    /// D-046: concurrent senders + drain never underflow or leak capacity.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn event_byte_capacity_concurrent_send_recv_no_leak() {
        let ev = sample_event(1);
        let one = estimate_event_bytes(&ev);
        let (delivery, mut receiver) =
            transaction_delivery(DeliveryLimits::try_new(32, one * 8).unwrap()).unwrap();
        let tx = delivery.event_tx.clone();
        let sent = Arc::new(AtomicU64::new(0));
        let mut joins = Vec::new();
        for t in 0..4u64 {
            let tx = tx.clone();
            let sent = Arc::clone(&sent);
            joins.push(tokio::spawn(async move {
                for i in 0..32u64 {
                    let event = sample_event(t * 100 + i + 1);
                    loop {
                        match tx.try_send(event.clone()) {
                            Ok(()) => {
                                sent.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            Err(EventEnqueueError::ByteCapacityExceeded)
                            | Err(EventEnqueueError::ItemCapacityExceeded) => {
                                tokio::task::yield_now().await;
                            }
                            Err(e) => panic!("unexpected enqueue error: {e:?}"),
                        }
                    }
                }
            }));
        }
        let drain = tokio::spawn(async move {
            let mut n = 0u64;
            while n < 128 {
                if let Some(_ev) = receiver.events.recv().await {
                    n += 1;
                } else {
                    break;
                }
            }
            n
        });
        for j in joins {
            j.await.unwrap();
        }
        drop(tx);
        let drained = drain.await.unwrap();
        assert_eq!(sent.load(Ordering::SeqCst), 128);
        assert_eq!(drained, 128);
        assert_eq!(
            delivery.event_tx.queued_bytes(),
            0,
            "no leaked byte capacity after concurrent drain"
        );
    }
}
