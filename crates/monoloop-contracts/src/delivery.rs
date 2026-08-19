//! Concrete bounded delivery mailboxes (Transaction Runtime v2).
//!
//! The core runtime publishes only into these library-created channels. It does
//! not invoke host [`crate::transaction::TransactionEventSink`] or
//! [`crate::transaction::CompletionCallback`] traits.

use crate::transaction::{TransactionCompletion, TransactionEvent};
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Runtime-held half of the event mailbox.
#[derive(Debug)]
pub struct TransactionEventSender {
    tx: mpsc::Sender<TransactionEvent>,
    /// Approximate bytes currently queued (best-effort accounting).
    queued_bytes: AtomicU64,
    max_event_bytes: usize,
}

/// Host-held half of the event mailbox.
#[derive(Debug)]
pub struct TransactionEventReceiver {
    rx: mpsc::Receiver<TransactionEvent>,
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
                queued_bytes: AtomicU64::new(0),
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

    /// Try to enqueue without waiting. Returns `false` when full or closed.
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
                match self.tx.try_send(event) {
                    Ok(()) => return Ok(()),
                    Err(mpsc::error::TrySendError::Full(e)) => {
                        self.queued_bytes.fetch_sub(nbytes as u64, Ordering::SeqCst);
                        let _ = e;
                        return Err(EventEnqueueError::ItemCapacityExceeded);
                    }
                    Err(mpsc::error::TrySendError::Closed(e)) => {
                        self.queued_bytes.fetch_sub(nbytes as u64, Ordering::SeqCst);
                        let _ = e;
                        return Err(EventEnqueueError::Closed);
                    }
                }
            }
        }
    }

    /// Enqueue, waiting until capacity is available or the receiver is dropped.
    pub async fn send(&self, event: TransactionEvent) -> Result<(), EventEnqueueError> {
        let nbytes = estimate_event_bytes(&event);
        if nbytes > self.max_event_bytes {
            return Err(EventEnqueueError::EventTooLarge);
        }
        // Reserve bytes before awaiting channel capacity.
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
        match self.tx.send(event).await {
            Ok(()) => Ok(()),
            Err(_) => {
                self.queued_bytes.fetch_sub(nbytes as u64, Ordering::SeqCst);
                Err(EventEnqueueError::Closed)
            }
        }
    }

    /// Account for a host-side receive (byte budget release).
    pub fn note_received(&self, nbytes: usize) {
        self.queued_bytes.fetch_sub(nbytes as u64, Ordering::SeqCst);
    }
}

impl TransactionEventReceiver {
    /// Receive the next event, or `None` when the sender has been dropped.
    pub async fn recv(&mut self) -> Option<TransactionEvent> {
        self.rx.recv().await
    }

    /// Non-blocking receive.
    pub fn try_recv(&mut self) -> Result<TransactionEvent, mpsc::error::TryRecvError> {
        self.rx.try_recv()
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

    #[tokio::test]
    async fn event_try_send_item_capacity() {
        let (delivery, _receiver) =
            transaction_delivery(DeliveryLimits::try_new(1, 1024 * 1024).unwrap()).unwrap();
        let tx_id = TransactionId::generate();
        let ev = TransactionEvent {
            transaction_id: tx_id,
            channel_id: ChannelId::try_new("ch").unwrap(),
            session_id: SessionId::try_new("s").unwrap(),
            sequence: 1,
            payload: TransactionEventPayload::Diagnostic(TransactionDiagnostic {
                diagnostic: crate::safe::SafeDiagnostic::try_new_default("internal", Some("x"))
                    .unwrap(),
            }),
        };
        delivery.event_tx.try_send(ev.clone()).unwrap();
        let err = delivery.event_tx.try_send(ev).unwrap_err();
        assert_eq!(err, EventEnqueueError::ItemCapacityExceeded);
    }
}
