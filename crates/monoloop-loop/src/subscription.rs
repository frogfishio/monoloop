//! Lossless, gap-detecting canonical event subscription for The Loop.

use monoloop_contracts::InterpreterOutputEvent;
use tokio::sync::mpsc;

/// Subscriber identity (correlation only).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubscriberId(String);

impl SubscriberId {
    /// Create a subscriber id.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the id.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Subscription status / gap notification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionStatus {
    /// Source opened.
    Opened,
    /// Source ending cleanly.
    Closing,
    /// Delivery sequence gap detected.
    Gap(SubscriptionGap),
    /// Source lost without clean end.
    Lost,
}

/// Gap details.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionGap {
    /// Expected next sequence.
    pub expected: u64,
    /// Observed sequence (if any).
    pub observed: Option<u64>,
}

/// One delivered event with explicit delivery sequence.
#[derive(Clone, Debug)]
pub struct DeliveredEvent {
    /// Monotonic delivery sequence (1-based) from the distributor.
    pub delivery_sequence: u64,
    /// Canonical interpreter event (unit or end).
    pub event: InterpreterOutputEvent,
}

/// Lossless subscription owned by one Loop (never shared with Console).
pub struct CanonicalEventSubscription {
    /// Subscriber id.
    pub subscriber_id: SubscriberId,
    rx: mpsc::Receiver<Result<DeliveredEvent, SubscriptionStatus>>,
}

impl CanonicalEventSubscription {
    /// Create from a channel end.
    pub fn new(
        subscriber_id: SubscriberId,
        rx: mpsc::Receiver<Result<DeliveredEvent, SubscriptionStatus>>,
    ) -> Self {
        Self { subscriber_id, rx }
    }

    /// Receive next delivery or status. `None` = channel closed.
    pub async fn recv(&mut self) -> Option<Result<DeliveredEvent, SubscriptionStatus>> {
        self.rx.recv().await
    }
}

/// Publisher half used by the event distributor (testkit / composition).
#[derive(Clone)]
pub struct SubscriptionPublisher {
    tx: mpsc::Sender<Result<DeliveredEvent, SubscriptionStatus>>,
    next_seq: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl SubscriptionPublisher {
    /// Create a bounded subscription pair.
    pub fn channel(
        subscriber_id: impl Into<String>,
        capacity: usize,
    ) -> (Self, CanonicalEventSubscription) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (
            Self {
                tx,
                next_seq: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
            },
            CanonicalEventSubscription::new(SubscriberId::new(subscriber_id), rx),
        )
    }

    /// Publish one event with the next delivery sequence (lossless backpressure).
    pub async fn publish(
        &self,
        event: InterpreterOutputEvent,
    ) -> Result<(), mpsc::error::SendError<Result<DeliveredEvent, SubscriptionStatus>>> {
        let seq = self
            .next_seq
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.tx
            .send(Ok(DeliveredEvent {
                delivery_sequence: seq,
                event,
            }))
            .await
    }

    /// Signal a gap (fail-closed for Loop).
    pub async fn signal_gap(&self, expected: u64, observed: Option<u64>) -> Result<(), ()> {
        self.tx
            .send(Err(SubscriptionStatus::Gap(SubscriptionGap {
                expected,
                observed,
            })))
            .await
            .map_err(|_| ())
    }

    /// Signal source lost.
    pub async fn signal_lost(&self) -> Result<(), ()> {
        self.tx
            .send(Err(SubscriptionStatus::Lost))
            .await
            .map_err(|_| ())
    }
}
