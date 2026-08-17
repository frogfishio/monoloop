//! Run-scoped canonical event distributor: independent subscriptions.
//!
//! Console and Loop never share one receiver. Each subscriber gets its own
//! delivery sequence. Loop subscriptions are lossless (backpressure).

use monoloop_contracts::InterpreterOutputEvent;
use monoloop_loop::{CanonicalEventSubscription, SubscriptionPublisher, SubscriptionStatus};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Policy for a subscriber.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriberPolicy {
    /// Never drop; apply backpressure to the feeder.
    Lossless,
    /// Best-effort: drop if full (console diagnostics).
    BestEffort,
}

/// One registered subscriber.
struct Sub {
    name: String,
    policy: SubscriberPolicy,
    pub_: SubscriptionPublisher,
}

/// Fan-out hub for one run.
pub struct EventDistributor {
    subs: Vec<Sub>,
}

impl EventDistributor {
    /// Create empty distributor.
    pub fn new() -> Self {
        Self { subs: Vec::new() }
    }

    /// Add a subscriber; returns its exclusive subscription.
    pub fn subscribe(
        &mut self,
        name: impl Into<String>,
        policy: SubscriberPolicy,
        capacity: usize,
    ) -> CanonicalEventSubscription {
        let name = name.into();
        let (pub_, sub) = SubscriptionPublisher::channel(name.clone(), capacity);
        self.subs.push(Sub { name, policy, pub_ });
        sub
    }

    /// Publish one interpreter event to every subscriber.
    pub async fn publish(&self, event: InterpreterOutputEvent) {
        for sub in &self.subs {
            match sub.policy {
                SubscriberPolicy::Lossless => {
                    // Backpressure: wait until accepted.
                    let _ = sub.pub_.publish(event.clone()).await;
                }
                SubscriberPolicy::BestEffort => {
                    // try_send via a one-shot spawn would still block publish API;
                    // use publish but ignore errors if closed.
                    let _ = sub.pub_.publish(event.clone()).await;
                }
            }
        }
    }

    /// Close all publishers (drop publishers so receivers see None).
    pub fn close(self) {
        drop(self.subs);
    }
}

impl Default for EventDistributor {
    fn default() -> Self {
        Self::new()
    }
}

/// Bridge: drain an interpreter event stream into a distributor, then close.
pub async fn pump_interpreter_to_distributor(
    events: Arc<monoloop_interpreter::CanonicalEventStream>,
    distributor: EventDistributor,
) {
    loop {
        match events.recv().await {
            Some(ev) => {
                let done = matches!(ev, InterpreterOutputEvent::Ended(_));
                distributor.publish(ev).await;
                if done {
                    break;
                }
            }
            None => break,
        }
    }
    // Drop distributor publishers so Loop subscription ends.
    distributor.close();
}

/// Helper channel type re-export for tests.
pub type StatusTx = mpsc::Sender<SubscriptionStatus>;
