//! Sticky cancellation: notify that is not lost if fired before a waiter registers.
//!
//! Bare `Notify::notify_waiters()` drops the permit when nobody is waiting; the
//! actor cancel path then awaits tool dispatch until its ordinary deadline (D-028).

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

/// Cancel flag + notify. `cancel()` is sticky; `cancelled().await` returns once set.
#[derive(Debug, Default)]
pub struct StickyCancel {
    flag: AtomicBool,
    notify: Notify,
}

impl StickyCancel {
    /// Create unset.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark cancelled and wake waiters (idempotent).
    pub fn cancel(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Whether cancel has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Wait until [`Self::cancel`] has been called.
    pub async fn cancelled(&self) {
        loop {
            // Subscribe before re-checking the flag so a concurrent cancel is not lost.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            if self.flag.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
            if self.flag.load(Ordering::SeqCst) {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn cancel_before_waiter_is_sticky() {
        let c = Arc::new(StickyCancel::new());
        c.cancel();
        tokio::time::timeout(std::time::Duration::from_millis(50), c.cancelled())
            .await
            .expect("sticky cancel must resolve without a prior waiter");
    }

    #[tokio::test]
    async fn cancel_wakes_existing_waiter() {
        let c = Arc::new(StickyCancel::new());
        let c2 = Arc::clone(&c);
        let join = tokio::spawn(async move {
            c2.cancelled().await;
        });
        tokio::task::yield_now().await;
        c.cancel();
        tokio::time::timeout(std::time::Duration::from_millis(50), join)
            .await
            .expect("waiter joined")
            .expect("task ok");
    }
}
