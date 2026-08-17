//! Runtime-owned completion-callback service (D-021).
//!
//! Callbacks run on owned child tasks under a bounded concurrency permit so
//! host panics/timeouts cannot kill actors, and outstanding work can be drained
//! at shutdown independent of actor liveness.

use monoloop_contracts::{CompletionCallback, TransactionEnd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Bounded, runtime-owned completion callback executor.
#[derive(Clone)]
pub struct CallbackService {
    permits: Arc<Semaphore>,
    inflight: Arc<AtomicUsize>,
    default_deadline: Duration,
}

impl CallbackService {
    /// Create with a maximum number of concurrent callback tasks.
    pub fn new(max_concurrent: usize, default_deadline: Duration) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_concurrent.max(1))),
            inflight: Arc::new(AtomicUsize::new(0)),
            default_deadline: if default_deadline.is_zero() {
                Duration::from_millis(50)
            } else {
                default_deadline
            },
        }
    }

    /// Number of callbacks currently executing or queued for a permit.
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    /// Schedule a host callback on an owned task (does not block the caller).
    ///
    /// Capacity reservation: increments inflight before spawn; releases permit
    /// and inflight exactly once when the child finishes (success, error, panic,
    /// or deadline abort).
    pub fn schedule(
        &self,
        callback: Box<dyn CompletionCallback>,
        end: TransactionEnd,
        deadline: Option<Duration>,
    ) {
        let permits = Arc::clone(&self.permits);
        let inflight = Arc::clone(&self.inflight);
        let budget = deadline.unwrap_or(self.default_deadline);
        inflight.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let permit = match permits.acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    inflight.fetch_sub(1, Ordering::SeqCst);
                    return;
                }
            };
            let call =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback.call(end)));
            match call {
                Ok(fut) => {
                    let handle = tokio::spawn(fut);
                    let abort = handle.abort_handle();
                    match tokio::time::timeout(budget, handle).await {
                        Ok(Ok(_)) | Ok(Err(_)) => {}
                        Err(_) => {
                            abort.abort();
                        }
                    }
                }
                Err(_) => {
                    // Panic at invoke: terminal cause already selected by actor.
                }
            }
            drop(permit);
            inflight.fetch_sub(1, Ordering::SeqCst);
        });
    }

    /// Wait until no callbacks are inflight, or until `deadline` elapses.
    pub async fn drain(&self, deadline: Duration) {
        let start = tokio::time::Instant::now();
        while self.inflight() > 0 {
            if start.elapsed() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
