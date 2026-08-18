//! Runtime-owned completion-callback service (D-021 / D-029).
//!
//! Callbacks run on owned child tasks under a bounded concurrency permit so
//! host panics/timeouts cannot kill actors, and outstanding work can be drained
//! at shutdown independent of actor liveness. Capacity is reserved at admission
//! (try_reserve) and retained through callback terminal state.

use super::executor_spawn::try_spawn;
use monoloop_contracts::{CompletionCallback, TransactionEnd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};

type CallbackJoin = (AbortHandle, JoinHandle<()>);
/// Std mutex so schedule can always register joins without async/try_lock drop (D-029).
type CallbackJoinSet = Arc<Mutex<Vec<CallbackJoin>>>;

/// Bounded, runtime-owned completion callback executor.
#[derive(Clone)]
pub struct CallbackService {
    permits: Arc<Semaphore>,
    /// Admission reservations + scheduled/running callbacks (D-029).
    reserved: Arc<AtomicUsize>,
    inflight: Arc<AtomicUsize>,
    default_deadline: Duration,
    /// Injected Tokio handle for owned callback tasks (D-032).
    executor: Handle,
    /// Owned callback joins for shutdown abort+join (D-029).
    joins: CallbackJoinSet,
}

/// Admission-time callback capacity reservation (D-029).
pub struct CallbackReservation {
    permits: Arc<Semaphore>,
    reserved: Arc<AtomicUsize>,
    permit: Option<OwnedSemaphorePermit>,
}

impl CallbackReservation {
    /// Release without scheduling (admission rollback).
    pub fn release(mut self) {
        let _ = self.permit.take();
        self.reserved.fetch_sub(1, Ordering::SeqCst);
    }

    fn into_parts(mut self) -> (Arc<Semaphore>, Arc<AtomicUsize>, OwnedSemaphorePermit) {
        let permit = self
            .permit
            .take()
            .expect("CallbackReservation permit present");
        self.reserved.fetch_sub(1, Ordering::SeqCst);
        (
            Arc::clone(&self.permits),
            Arc::clone(&self.reserved),
            permit,
        )
    }
}

impl Drop for CallbackReservation {
    fn drop(&mut self) {
        if self.permit.take().is_some() {
            self.reserved.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl CallbackService {
    /// Create with a maximum number of concurrent callback tasks.
    pub fn new(max_concurrent: usize, default_deadline: Duration, executor: Handle) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(max_concurrent.max(1))),
            reserved: Arc::new(AtomicUsize::new(0)),
            inflight: Arc::new(AtomicUsize::new(0)),
            default_deadline: if default_deadline.is_zero() {
                Duration::from_millis(50)
            } else {
                default_deadline
            },
            executor,
            joins: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Number of callbacks currently executing.
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    /// Reserved + inflight callback slots (admission bound).
    pub fn reserved(&self) -> usize {
        self.reserved.load(Ordering::SeqCst)
    }

    /// Reserve one callback slot at admission (fail closed when full).
    pub fn try_reserve(&self) -> Option<CallbackReservation> {
        let permit = self.permits.clone().try_acquire_owned().ok()?;
        self.reserved.fetch_add(1, Ordering::SeqCst);
        Some(CallbackReservation {
            permits: Arc::clone(&self.permits),
            reserved: Arc::clone(&self.reserved),
            permit: Some(permit),
        })
    }

    /// Schedule a host callback using an admission reservation (D-029).
    ///
    /// If the executor rejects the spawn, the reservation permit is dropped and
    /// inflight is not left elevated (D-032).
    pub fn schedule_reserved(
        &self,
        reservation: CallbackReservation,
        callback: Box<dyn CompletionCallback>,
        end: TransactionEnd,
        deadline: Option<Duration>,
    ) {
        let (_permits, _reserved_counter, permit) = reservation.into_parts();
        let inflight = Arc::clone(&self.inflight);
        let joins = Arc::clone(&self.joins);
        let executor = self.executor.clone();
        let budget = deadline.unwrap_or(self.default_deadline);
        inflight.fetch_add(1, Ordering::SeqCst);
        let inflight_child = Arc::clone(&inflight);
        let handle = match try_spawn(&self.executor, async move {
            let call =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback.call(end)));
            if let Ok(fut) = call {
                let mut handle = match try_spawn(&executor, fut) {
                    Ok(h) => h,
                    Err(()) => {
                        drop(permit);
                        inflight_child.fetch_sub(1, Ordering::SeqCst);
                        return;
                    }
                };
                let abort = handle.abort_handle();
                match tokio::time::timeout(budget, &mut handle).await {
                    Ok(Ok(_)) | Ok(Err(_)) => {}
                    Err(_) => {
                        abort.abort();
                        let _ = handle.await;
                    }
                }
            }
            drop(permit);
            inflight_child.fetch_sub(1, Ordering::SeqCst);
        }) {
            Ok(h) => h,
            Err(()) => {
                // Future (and permit) dropped with failed spawn; clear inflight only.
                inflight.fetch_sub(1, Ordering::SeqCst);
                return;
            }
        };
        let abort = handle.abort_handle();
        // Always own the join for shutdown (D-029 residual).
        joins
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((abort, handle));
    }

    /// Schedule a host callback on an owned task (does not block the caller).
    ///
    /// Prefer [`schedule_reserved`] after admission. This path still acquires a
    /// permit before work; if none are available the callback is dropped fail-closed.
    pub fn schedule(
        &self,
        callback: Box<dyn CompletionCallback>,
        end: TransactionEnd,
        deadline: Option<Duration>,
    ) {
        let Some(reservation) = self.try_reserve() else {
            return;
        };
        self.schedule_reserved(reservation, callback, end, deadline);
    }

    /// Wait until no callbacks are inflight, or until `deadline` elapses.
    /// On expiry, abort owned callback tasks and join briefly (D-029).
    pub async fn drain(&self, deadline: Duration) {
        let start = tokio::time::Instant::now();
        while self.inflight() > 0 {
            if start.elapsed() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if self.inflight() == 0 {
            return;
        }
        let remaining = deadline.saturating_sub(start.elapsed());
        let mut handles = {
            let mut g = self.joins.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *g)
        };
        for (abort, join) in handles.drain(..) {
            if remaining.is_zero() {
                abort.abort();
                let _ = join.await;
            } else {
                abort.abort();
                let _ = tokio::time::timeout(remaining, join).await;
            }
        }
    }
}
