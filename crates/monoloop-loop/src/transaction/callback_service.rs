//! Runtime-owned completion-callback service (D-021 / D-029).
//!
//! Callbacks run on owned child tasks under a bounded concurrency permit.
//! On deadline abort the join handle and permit are retained until the join
//! finishes so non-yielding callbacks cannot detach while freeing capacity.

use super::executor_spawn::try_spawn;
use super::spawn_gate::SpawnGate;
use monoloop_contracts::{CompletionCallback, TransactionEnd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinHandle};

type CallbackJoin = (AbortHandle, JoinHandle<()>);
type CallbackJoinSet = Arc<Mutex<Vec<CallbackJoin>>>;

/// Timed-out callback still holding its permit until the join finishes.
struct RetainedCallback {
    abort: AbortHandle,
    join: JoinHandle<()>,
    permit: OwnedSemaphorePermit,
}

/// Bounded, runtime-owned completion callback executor.
#[derive(Clone)]
pub struct CallbackService {
    permits: Arc<Semaphore>,
    reserved: Arc<AtomicUsize>,
    inflight: Arc<AtomicUsize>,
    default_deadline: Duration,
    executor: Handle,
    spawn_gate: SpawnGate,
    joins: CallbackJoinSet,
    retained: Arc<Mutex<Vec<RetainedCallback>>>,
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
    pub fn new(
        max_concurrent: usize,
        default_deadline: Duration,
        executor: Handle,
        spawn_gate: SpawnGate,
    ) -> Self {
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
            spawn_gate,
            joins: Arc::new(Mutex::new(Vec::new())),
            retained: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Number of callbacks currently executing (including retained).
    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    /// Reserved + inflight callback slots (admission bound).
    pub fn reserved(&self) -> usize {
        self.reserved.load(Ordering::SeqCst)
    }

    /// Free callback semaphore permits (for tests / drain readiness).
    pub fn available_permits(&self) -> usize {
        Self::reap_retained(&self.retained, &self.inflight);
        self.permits.available_permits()
    }

    /// Reserve one callback slot at admission (fail closed when full).
    pub fn try_reserve(&self) -> Option<CallbackReservation> {
        // Reap finished retained callbacks *before* acquiring so permits held by
        // completed work cannot permanently starve new reservations.
        Self::reap_retained(&self.retained, &self.inflight);
        let permit = self.permits.clone().try_acquire_owned().ok()?;
        self.reserved.fetch_add(1, Ordering::SeqCst);
        Some(CallbackReservation {
            permits: Arc::clone(&self.permits),
            reserved: Arc::clone(&self.reserved),
            permit: Some(permit),
        })
    }

    fn reap_retained(retained: &Mutex<Vec<RetainedCallback>>, inflight: &AtomicUsize) {
        let mut g = retained.lock().unwrap_or_else(|e| e.into_inner());
        let mut keep = Vec::new();
        for r in g.drain(..) {
            if r.join.is_finished() {
                drop(r.join);
                drop(r.permit);
                inflight.fetch_sub(1, Ordering::SeqCst);
            } else {
                keep.push(r);
            }
        }
        *g = keep;
    }

    /// Schedule a host callback using an admission reservation (D-029).
    ///
    /// Returns `Err((reservation, callback))` if scheduling is impossible (gate
    /// closed / spawn rejected) so the caller can restore finalization state
    /// instead of marking the callback scheduled.
    pub fn schedule_reserved(
        &self,
        reservation: CallbackReservation,
        callback: Box<dyn CompletionCallback>,
        end: TransactionEnd,
        deadline: Option<Duration>,
    ) -> Result<(), (CallbackReservation, Box<dyn CompletionCallback>)> {
        Self::reap_retained(&self.retained, &self.inflight);
        if !self.spawn_gate.is_open() {
            return Err((reservation, callback));
        }
        let inflight = Arc::clone(&self.inflight);
        let joins = Arc::clone(&self.joins);
        let retained = Arc::clone(&self.retained);
        let executor = self.executor.clone();
        let gate = self.spawn_gate.clone();
        let budget = deadline.unwrap_or(self.default_deadline);
        // Deliver work via oneshot so a failed try_spawn can return the callback.
        let (work_tx, work_rx) = tokio::sync::oneshot::channel::<(
            Box<dyn CompletionCallback>,
            TransactionEnd,
            OwnedSemaphorePermit,
        )>();
        inflight.fetch_add(1, Ordering::SeqCst);
        let inflight_child = Arc::clone(&inflight);
        let handle = match try_spawn(&self.executor, &gate, async move {
            let Ok((callback, end, permit)) = work_rx.await else {
                inflight_child.fetch_sub(1, Ordering::SeqCst);
                return;
            };
            let call =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback.call(end)));
            match call {
                Ok(fut) => {
                    // Permit is released in the callback task itself so an outer
                    // scheduling hop cannot starve capacity after the body finishes.
                    let permit_slot = Arc::new(Mutex::new(Some(permit)));
                    let slot_in_task = Arc::clone(&permit_slot);
                    let mut handle =
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            executor.spawn(async move {
                                let _ = fut.await;
                                drop(
                                    slot_in_task
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner())
                                        .take(),
                                );
                            })
                        })) {
                            Ok(h) => h,
                            Err(_) => {
                                drop(permit_slot.lock().unwrap_or_else(|e| e.into_inner()).take());
                                inflight_child.fetch_sub(1, Ordering::SeqCst);
                                return;
                            }
                        };
                    let abort = handle.abort_handle();
                    match tokio::time::timeout(budget, &mut handle).await {
                        Ok(Ok(_)) | Ok(Err(_)) => {
                            // Residual take if task dropped without clearing.
                            drop(permit_slot.lock().unwrap_or_else(|e| e.into_inner()).take());
                            inflight_child.fetch_sub(1, Ordering::SeqCst);
                        }
                        Err(_) => {
                            abort.abort();
                            let leftover =
                                permit_slot.lock().unwrap_or_else(|e| e.into_inner()).take();
                            if let Some(permit) = leftover {
                                retained.lock().unwrap_or_else(|e| e.into_inner()).push(
                                    RetainedCallback {
                                        abort,
                                        join: handle,
                                        permit,
                                    },
                                );
                            } else {
                                // Body finished in the race window — observe join.
                                let _ = handle.await;
                                inflight_child.fetch_sub(1, Ordering::SeqCst);
                            }
                        }
                    }
                }
                Err(_) => {
                    drop(permit);
                    inflight_child.fetch_sub(1, Ordering::SeqCst);
                }
            }
        }) {
            Ok(h) => h,
            Err(()) => {
                inflight.fetch_sub(1, Ordering::SeqCst);
                return Err((reservation, callback));
            }
        };
        let (permits, reserved_counter, permit) = reservation.into_parts();
        if let Err((callback, end, permit)) = work_tx.send((callback, end, permit)) {
            let _ = end;
            handle.abort();
            inflight.fetch_sub(1, Ordering::SeqCst);
            reserved_counter.fetch_add(1, Ordering::SeqCst);
            return Err((
                CallbackReservation {
                    permits,
                    reserved: reserved_counter,
                    permit: Some(permit),
                },
                callback,
            ));
        }
        let abort = handle.abort_handle();
        let mut g = joins.lock().unwrap_or_else(|e| e.into_inner());
        g.retain(|(_, join)| !join.is_finished());
        g.push((abort, handle));
        Ok(())
    }

    /// Schedule a host callback on an owned task (does not block the caller).
    pub fn schedule(
        &self,
        callback: Box<dyn CompletionCallback>,
        end: TransactionEnd,
        deadline: Option<Duration>,
    ) {
        let Some(reservation) = self.try_reserve() else {
            return;
        };
        let _ = self.schedule_reserved(reservation, callback, end, deadline);
    }

    /// Wait until no callbacks are inflight, or until `deadline` elapses.
    pub async fn drain(&self, deadline: Duration) {
        let start = tokio::time::Instant::now();
        while self.inflight() > 0 {
            Self::reap_retained(&self.retained, &self.inflight);
            if start.elapsed() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let remaining = deadline.saturating_sub(start.elapsed());
        let mut handles = {
            let mut g = self.joins.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *g)
        };
        let mut still_joins = Vec::new();
        for (abort, mut join) in handles.drain(..) {
            abort.abort();
            if remaining.is_zero() {
                if join.is_finished() {
                    drop(join);
                } else {
                    // Keep ownership — do not detach when the drain budget is gone.
                    still_joins.push((abort, join));
                }
                continue;
            }
            match tokio::time::timeout(remaining, &mut join).await {
                Ok(_) => {}
                Err(_) => still_joins.push((abort, join)),
            }
        }
        if !still_joins.is_empty() {
            *self.joins.lock().unwrap_or_else(|e| e.into_inner()) = still_joins;
        }
        // Retained: abort and join within remaining budget; unfinished keep permits.
        let mut retained = {
            let mut g = self.retained.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *g)
        };
        let rem = deadline.saturating_sub(start.elapsed());
        let mut still = Vec::new();
        for mut r in retained.drain(..) {
            r.abort.abort();
            if rem.is_zero() {
                if r.join.is_finished() {
                    drop(r.permit);
                    self.inflight.fetch_sub(1, Ordering::SeqCst);
                } else {
                    still.push(r);
                }
                continue;
            }
            match tokio::time::timeout(rem, &mut r.join).await {
                Ok(_) => {
                    drop(r.permit);
                    self.inflight.fetch_sub(1, Ordering::SeqCst);
                }
                Err(_) => still.push(r),
            }
        }
        if !still.is_empty() {
            *self.retained.lock().unwrap_or_else(|e| e.into_inner()) = still;
        }
    }
}
