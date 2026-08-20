//! Worker-facing spawn proxy into the supervisor-owned [`TaskSupervisor`] (v2 §7.3 / §16).

use super::task_supervisor::{TaskClass, TaskId};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Request to register+spawn a future on the supervisor's TaskSupervisor.
pub(crate) struct SpawnRequest {
    pub class: TaskClass,
    pub future: BoxFuture,
    pub reply: oneshot::Sender<TaskId>,
}

/// Cloneable handle workers use to spawn owned tasks without holding JoinHandles.
#[derive(Clone, Debug)]
pub struct TransactionTaskSpawner {
    tx: mpsc::Sender<SpawnRequest>,
}

/// Why [`TransactionTaskSpawner::spawn`] rejected or could not confirm ownership.
pub enum SpawnReject {
    /// Mailbox full before accept; caller still owns the future (drive or drop).
    Busy {
        /// Unspawned future.
        future: BoxFuture,
    },
    /// Channel closed before accept; caller still owns the future.
    Rejected {
        /// Unspawned future.
        future: BoxFuture,
    },
    /// `try_send` succeeded but TaskId reply was lost (e.g. shutdown drained the
    /// request without spawning, or reply send failed). The future is **not**
    /// returned — caller MUST NOT drive a substitute (Law 23 / 25). Fail closed.
    Orphaned,
}

impl TransactionTaskSpawner {
    /// Create a spawner and the receiver drained by the supervisor loop.
    pub(crate) fn channel(capacity: usize) -> (Self, mpsc::Receiver<SpawnRequest>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (Self { tx }, rx)
    }

    /// Register then spawn `future` under `class`.
    ///
    /// Uses `try_send` so workers never block forever on a full mailbox while the
    /// supervisor is in `abort_and_drain`. On Busy/Rejected before accept, the
    /// boxed future is returned so the caller can drive cleanup inline. On
    /// [`SpawnReject::Orphaned`], the future is gone from the caller — fail closed.
    pub async fn spawn<F>(&self, class: TaskClass, future: F) -> Result<TaskId, SpawnReject>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_boxed(class, Box::pin(future)).await
    }

    /// Same as [`Self::spawn`] with an already-boxed future (Busy retry).
    pub async fn spawn_boxed(
        &self,
        class: TaskClass,
        future: BoxFuture,
    ) -> Result<TaskId, SpawnReject> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let req = SpawnRequest {
            class,
            future,
            reply: reply_tx,
        };
        match self.tx.try_send(req) {
            Ok(()) => match reply_rx.await {
                Ok(id) => Ok(id),
                // Accepted into mailbox; do not invent a dummy future (Law 23/25).
                Err(_) => Err(SpawnReject::Orphaned),
            },
            Err(TrySendError::Full(req)) => Err(SpawnReject::Busy { future: req.future }),
            Err(TrySendError::Closed(req)) => Err(SpawnReject::Rejected { future: req.future }),
        }
    }

    /// Prefer supervisor ownership: bounded Busy retries, then return the last reject.
    ///
    /// Does not drive the future inline — caller decides fail-closed vs last-resort join.
    pub async fn spawn_with_busy_retry<F, C>(
        &self,
        class: TaskClass,
        future: F,
        max_retries: u32,
        mut is_cancelled: C,
    ) -> Result<TaskId, SpawnReject>
    where
        F: Future<Output = ()> + Send + 'static,
        C: FnMut() -> bool,
    {
        let mut future: BoxFuture = Box::pin(future);
        let mut attempt = 0u32;
        loop {
            if is_cancelled() {
                return Err(SpawnReject::Rejected { future });
            }
            match self.spawn_boxed(class.clone(), future).await {
                Ok(id) => return Ok(id),
                Err(SpawnReject::Busy { future: f }) => {
                    future = f;
                    if attempt >= max_retries {
                        return Err(SpawnReject::Busy { future });
                    }
                    attempt = attempt.saturating_add(1);
                    tokio::task::yield_now().await;
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                Err(other) => return Err(other),
            }
        }
    }
}
