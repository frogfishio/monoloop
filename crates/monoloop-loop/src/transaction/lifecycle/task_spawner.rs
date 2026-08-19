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

/// Spawn rejected because the supervisor queue closed, is full, or reply lost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpawnError {
    /// Supervisor is gone / not accepting spawns.
    Closed,
    /// Spawn mailbox at capacity (fail closed; do not block the worker).
    Busy,
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
    /// supervisor is in `abort_and_drain`. On `Busy`/`Closed` before accept, the
    /// boxed future is returned so the caller can drive cleanup inline.
    pub async fn spawn<F>(
        &self,
        class: TaskClass,
        future: F,
    ) -> Result<
        TaskId,
        (
            SpawnError,
            Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
        ),
    >
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let req = SpawnRequest {
            class,
            future: Box::pin(future),
            reply: reply_tx,
        };
        match self.tx.try_send(req) {
            Ok(()) => reply_rx.await.map_err(|_| {
                (
                    SpawnError::Closed,
                    Box::pin(async {}) as Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
                )
            }),
            Err(TrySendError::Full(req)) => Err((SpawnError::Busy, req.future)),
            Err(TrySendError::Closed(req)) => Err((SpawnError::Closed, req.future)),
        }
    }
}
