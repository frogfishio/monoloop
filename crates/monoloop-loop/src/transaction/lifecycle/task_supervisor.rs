//! Global task join ownership (v2 §7.3).
//!
//! Every runtime task is registered before its start gate is released. Abort
//! retains the join until the result is observed. Live `JoinHandle` values are
//! never dropped.

use futures_util::FutureExt;
use monoloop_contracts::{ExchangeId, ToolExecutionId, TransactionId};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use tokio::task::{AbortHandle, JoinSet};

/// Stable task id within one runtime owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TaskId(pub u64);

/// Classification for every supervised spawn.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TaskClass {
    /// Per-transaction coordinator.
    TransactionCoordinator(TransactionId),
    /// Event publisher task for one transaction.
    EventPublisher(TransactionId),
    /// Connector ownership task.
    ConnectorOwner(TransactionId, ExchangeId),
    /// Interpreter pump task.
    InterpreterOwner(TransactionId, ExchangeId),
    /// Tool worker.
    ToolWorker(TransactionId, ToolExecutionId),
    /// MCP request task.
    McpRequest(TransactionId),
    /// Runtime-wide service (MCP listener, etc.).
    RuntimeService,
}

impl TaskClass {
    /// Owning transaction when task-scoped.
    pub fn transaction_id(&self) -> Option<TransactionId> {
        match self {
            Self::TransactionCoordinator(t)
            | Self::EventPublisher(t)
            | Self::ConnectorOwner(t, _)
            | Self::InterpreterOwner(t, _)
            | Self::ToolWorker(t, _)
            | Self::McpRequest(t) => Some(*t),
            Self::RuntimeService => None,
        }
    }
}

/// Observed task exit kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskExit {
    /// Future completed normally.
    Completed,
    /// Task was aborted / cancelled.
    Cancelled,
    /// Join observed a panic.
    Panicked,
}

#[derive(Debug)]
struct TaskMeta {
    class: TaskClass,
    abort: AbortHandle,
    abort_requested: bool,
}

/// Retains every runtime join until observed complete.
#[derive(Debug)]
pub struct TaskSupervisor {
    joins: JoinSet<(TaskId, TaskExit)>,
    meta: HashMap<TaskId, TaskMeta>,
    by_transaction: HashMap<TransactionId, HashSet<TaskId>>,
    next_id: u64,
}

impl Default for TaskSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskSupervisor {
    /// Empty supervisor.
    pub fn new() -> Self {
        Self {
            joins: JoinSet::new(),
            meta: HashMap::new(),
            by_transaction: HashMap::new(),
            next_id: 1,
        }
    }

    /// Number of tasks still registered (including abort-requested).
    pub fn registered_count(&self) -> usize {
        self.meta.len()
    }

    /// Whether stopped proof can assert zero owned tasks.
    pub fn is_empty(&self) -> bool {
        self.meta.is_empty() && self.joins.is_empty()
    }

    /// Tasks still associated with a transaction.
    pub fn tasks_for(&self, tx: &TransactionId) -> Vec<TaskId> {
        self.by_transaction
            .get(tx)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Register then spawn. The future does not run until after registration
    /// completes (start-gate released at the end of this method).
    pub fn spawn<F>(&mut self, class: TaskClass, future: F) -> TaskId
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let id = TaskId(self.next_id);
        self.next_id = self.next_id.saturating_add(1);

        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        let abort = self.joins.spawn(async move {
            // Fail closed if the gate is dropped before release (abort-before-start).
            match gate_rx.await {
                Ok(()) => match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(()) => (id, TaskExit::Completed),
                    Err(_) => (id, TaskExit::Panicked),
                },
                Err(_) => (id, TaskExit::Cancelled),
            }
        });

        if let Some(tx) = class.transaction_id() {
            self.by_transaction.entry(tx).or_default().insert(id);
        }
        self.meta.insert(
            id,
            TaskMeta {
                class,
                abort,
                abort_requested: false,
            },
        );
        // Release start gate only after registration is complete.
        let _ = gate_tx.send(());
        id
    }

    /// Request abort; task remains registered until join is observed.
    pub fn abort(&mut self, id: TaskId) {
        if let Some(meta) = self.meta.get_mut(&id) {
            meta.abort_requested = true;
            meta.abort.abort();
        }
    }

    /// Abort all tasks for a transaction.
    pub fn abort_transaction(&mut self, tx: &TransactionId) {
        let ids = self.tasks_for(tx);
        for id in ids {
            self.abort(id);
        }
    }

    /// Abort every registered task (shutdown).
    pub fn abort_all(&mut self) {
        let ids: Vec<_> = self.meta.keys().copied().collect();
        for id in ids {
            self.abort(id);
        }
    }

    /// Abort all tasks and observe joins until the set is empty.
    pub async fn abort_and_drain(&mut self) {
        self.abort_all();
        while self.join_next().await.is_some() {}
        self.meta.clear();
        self.by_transaction.clear();
    }

    /// Poll for the next finished task and deregister it.
    pub async fn join_next(&mut self) -> Option<(TaskId, TaskClass, TaskExit)> {
        let finished = self.joins.join_next().await?;
        match finished {
            Ok((id, exit)) => {
                let exit = if self.meta.get(&id).is_some_and(|m| m.abort_requested)
                    && matches!(exit, TaskExit::Completed)
                {
                    TaskExit::Cancelled
                } else {
                    exit
                };
                let class = self.deregister(id)?;
                Some((id, class, exit))
            }
            Err(err) => {
                // Abort/panic dropped the future before it returned our tuple.
                let exit = if err.is_cancelled() {
                    TaskExit::Cancelled
                } else {
                    TaskExit::Panicked
                };
                let id = self.find_finished_meta()?;
                let class = self.deregister(id)?;
                Some((id, class, exit))
            }
        }
    }

    /// Non-blocking reap of already-finished joins.
    pub fn try_reap_finished(&mut self) -> Vec<(TaskId, TaskClass, TaskExit)> {
        let mut out = Vec::new();
        while let Some(finished) = self.joins.try_join_next() {
            match finished {
                Ok((id, exit)) => {
                    if let Some(class) = self.deregister(id) {
                        out.push((id, class, exit));
                    }
                }
                Err(err) => {
                    let exit = if err.is_cancelled() {
                        TaskExit::Cancelled
                    } else {
                        TaskExit::Panicked
                    };
                    if let Some(id) = self.find_finished_meta() {
                        if let Some(class) = self.deregister(id) {
                            out.push((id, class, exit));
                        }
                    }
                }
            }
        }
        out
    }

    fn find_finished_meta(&self) -> Option<TaskId> {
        self.meta
            .iter()
            .find(|(_, m)| m.abort.is_finished())
            .map(|(id, _)| *id)
    }

    fn deregister(&mut self, id: TaskId) -> Option<TaskClass> {
        let meta = self.meta.remove(&id)?;
        if let Some(tx) = meta.class.transaction_id() {
            if let Some(set) = self.by_transaction.get_mut(&tx) {
                set.remove(&id);
                if set.is_empty() {
                    self.by_transaction.remove(&tx);
                }
            }
        }
        Some(meta.class)
    }
}
