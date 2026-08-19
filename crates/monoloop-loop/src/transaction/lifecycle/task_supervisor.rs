//! Global task join ownership (M2 scaffold).

use monoloop_contracts::{ExchangeId, ToolExecutionId, TransactionId};
use std::collections::HashMap;

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

/// Retains every runtime join until observed complete (M2).
#[derive(Debug, Default)]
pub struct TaskSupervisor {
    next_id: u64,
    by_transaction: HashMap<TransactionId, Vec<TaskId>>,
}

impl TaskSupervisor {
    /// Empty supervisor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a task id (registration before start gate — M2).
    pub fn allocate(&mut self, class: TaskClass) -> TaskId {
        let id = TaskId(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        if let Some(tx) = class.transaction_id() {
            self.by_transaction.entry(tx).or_default().push(id);
        }
        id
    }

    /// Tasks still associated with a transaction.
    pub fn tasks_for(&self, tx: &TransactionId) -> &[TaskId] {
        self.by_transaction
            .get(tx)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Total registered task ids (scaffold; not yet tied to JoinSet).
    pub fn registered_count(&self) -> usize {
        self.by_transaction.values().map(Vec::len).sum()
    }
}

impl TaskClass {
    fn transaction_id(&self) -> Option<TransactionId> {
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
