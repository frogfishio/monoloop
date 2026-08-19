//! Transaction Runtime v2 lifecycle subsystem.
//!
//! Normative specification: `doc/TRANSACTION_RUNTIME_V2_SPEC.md`.
//!
//! This module replaces the deleted v1 lifecycle files with one cohesive
//! ownership model. **M2** lands owner/executor, task supervisor, ledger,
//! RAII reservations, and synchronous admission.

mod admission;
mod capacity;
mod coordinator;
mod delivery;
mod ledger;
mod owner;
mod shutdown;
mod supervisor;
mod task_supervisor;
mod terminal;

pub use capacity::{ReservationPool, ReservationPoolError, TransactionReservations};
pub use coordinator::TransactionCoordinator;
pub use delivery::{
    adapt_completion_callback, adapt_event_sink, HostCompletionAdapter, HostEventAdapter,
};
pub use ledger::{LedgerEntry, LifecycleLedger, TransactionPhase};
pub use owner::{RuntimeOwner, StartedRuntime, TransactionRuntimeHandle};
pub use shutdown::ShutdownTicket;
pub use supervisor::SupervisorCommand;
pub use task_supervisor::{TaskClass, TaskExit, TaskId, TaskSupervisor};
pub use terminal::{build_completion, TerminalDecision};

#[cfg(test)]
mod tests;
