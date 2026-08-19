//! Transaction Runtime v2 lifecycle subsystem.
//!
//! Normative specification: `doc/TRANSACTION_RUNTIME_V2_SPEC.md`.
//!
//! This module replaces the deleted v1 lifecycle files (`runtime`, `admission`,
//! `actor`, `finalization`, `callback_service`, `executor_spawn`,
//! `tool_join_vault`) with one cohesive ownership model. Stages land per the
//! migration plan (M1 delivery/shutdown contracts → M2 owner/ledger → …).

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

pub use admission::rejecting;
pub use capacity::TransactionReservations;
pub use coordinator::TransactionCoordinator;
pub use delivery::{
    adapt_completion_callback, adapt_event_sink, HostCompletionAdapter, HostEventAdapter,
};
pub use ledger::{LedgerEntry, LifecycleLedger, TransactionPhase};
pub use owner::{RuntimeOwner, StartedRuntime, TransactionRuntimeHandle};
pub use shutdown::{begin_shutdown_placeholder, ShutdownTicket};
pub use supervisor::SupervisorCommand;
pub use task_supervisor::{TaskClass, TaskId, TaskSupervisor};
pub use terminal::{build_completion, TerminalDecision};
