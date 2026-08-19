//! Unique runtime owner and cloneable control handle (M2 scaffold).

use super::shutdown::ShutdownTicket;
use monoloop_contracts::{
    ShutdownWaitOutcome, TerminationDisposition, TerminationMode, TransactionSelector,
};
use std::time::Duration;

/// Unique owner of the executor, supervisor, ledger, and shutdown state.
///
/// Annotated `must_use`: dropping before `Stopped` is a contract violation; Drop
/// still preserves ownership (may block on non-cooperative work).
#[must_use = "RuntimeOwner must begin_shutdown and wait_stopped until Stopped"]
pub struct RuntimeOwner {
    _private: (),
}

/// Cloneable admission/control handle (no executor shutdown authority).
#[derive(Clone, Debug)]
pub struct TransactionRuntimeHandle {
    _private: (),
}

/// Result of a successful production start handshake.
pub struct StartedRuntime {
    /// Unique owner.
    pub owner: RuntimeOwner,
    /// Cloneable control handle.
    pub handle: TransactionRuntimeHandle,
}

impl RuntimeOwner {
    /// Scaffold constructor for M2 — not yet a live runtime.
    pub(crate) fn new_scaffold() -> Self {
        Self { _private: () }
    }

    /// Begin shutdown (idempotent). Scaffold: returns an inert ticket.
    pub fn begin_shutdown(&self) -> ShutdownTicket {
        ShutdownTicket::scaffold()
    }

    /// Wait until stopped or the deadline elapses. Scaffold always times out.
    pub async fn wait_stopped(&mut self, _deadline: Duration) -> ShutdownWaitOutcome {
        ShutdownWaitOutcome::TimedOut(monoloop_contracts::ShutdownSnapshot::default())
    }
}

impl TransactionRuntimeHandle {
    pub(crate) fn new_scaffold() -> Self {
        Self { _private: () }
    }

    /// Terminate scaffold (always `NotFound` until M2/M3).
    pub fn terminate(
        &self,
        _selector: TransactionSelector,
        _mode: TerminationMode,
    ) -> TerminationDisposition {
        TerminationDisposition::NotFound
    }
}

impl StartedRuntime {
    /// Scaffold start product for wiring tests before M2 lands.
    pub fn scaffold() -> Self {
        Self {
            owner: RuntimeOwner::new_scaffold(),
            handle: TransactionRuntimeHandle::new_scaffold(),
        }
    }
}
