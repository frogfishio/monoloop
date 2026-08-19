//! Supervisor command vocabulary (M2 scaffold).

use monoloop_contracts::TransactionId;

/// Bounded commands workers send to the unique supervisor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SupervisorCommand {
    /// Start an admitted queued transaction.
    Start(TransactionId),
    /// Request cooperative cancel.
    Cancel(TransactionId),
    /// Request forced terminate.
    ForceTerminate(TransactionId),
    /// Worker observed a terminal condition (supervisor selects cause).
    ObserveTerminal {
        /// Transaction.
        transaction_id: TransactionId,
        /// Opaque worker-local cause tag (typed in M3).
        cause_tag: &'static str,
    },
}
