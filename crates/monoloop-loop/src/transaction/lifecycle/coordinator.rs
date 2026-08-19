//! Per-transaction coordinator state machine (M3 scaffold).

/// Coordinator placeholder until M3 wires Interpreter units and terminal publish.
#[derive(Debug, Default)]
pub struct TransactionCoordinator {
    _private: (),
}

impl TransactionCoordinator {
    /// Construct an inert coordinator.
    pub fn new() -> Self {
        Self::default()
    }
}
