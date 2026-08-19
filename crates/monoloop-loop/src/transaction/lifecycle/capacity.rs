//! RAII transaction reservations (M2 scaffold).
//!
//! Counter-only acquire/release APIs are forbidden (v2 §9.3).

/// Placeholder reservation bundle; real `OwnedPermit` fields land in M2.
#[derive(Debug, Default)]
pub struct TransactionReservations {
    _marker: (),
}

impl TransactionReservations {
    /// Empty scaffold reservations (no permits held).
    pub fn empty() -> Self {
        Self { _marker: () }
    }
}
