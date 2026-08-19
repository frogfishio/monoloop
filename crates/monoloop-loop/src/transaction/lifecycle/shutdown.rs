//! Quiescing / stopped wait surface (v2 §18).

/// Opaque ticket from [`super::owner::RuntimeOwner::begin_shutdown`].
#[derive(Clone, Debug)]
pub struct ShutdownTicket {
    /// Shutdown generation shared by concurrent waiters.
    pub(crate) generation: u64,
}

impl ShutdownTicket {
    /// Shutdown generation id.
    pub fn generation(&self) -> u64 {
        self.generation
    }
}
