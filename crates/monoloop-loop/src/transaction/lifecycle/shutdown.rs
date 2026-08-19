//! Quiescing / stopped wait surface (M1 types; M2–M6 behavior).

/// Opaque ticket from [`super::owner::RuntimeOwner::begin_shutdown`].
#[derive(Clone, Debug)]
pub struct ShutdownTicket {
    pub(crate) generation: u64,
}

impl ShutdownTicket {
    pub(crate) fn scaffold() -> Self {
        Self { generation: 0 }
    }

    /// Shutdown generation shared by concurrent waiters.
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Placeholder export so call sites can name the M2 entrypoint.
pub fn begin_shutdown_placeholder() -> ShutdownTicket {
    ShutdownTicket::scaffold()
}
