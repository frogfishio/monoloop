//! Process-local spawn admission gate (D-032).
//!
//! Closed after supervisor callback finalization during shutdown so
//! `try_spawn` fails closed for subsequent work while finalized callbacks
//! can still be scheduled.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Shared flag: when false, [`super::executor_spawn::try_spawn`] rejects work.
#[derive(Clone, Debug, Default)]
pub struct SpawnGate {
    open: Arc<AtomicBool>,
}

impl SpawnGate {
    /// Create an open gate.
    pub fn open() -> Self {
        Self {
            open: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Whether new spawns are accepted.
    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Close permanently (shutdown). Idempotent.
    pub fn close(&self) {
        self.open.store(false, Ordering::Release);
    }
}
