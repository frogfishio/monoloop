//! Process-local spawn admission gate (D-032).
//!
//! Closed at the start of runtime shutdown *before* draining actors so
//! `try_spawn` fails closed instead of returning success for work that will
//! never be polled.

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
