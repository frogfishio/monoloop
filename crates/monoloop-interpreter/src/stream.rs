//! Bounded canonical event stream (single Interpretation output).

use monoloop_contracts::{InterpreterError, InterpreterOutputEvent};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Cloneable receiver handle for canonical events from one Interpretation.
#[derive(Debug)]
pub struct CanonicalEventStream {
    rx: Mutex<mpsc::Receiver<InterpreterOutputEvent>>,
}

impl CanonicalEventStream {
    pub(crate) fn new(rx: mpsc::Receiver<InterpreterOutputEvent>) -> Self {
        Self { rx: Mutex::new(rx) }
    }

    /// Receive the next event. `None` when the stream is closed after terminal end
    /// was already delivered (or the owner dropped without end — treated as loss).
    pub async fn recv(&self) -> Option<InterpreterOutputEvent> {
        let mut guard = self.rx.lock().await;
        guard.recv().await
    }
}

/// Shared publisher used by the interpretation owner task.
#[derive(Clone)]
pub(crate) struct EventPublisher {
    tx: mpsc::Sender<InterpreterOutputEvent>,
    count: Arc<std::sync::atomic::AtomicU64>,
}

impl EventPublisher {
    pub(crate) fn new(capacity: usize) -> (Self, CanonicalEventStream) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (
            Self {
                tx,
                count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            },
            CanonicalEventStream::new(rx),
        )
    }

    pub(crate) fn count(&self) -> u64 {
        self.count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Publish with backpressure. Does not drop events.
    pub(crate) async fn publish(
        &self,
        event: InterpreterOutputEvent,
    ) -> Result<(), InterpreterError> {
        self.tx
            .send(event)
            .await
            .map_err(|_| InterpreterError::backpressure())?;
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}
