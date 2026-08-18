//! Spawn on an injected [`tokio::runtime::Handle`] (D-032).
//!
//! Synchronous admission and other host-facing paths must not rely on ambient
//! `tokio::spawn`, which panics when no reactor is entered on the calling thread.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::task::JoinHandle;

/// Spawn `future` on `executor` without requiring an entered Tokio context.
///
/// Returns `Err(())` when the executor rejects the spawn (panic *or* shut down —
/// including the race where shutdown wins after `spawn` returns but before the
/// task's first poll), so callers can map to typed admission/startup failure.
pub(crate) fn try_spawn<F>(executor: &Handle, future: F) -> Result<JoinHandle<F::Output>, ()>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Rendezvous: first poll of the wrapper signals start. If the task is
    // cancelled without polling, the sender is dropped and the receiver errors.
    let (started_tx, started_rx) = mpsc::channel::<()>();
    let wrapped = async move {
        let _ = started_tx.send(());
        future.await
    };
    let handle = catch_unwind(AssertUnwindSafe(|| executor.spawn(wrapped))).map_err(|_| ())?;

    // Already-shut-down executors cancel synchronously before first poll.
    if handle.is_finished() {
        return match started_rx.try_recv() {
            Ok(()) => Ok(handle), // completed extremely fast after starting
            Err(_) => Err(()),
        };
    }

    // Multi-thread (production): wait until the task starts or is cancelled so
    // a concurrent shutdown cannot return success for never-started work.
    // Current-thread: waiting would deadlock the caller that owns the reactor,
    // so only the immediate check above applies there.
    let on_current_thread = Handle::try_current()
        .map(|h| h.runtime_flavor() == RuntimeFlavor::CurrentThread)
        .unwrap_or(false);
    if on_current_thread {
        return Ok(handle);
    }

    let confirm = || confirm_started(handle, started_rx);
    match Handle::try_current() {
        Ok(_) => tokio::task::block_in_place(confirm),
        Err(_) => confirm(),
    }
}

fn confirm_started<T>(
    handle: JoinHandle<T>,
    started_rx: mpsc::Receiver<()>,
) -> Result<JoinHandle<T>, ()> {
    // Bound the wait so a starved scheduler cannot hang admission forever.
    let deadline = Instant::now() + Duration::from_millis(50);
    loop {
        if started_rx.try_recv().is_ok() {
            return Ok(handle);
        }
        if handle.is_finished() {
            // Cancelled without start, or completed after start (try_recv raced).
            return match started_rx.try_recv() {
                Ok(()) => Ok(handle),
                Err(mpsc::TryRecvError::Empty) | Err(mpsc::TryRecvError::Disconnected) => Err(()),
            };
        }
        if Instant::now() >= deadline {
            // Still queued on a live scheduler after the bound — accept.
            return Ok(handle);
        }
        match started_rx.recv_timeout(Duration::from_millis(1)) {
            Ok(()) => return Ok(handle),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return if handle.is_finished() {
                    Err(())
                } else {
                    // Sender dropped without finish is unexpected; fail closed.
                    Err(())
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn try_spawn_rejects_shutdown_runtime() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();
        rt.shutdown_timeout(Duration::from_millis(200));
        let result = try_spawn(&handle, async { 1u8 });
        assert!(result.is_err(), "shut-down executor must fail closed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_spawn_accepts_live_runtime() {
        let handle = tokio::runtime::Handle::current();
        let join = try_spawn(&handle, async { 7u8 }).expect("live spawn");
        assert_eq!(join.await.expect("join"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_spawn_accepts_already_ready_future() {
        let handle = tokio::runtime::Handle::current();
        let join = try_spawn(&handle, std::future::ready(9u8)).expect("ready spawn");
        assert_eq!(join.await.expect("join"), 9);
    }
}
