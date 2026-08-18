//! Spawn on an injected [`tokio::runtime::Handle`] (D-032).
//!
//! Synchronous admission and other host-facing paths must not rely on ambient
//! `tokio::spawn`, which panics when no reactor is entered on the calling thread.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

/// Spawn `future` on `executor` without requiring an entered Tokio context.
///
/// Returns `Err(())` when the executor rejects the spawn (panic *or* already
/// shut down — Tokio returns an immediately-cancelled join without panicking),
/// so callers can map to typed admission/startup failure and roll back reservations.
pub(crate) fn try_spawn<F>(executor: &Handle, future: F) -> Result<JoinHandle<F::Output>, ()>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Detect shut-down executors: Tokio still returns a JoinHandle, but the task
    // is cancelled without ever being polled. A start flag distinguishes that
    // from a live task that simply finished very quickly.
    let started = Arc::new(AtomicBool::new(false));
    let started_flag = Arc::clone(&started);
    let wrapped = async move {
        started_flag.store(true, Ordering::SeqCst);
        future.await
    };
    let handle = catch_unwind(AssertUnwindSafe(|| executor.spawn(wrapped))).map_err(|_| ())?;
    if handle.is_finished() && !started.load(Ordering::SeqCst) {
        return Err(());
    }
    Ok(handle)
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
        // Handle remains usable as a value but must not admit new work.
        let result = try_spawn(&handle, async { 1u8 });
        assert!(result.is_err(), "shut-down executor must fail closed");
    }

    #[tokio::test]
    async fn try_spawn_accepts_live_runtime() {
        let handle = tokio::runtime::Handle::current();
        let join = try_spawn(&handle, async { 7u8 }).expect("live spawn");
        assert_eq!(join.await.expect("join"), 7);
    }

    #[tokio::test]
    async fn try_spawn_accepts_already_ready_future() {
        let handle = tokio::runtime::Handle::current();
        // Ready futures may finish before the post-spawn check observes them.
        let join = try_spawn(&handle, std::future::ready(9u8)).expect("ready spawn");
        assert_eq!(join.await.expect("join"), 9);
    }
}
