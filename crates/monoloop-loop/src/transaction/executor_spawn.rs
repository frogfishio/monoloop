//! Spawn on an injected [`tokio::runtime::Handle`] (D-032).
//!
//! Synchronous admission must not rely on ambient `tokio::spawn`. Spawns are
//! non-blocking and fail closed when the [`SpawnGate`] is closed, spawn panics,
//! or the join is already cancelled without starting. Concurrent executor
//! shutdown after return is narrowed by a post-spawn yield + recheck; the task
//! body also refuses caller work if the gate closed before first poll.

use super::spawn_gate::SpawnGate;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

/// Spawn `future` on `executor` without requiring an entered Tokio context.
///
/// Returns `Err(())` when the gate is closed, spawn panics, or the join is
/// already cancelled without the task having started (executor shut down).
pub(crate) fn try_spawn<F>(
    executor: &Handle,
    gate: &SpawnGate,
    future: F,
) -> Result<JoinHandle<F::Output>, ()>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if !gate.is_open() {
        return Err(());
    }
    let started = Arc::new(AtomicBool::new(false));
    let started_flag = Arc::clone(&started);
    let gate_body = gate.clone();
    let wrapped = async move {
        if !gate_body.is_open() {
            // Gate closed between spawn and first poll — do not run caller work.
            std::future::pending::<F::Output>().await
        } else {
            started_flag.store(true, Ordering::SeqCst);
            future.await
        }
    };
    let handle = catch_unwind(AssertUnwindSafe(|| executor.spawn(wrapped))).map_err(|_| ())?;
    if !gate.is_open() {
        handle.abort();
        return Err(());
    }
    // Detect executor-shutdown cancel that completed before first poll.
    // One scheduler yield narrows the race without a timed wait (non-blocking admit).
    if handle.is_finished() && !started.load(Ordering::SeqCst) {
        return Err(());
    }
    std::thread::yield_now();
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
        let gate = SpawnGate::open();
        rt.shutdown_timeout(Duration::from_millis(200));
        let result = try_spawn(&handle, &gate, async { 1u8 });
        assert!(result.is_err(), "shut-down executor must fail closed");
    }

    #[test]
    fn try_spawn_rejects_closed_gate() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();
        let gate = SpawnGate::open();
        gate.close();
        assert!(try_spawn(&handle, &gate, async { 1u8 }).is_err());
        drop(rt);
    }

    #[test]
    fn try_spawn_is_non_blocking_on_live_runtime() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();
        let gate = SpawnGate::open();
        let start = std::time::Instant::now();
        let join = try_spawn(&handle, &gate, async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            1u8
        })
        .expect("live spawn");
        assert!(
            start.elapsed() < Duration::from_millis(20),
            "try_spawn must not wait for task start"
        );
        join.abort();
        drop(rt);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_spawn_accepts_live_runtime() {
        let handle = tokio::runtime::Handle::current();
        let gate = SpawnGate::open();
        let join = try_spawn(&handle, &gate, async { 7u8 }).expect("live spawn");
        assert_eq!(join.await.expect("join"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_spawn_accepts_already_ready_future() {
        let handle = tokio::runtime::Handle::current();
        let gate = SpawnGate::open();
        let join = try_spawn(&handle, &gate, std::future::ready(9u8)).expect("ready spawn");
        assert_eq!(join.await.expect("join"), 9);
    }
}
