//! Spawn on an injected [`tokio::runtime::Handle`] (D-032).
//!
//! Synchronous admission must not rely on ambient `tokio::spawn`. [`try_spawn`]
//! is **non-blocking**: it never waits for first poll. Already-spawned tasks
//! always run their futures — the gate only blocks *new* spawns.
//!
//! Cancel-before-start is fail-closed via an immediate finished-without-start
//! check. Production runtimes reject current-thread executors at bootstrap so
//! admission cannot rely on a confirm path that would deadlock the reactor.

use super::spawn_gate::SpawnGate;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

/// Spawn `future` on `executor` without requiring an entered Tokio context.
///
/// Returns `Err(())` when the gate is closed, spawn panics, or the task is
/// already finished without ever starting. Does **not** wait for first poll —
/// callers that need start confirmation must use [`confirm_spawn`] (async).
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
    // Signal first poll without consulting the gate — accepted tasks must run.
    let wrapped = async move {
        started_flag.store(true, Ordering::SeqCst);
        future.await
    };
    let handle = catch_unwind(AssertUnwindSafe(|| executor.spawn(wrapped))).map_err(|_| ())?;
    if !gate.is_open() {
        handle.abort();
        return Err(());
    }
    // Fail closed only when the task is already gone without starting (e.g.
    // executor shut down between spawn and return). Never block waiting for poll.
    if handle.is_finished() && !started.load(Ordering::SeqCst) {
        return Err(());
    }
    Ok(handle)
}

/// Async start confirmation for paths that can await (shutdown callbacks).
///
/// Aborts and returns `Err` if the task finishes without starting within
/// `budget`. Does not block synchronous admission.
pub(crate) async fn confirm_spawn<T>(
    handle: JoinHandle<T>,
    started: &AtomicBool,
    budget: std::time::Duration,
) -> Result<JoinHandle<T>, ()> {
    if started.load(Ordering::SeqCst) {
        return Ok(handle);
    }
    if handle.is_finished() {
        return Err(());
    }
    if budget.is_zero() {
        // Non-blocking check only.
        return if started.load(Ordering::SeqCst) {
            Ok(handle)
        } else if handle.is_finished() {
            Err(())
        } else {
            // Scheduled but not yet polled — accept; JoinHandle remains owned.
            Ok(handle)
        };
    }
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if started.load(Ordering::SeqCst) {
            return Ok(handle);
        }
        if handle.is_finished() {
            return Err(());
        }
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            handle.abort();
            // One last observation after abort.
            if started.load(Ordering::SeqCst) {
                return Ok(handle);
            }
            return Err(());
        }
        tokio::task::yield_now().await;
        let slice = left.min(std::time::Duration::from_millis(1));
        tokio::time::sleep(slice).await;
    }
}

/// Spawn and (asynchronously) confirm first poll within `budget`.
pub(crate) async fn try_spawn_confirmed<F>(
    executor: &Handle,
    gate: &SpawnGate,
    future: F,
    budget: std::time::Duration,
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
    let wrapped = async move {
        started_flag.store(true, Ordering::SeqCst);
        future.await
    };
    let handle = catch_unwind(AssertUnwindSafe(|| executor.spawn(wrapped))).map_err(|_| ())?;
    if !gate.is_open() {
        handle.abort();
        return Err(());
    }
    if handle.is_finished() && !started.load(Ordering::SeqCst) {
        return Err(());
    }
    confirm_spawn(handle, &started, budget).await
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
    fn try_spawn_does_not_block_on_unstarted_task() {
        // Park the only worker so a spawned task cannot be polled. try_spawn must
        // still return promptly (non-blocking admission).
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();
        let gate = SpawnGate::open();
        let (lock_tx, lock_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        // Occupy the single worker.
        handle.spawn(async move {
            lock_tx.send(()).ok();
            let _ = release_rx.recv();
        });
        lock_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("worker occupied");
        let started = std::time::Instant::now();
        let result = try_spawn(&handle, &gate, async { 1u8 });
        let elapsed = started.elapsed();
        let _ = release_tx.send(());
        assert!(
            result.is_ok(),
            "scheduled task must be accepted without waiting"
        );
        assert!(
            elapsed < Duration::from_millis(500),
            "try_spawn blocked for {elapsed:?}"
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accepted_task_runs_after_gate_closes() {
        let handle = tokio::runtime::Handle::current();
        let gate = SpawnGate::open();
        let (tx, rx) = tokio::sync::oneshot::channel::<u8>();
        let join = try_spawn(&handle, &gate, async move {
            tx.send(42).unwrap();
            42u8
        })
        .expect("spawn while open");
        gate.close();
        assert_eq!(rx.await.expect("callback body must run"), 42);
        assert_eq!(join.await.expect("join"), 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_spawn_confirmed_waits_for_start() {
        let handle = tokio::runtime::Handle::current();
        let gate = SpawnGate::open();
        let join = try_spawn_confirmed(&handle, &gate, async { 3u8 }, Duration::from_secs(1))
            .await
            .expect("confirmed");
        assert_eq!(join.await.expect("join"), 3);
    }
}
