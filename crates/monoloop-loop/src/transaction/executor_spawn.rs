//! Spawn on an injected [`tokio::runtime::Handle`] (D-032).
//!
//! Synchronous admission must not rely on ambient `tokio::spawn`. On multi-thread
//! executors, [`try_spawn`] does not return `Ok` until the task has been polled
//! once (or cancelled), so concurrent executor shutdown cannot admit never-started
//! work. Already-spawned tasks always run their futures — the gate only blocks
//! *new* spawns.

use super::spawn_gate::SpawnGate;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::task::JoinHandle;

/// Spawn `future` on `executor` without requiring an entered Tokio context.
///
/// Returns `Err(())` when the gate is closed, spawn panics, or the task is
/// cancelled without ever starting. On multi-thread runtimes, waits until first
/// poll (or cancel) so the executor-shutdown race fails closed. On current-thread
/// (typical in unit tests), returns after the immediate finished check only to
/// avoid deadlocking the reactor.
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
    let (started_tx, started_rx) = mpsc::channel::<()>();
    let wrapped = async move {
        let _ = started_tx.send(());
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

    let on_current_thread = Handle::try_current()
        .map(|h| h.runtime_flavor() == RuntimeFlavor::CurrentThread)
        .unwrap_or(false);
    if on_current_thread {
        // Cannot wait for first poll without deadlocking the reactor.
        return Ok(handle);
    }

    let confirm = || confirm_started(handle, started_rx, &started);
    match Handle::try_current() {
        Ok(_) => tokio::task::block_in_place(confirm),
        Err(_) => confirm(),
    }
}

fn confirm_started<T>(
    handle: JoinHandle<T>,
    started_rx: mpsc::Receiver<()>,
    started: &AtomicBool,
) -> Result<JoinHandle<T>, ()> {
    // Fail closed if start never arrives (do not accept unstarted after a timeout).
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if started_rx.try_recv().is_ok() || started.load(Ordering::SeqCst) {
            return Ok(handle);
        }
        if handle.is_finished() && !started.load(Ordering::SeqCst) {
            return Err(());
        }
        if Instant::now() >= deadline {
            handle.abort();
            return Err(());
        }
        match started_rx.recv_timeout(Duration::from_millis(1)) {
            Ok(()) => return Ok(handle),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return if started.load(Ordering::SeqCst) {
                    Ok(handle)
                } else {
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
}
