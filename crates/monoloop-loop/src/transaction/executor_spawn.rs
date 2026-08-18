//! Spawn on an injected [`tokio::runtime::Handle`] (D-032).
//!
//! Synchronous admission and other host-facing paths must not rely on ambient
//! `tokio::spawn`, which panics when no reactor is entered on the calling thread.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

/// Spawn `future` on `executor` without requiring an entered Tokio context.
///
/// Returns `Err(())` when the executor rejects the spawn (e.g. shut down), so
/// callers can map to typed admission/startup failure and roll back reservations.
pub(crate) fn try_spawn<F>(executor: &Handle, future: F) -> Result<JoinHandle<F::Output>, ()>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    catch_unwind(AssertUnwindSafe(|| executor.spawn(future))).map_err(|_| ())
}
