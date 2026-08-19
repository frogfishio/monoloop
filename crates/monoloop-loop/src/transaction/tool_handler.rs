//! Linked tool handlers, execution handles, and cancellation controls.

use monoloop_contracts::{
    ToolCall, ToolCallContext, ToolCompletion, ToolExecutionId, ToolStartError,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, Notify};
use tokio::task::AbortHandle;

/// Host-linked tool implementation.
pub trait ToolHandler: Send + Sync {
    /// Start one execution. Must return a handle with a single completion.
    fn start(
        &self,
        call: ToolCall,
        context: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError>;

    /// Whether cooperative/abort cancellation is honored (D-024 / D-028).
    /// Default **false** (fail-closed): capability booleans must not self-assert.
    fn supports_abort(&self) -> bool {
        false
    }

    /// Whether isolated kill after grace is available (D-024 / D-028).
    /// Default **false** (fail-closed); requires a structural [`ToolKillHandle`].
    fn supports_isolated_kill(&self) -> bool {
        false
    }
}

/// Cancellation control for a running linked tool.
#[derive(Clone, Debug)]
pub struct ToolExecutionControl {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl ToolExecutionControl {
    /// Create a fresh control channel.
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Request cooperative/abort cancel (idempotent).
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Whether cancel was requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Wait until cancelled.
    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            self.notify.notified().await;
        }
    }
}

impl Default for ToolExecutionControl {
    fn default() -> Self {
        Self::new()
    }
}

/// One-shot completion consumer for a linked tool execution.
#[derive(Debug)]
pub struct ToolExecutionCompletion {
    rx: oneshot::Receiver<ToolCompletion>,
}

impl ToolExecutionCompletion {
    /// Wrap a receiver (exactly-once consumption via [`Self::wait`]).
    pub fn new(rx: oneshot::Receiver<ToolCompletion>) -> Self {
        Self { rx }
    }

    /// Await the single completion (or lost-completion if dropped).
    pub async fn wait(self) -> ToolCompletion {
        self.rx.await.unwrap_or(ToolCompletion::RuntimeFailed(
            monoloop_contracts::ToolRuntimeError::CompletionLost,
        ))
    }
}

/// Force-stop + join handle for IsolatedKillable / Abortable workers (D-024).
///
/// Owns the worker `tokio::task::JoinHandle` so kill can be followed by a real
/// join. Timed waits must not drop the handle on timeout (put-back) or the
/// worker would detach while capacity is released.
#[derive(Clone, Debug)]
pub struct ToolKillHandle {
    abort: AbortHandle,
    join: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl ToolKillHandle {
    /// Take ownership of a worker task (abort + join).
    pub fn new(join: tokio::task::JoinHandle<()>) -> Self {
        Self {
            abort: join.abort_handle(),
            join: Arc::new(Mutex::new(Some(join))),
        }
    }

    /// Abort the isolated worker task (idempotent).
    pub fn kill(&self) {
        self.abort.abort();
    }

    /// Await worker teardown. On timeout, restores the join handle so the
    /// worker is not detached; caller must keep capacity until a later join.
    pub async fn join_timeout(&self, budget: std::time::Duration) -> Result<(), ()> {
        let handle = self.join.lock().unwrap_or_else(|e| e.into_inner()).take();
        let Some(mut handle) = handle else {
            return Ok(()); // already joined
        };
        match tokio::time::timeout(budget, &mut handle).await {
            Ok(_) => Ok(()),
            Err(_) => {
                // Put back — do not detach on timeout.
                *self.join.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
                Err(())
            }
        }
    }

    /// Await worker teardown after kill (unbounded). Prefer after abort so the
    /// join completes when the task is cancelled; keeps ownership until Ready.
    pub async fn join(&self) {
        let handle = self.join.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(h) = handle {
            let _ = h.await;
        }
    }
}

/// Handle returned from [`ToolHandler::start`].
#[derive(Debug)]
pub struct LinkedToolExecutionHandle {
    /// Stable execution id for this start.
    pub execution_id: ToolExecutionId,
    /// Cancellation control.
    pub control: ToolExecutionControl,
    /// Exactly-once completion.
    pub completion: ToolExecutionCompletion,
    /// Optional kill handle for escalate-after-grace (D-024).
    pub kill: Option<ToolKillHandle>,
}

/// Handler that completes immediately from a synchronous function.
pub struct ImmediateToolHandler<F> {
    f: F,
}

impl<F> ImmediateToolHandler<F>
where
    F: Fn(ToolCall, ToolCallContext) -> Result<ToolCompletion, ToolStartError> + Send + Sync,
{
    /// Construct from a function.
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> ToolHandler for ImmediateToolHandler<F>
where
    F: Fn(ToolCall, ToolCallContext) -> Result<ToolCompletion, ToolStartError> + Send + Sync,
{
    fn start(
        &self,
        call: ToolCall,
        context: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError> {
        let completion = (self.f)(call, context)?;
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(completion);
        Ok(LinkedToolExecutionHandle {
            execution_id: ToolExecutionId::generate(),
            control: ToolExecutionControl::new(),
            completion: ToolExecutionCompletion::new(rx),
            kill: None,
        })
    }
}

type BoxFut = Pin<Box<dyn Future<Output = ToolCompletion> + Send>>;

/// Handler that runs an async body with abortable cancellation.
pub struct AsyncToolHandler<F> {
    f: F,
}

impl<F> AsyncToolHandler<F>
where
    F: Fn(ToolCall, ToolCallContext, ToolExecutionControl) -> BoxFut + Send + Sync,
{
    /// Construct from a function that returns a boxed future.
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> ToolHandler for AsyncToolHandler<F>
where
    F: Fn(ToolCall, ToolCallContext, ToolExecutionControl) -> BoxFut + Send + Sync,
{
    fn start(
        &self,
        call: ToolCall,
        context: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError> {
        let control = ToolExecutionControl::new();
        let control_body = control.clone();
        let fut = (self.f)(call, context, control_body.clone());
        let (tx, rx) = oneshot::channel();
        // Single owned worker: select cancel vs body (no detached watcher — LAW 23).
        let join = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = control_body.cancelled() => {
                    let _ = tx.send(ToolCompletion::RuntimeFailed(
                        monoloop_contracts::ToolRuntimeError::TerminationFailed,
                    ));
                }
                result = fut => {
                    let _ = tx.send(result);
                }
            }
        });
        let kill = ToolKillHandle::new(join);
        Ok(LinkedToolExecutionHandle {
            execution_id: ToolExecutionId::generate(),
            control,
            completion: ToolExecutionCompletion::new(rx),
            kill: Some(kill),
        })
    }

    fn supports_abort(&self) -> bool {
        true
    }
}

/// Isolated worker that ignores cooperative cancel until [`ToolKillHandle::kill`] (D-024 tests).
pub struct IsolatedKillableToolHandler<F> {
    f: F,
}

impl<F> IsolatedKillableToolHandler<F>
where
    F: Fn(ToolCall, ToolCallContext) -> BoxFut + Send + Sync,
{
    /// Construct from a function that returns a boxed future.
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> ToolHandler for IsolatedKillableToolHandler<F>
where
    F: Fn(ToolCall, ToolCallContext) -> BoxFut + Send + Sync,
{
    fn start(
        &self,
        call: ToolCall,
        context: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError> {
        let control = ToolExecutionControl::new();
        let fut = (self.f)(call, context);
        let (tx, rx) = oneshot::channel();
        let join = tokio::spawn(async move {
            let result = fut.await;
            let _ = tx.send(result);
        });
        let kill = ToolKillHandle::new(join);
        Ok(LinkedToolExecutionHandle {
            execution_id: ToolExecutionId::generate(),
            control,
            completion: ToolExecutionCompletion::new(rx),
            kill: Some(kill),
        })
    }

    fn supports_abort(&self) -> bool {
        // Cooperative cancel alone does not stop this worker.
        false
    }

    fn supports_isolated_kill(&self) -> bool {
        true
    }
}

/// Handler that always fails at start (tests).
#[derive(Debug, Default)]
pub struct StartFailHandler {
    /// Rejection message.
    pub reason: &'static str,
}

impl ToolHandler for StartFailHandler {
    fn start(
        &self,
        _call: ToolCall,
        _context: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError> {
        Err(ToolStartError::Rejected(self.reason))
    }
}

/// Handler that panics on start (tests).
#[derive(Debug, Default)]
pub struct PanicOnStartHandler;

impl ToolHandler for PanicOnStartHandler {
    fn start(
        &self,
        _call: ToolCall,
        _context: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError> {
        panic!("deliberate tool panic");
    }
}

/// Handler whose completion is never sent (tests).
#[derive(Debug, Default)]
pub struct LostCompletionHandler;

impl ToolHandler for LostCompletionHandler {
    fn start(
        &self,
        _call: ToolCall,
        _context: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError> {
        let (tx, rx) = oneshot::channel();
        drop(tx);
        Ok(LinkedToolExecutionHandle {
            execution_id: ToolExecutionId::generate(),
            control: ToolExecutionControl::new(),
            completion: ToolExecutionCompletion::new(rx),
            kill: None,
        })
    }
}
