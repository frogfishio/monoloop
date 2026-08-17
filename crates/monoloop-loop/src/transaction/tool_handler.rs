//! Linked tool handlers, execution handles, and cancellation controls.

use monoloop_contracts::{
    ToolCall, ToolCallContext, ToolCompletion, ToolExecutionId, ToolStartError,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Notify};

/// Host-linked tool implementation.
pub trait ToolHandler: Send + Sync {
    /// Start one execution. Must return a handle with a single completion.
    fn start(
        &self,
        call: ToolCall,
        context: ToolCallContext,
    ) -> Result<LinkedToolExecutionHandle, ToolStartError>;
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

/// Handle returned from [`ToolHandler::start`].
#[derive(Debug)]
pub struct LinkedToolExecutionHandle {
    /// Stable execution id for this start.
    pub execution_id: ToolExecutionId,
    /// Cancellation control.
    pub control: ToolExecutionControl,
    /// Exactly-once completion.
    pub completion: ToolExecutionCompletion,
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
        let fut = (self.f)(call, context, control_body);
        let (tx, rx) = oneshot::channel();
        let join = tokio::spawn(async move {
            let result = fut.await;
            let _ = tx.send(result);
        });
        // Abort on cancel: poller task watches control.
        let control_watch = control.clone();
        tokio::spawn(async move {
            control_watch.cancelled().await;
            join.abort();
        });
        Ok(LinkedToolExecutionHandle {
            execution_id: ToolExecutionId::generate(),
            control,
            completion: ToolExecutionCompletion::new(rx),
        })
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
        })
    }
}
