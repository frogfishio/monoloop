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
    /// Default **false** (fail-closed). For [`ToolExecutionClass::ProcessIsolated`],
    /// registration also requires [`Self::os_process_isolated`].
    fn supports_isolated_kill(&self) -> bool {
        false
    }

    /// Structural OS process isolation boundary (V2 §14.3).
    ///
    /// Default **false**. Only handlers that own a real child process (not a
    /// Tokio task) may return true. Capability booleans alone are insufficient.
    fn os_process_isolated(&self) -> bool {
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

/// Force-stop + join for Abortable (Tokio) or ProcessIsolated (OS child) workers.
///
/// Timed waits must not drop the join on timeout (put-back) or the worker would
/// detach while capacity is released. Process kill uses OS signals (D-043).
#[derive(Clone, Debug)]
pub struct ToolKillHandle {
    inner: Arc<KillInner>,
}

#[derive(Debug)]
enum KillInner {
    /// In-process Tokio task — abort at yield only (not ProcessIsolated).
    Tokio {
        abort: AbortHandle,
        join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    },
    /// Join ownership without abort (CooperativeInProcess — §14.1 / §22.4).
    JoinOnly {
        join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    },
    /// OS child process — real kill boundary (V2 §14.3).
    Process {
        child: Arc<Mutex<Option<std::process::Child>>>,
        join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    },
}

impl ToolKillHandle {
    /// Take ownership of a Tokio worker task (abort + join).
    ///
    /// This is **not** a ProcessIsolated boundary.
    pub fn new(join: tokio::task::JoinHandle<()>) -> Self {
        Self {
            inner: Arc::new(KillInner::Tokio {
                abort: join.abort_handle(),
                join: Mutex::new(Some(join)),
            }),
        }
    }

    /// Own a cooperative worker join without hard abort (§22.4).
    ///
    /// `kill` is a no-op; cancel remains on [`ToolExecutionControl`]. Permit stays
    /// held until [`Self::join_timeout`] / vault reap observes the join.
    pub fn join_only(join: tokio::task::JoinHandle<()>) -> Self {
        Self {
            inner: Arc::new(KillInner::JoinOnly {
                join: Mutex::new(Some(join)),
            }),
        }
    }

    /// Own an OS [`std::process::Child`] plus a blocking wait task (D-043).
    ///
    /// `child` is shared with the wait task so kill and wait observe the same process.
    pub(crate) fn from_process(
        child: Arc<Mutex<Option<std::process::Child>>>,
        join: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            inner: Arc::new(KillInner::Process {
                child,
                join: Mutex::new(Some(join)),
            }),
        }
    }

    /// Abort Tokio task or OS-kill the child (idempotent).
    ///
    /// [`KillInner::JoinOnly`] is a no-op (cooperative — cancel via control only).
    pub fn kill(&self) {
        match &*self.inner {
            KillInner::Tokio { abort, .. } => abort.abort(),
            KillInner::JoinOnly { .. } => {}
            KillInner::Process { child, .. } => {
                if let Some(c) = child.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
                    let _ = c.kill();
                }
            }
        }
    }

    /// Await worker teardown. On timeout, restores the join handle so the
    /// worker is not detached; caller must keep capacity until a later join.
    pub async fn join_timeout(&self, budget: std::time::Duration) -> Result<(), ()> {
        let handle = match &*self.inner {
            KillInner::Tokio { join, .. }
            | KillInner::JoinOnly { join }
            | KillInner::Process { join, .. } => {
                join.lock().unwrap_or_else(|e| e.into_inner()).take()
            }
        };
        let Some(mut handle) = handle else {
            return Ok(()); // already joined
        };
        match tokio::time::timeout(budget, &mut handle).await {
            Ok(_) => Ok(()),
            Err(_) => {
                match &*self.inner {
                    KillInner::Tokio { join, .. }
                    | KillInner::JoinOnly { join }
                    | KillInner::Process { join, .. } => {
                        *join.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
                    }
                }
                Err(())
            }
        }
    }

    /// Take the join handle if still present (for TaskSupervisor registration).
    #[allow(dead_code)]
    pub(crate) fn take_join(&self) -> Option<tokio::task::JoinHandle<()>> {
        match &*self.inner {
            KillInner::Tokio { join, .. }
            | KillInner::JoinOnly { join }
            | KillInner::Process { join, .. } => {
                join.lock().unwrap_or_else(|e| e.into_inner()).take()
            }
        }
    }

    /// Whether a join handle is still owned.
    #[allow(dead_code)]
    pub(crate) fn has_join(&self) -> bool {
        match &*self.inner {
            KillInner::Tokio { join, .. }
            | KillInner::JoinOnly { join }
            | KillInner::Process { join, .. } => {
                join.lock().unwrap_or_else(|e| e.into_inner()).is_some()
            }
        }
    }

    /// True when this handle owns an OS process (ProcessIsolated).
    pub fn is_process_isolated(&self) -> bool {
        matches!(&*self.inner, KillInner::Process { .. })
    }

    /// True when this is cooperative join-only ownership (no hard abort).
    pub fn is_join_only(&self) -> bool {
        matches!(&*self.inner, KillInner::JoinOnly { .. })
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

/// Stubborn in-process worker for AbortableAtYield / legacy D-024 fixtures.
///
/// **Not** ProcessIsolated: kill is Tokio `abort` only (V2 §14.3 / D-043).
/// [`Self::os_process_isolated`] is false — cannot register as ProcessIsolated.
/// Prefer [`super::process_tool::ProcessIsolatedToolHandler`] for real OS kill.
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
        // Tokio abort at yield — not cooperative-only, not OS isolation.
        true
    }

    fn supports_isolated_kill(&self) -> bool {
        // D-043: Tokio abort must not satisfy ProcessIsolated registration.
        false
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
