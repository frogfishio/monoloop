//! Linked tool handlers, execution handles, and cancellation controls.

use monoloop_contracts::{
    ToolCall, ToolCallContext, ToolCompletion, ToolExecutionId, ToolStartError,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, Notify};

/// RAII decrement for [`ShutdownSnapshot::owned_processes`] (§18.2 honesty).
#[derive(Debug)]
pub(crate) struct OwnedProcessLease {
    counter: Arc<AtomicU32>,
}

impl OwnedProcessLease {
    fn acquire(counter: Arc<AtomicU32>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for OwnedProcessLease {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

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
    /// Default **false** (fail-closed). For process-isolated tool classes,
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

    /// Structural AbortableAtYield ownership (V2 §14.2 / D-050).
    ///
    /// Default **false**. Only handlers registered via
    /// [`super::host_tools::RegisteredTool::try_new_abortable`] may return true;
    /// they yield CancelOnly + an unspawned `drive` future the runtime polls.
    /// Capability booleans alone are insufficient.
    fn runtime_owns_abortable_drive(&self) -> bool {
        false
    }
}

mod abortable_seal {
    /// Seals [`super::AbortableAtYieldHandler`] to crate-defined factories.
    pub trait Sealed {}
}

/// Structural AbortableAtYield factory marker (V2 §14.2 / D-050).
///
/// Only crate-owned handlers that produce CancelOnly + inline `drive` implement
/// this. Custom `dyn ToolHandler` values cannot self-assert via `supports_abort`.
pub trait AbortableAtYieldHandler: ToolHandler + abortable_seal::Sealed {}

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
    /// Inline AbortableAtYield body driven on the caller's task (M5.4 — no ambient spawn).
    ///
    /// `kill` cancels [`ToolExecutionControl`]; dropping the drive future stops
    /// work at the next `.await`. No separate JoinHandle to park.
    CancelOnly { control: ToolExecutionControl },
    /// OS child process — real kill boundary (V2 §14.3).
    Process {
        child: Arc<Mutex<Option<tokio::process::Child>>>,
        /// Live until the child is observed reaped (or spill/Drop releases).
        owned_slot: Mutex<Option<OwnedProcessLease>>,
    },
}

impl ToolKillHandle {
    /// AbortableAtYield without a nested Tokio task (M5.4).
    ///
    /// Caller drives [`LinkedToolExecutionHandle::drive`] on the supervised
    /// dispatch task; `kill` requests cooperative cancel via `control`.
    pub fn cancel_only(control: ToolExecutionControl) -> Self {
        Self {
            inner: Arc::new(KillInner::CancelOnly { control }),
        }
    }

    /// Own an OS [`tokio::process::Child`] (D-043 / M5.4 / D-048).
    ///
    /// Wait/poll runs on [`LinkedToolExecutionHandle::drive`] (no ambient
    /// `spawn_blocking`). `child` is shared so kill and the drive loop observe
    /// the same process.
    pub(crate) fn from_process(child: Arc<Mutex<Option<tokio::process::Child>>>) -> Self {
        Self {
            inner: Arc::new(KillInner::Process {
                child,
                owned_slot: Mutex::new(None),
            }),
        }
    }

    /// Register this ProcessIsolated child in the runtime `owned_processes` count.
    ///
    /// Idempotent. Call from the dispatcher after `start` when a shared counter exists.
    pub fn register_owned_process(&self, counter: Arc<AtomicU32>) {
        let KillInner::Process { owned_slot, .. } = &*self.inner else {
            return;
        };
        let mut slot = owned_slot.lock().unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            *slot = Some(OwnedProcessLease::acquire(counter));
        }
    }

    /// Release the owned-process lease once the child is observed reaped.
    pub fn note_process_reaped(&self) {
        let KillInner::Process { owned_slot, .. } = &*self.inner else {
            return;
        };
        let _ = owned_slot.lock().unwrap_or_else(|e| e.into_inner()).take();
    }

    /// Take the lease for spill parking (keeps `owned_processes` honest across Drop).
    #[allow(dead_code)] // retained for D-048 registry / spill compatibility
    pub(crate) fn take_process_lease(&self) -> Option<OwnedProcessLease> {
        let KillInner::Process { owned_slot, .. } = &*self.inner else {
            return None;
        };
        owned_slot.lock().unwrap_or_else(|e| e.into_inner()).take()
    }

    /// Request cancel (CancelOnly) or OS-kill the child (ProcessIsolated). Idempotent.
    pub fn kill(&self) {
        match &*self.inner {
            KillInner::CancelOnly { control } => control.cancel(),
            KillInner::Process { child, .. } => {
                if let Some(c) = child.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
                    let _ = c.start_kill();
                }
            }
        }
    }

    /// Await worker teardown. On timeout, ProcessIsolated leaves the child owned
    /// so capacity stays held; caller must keep joining or park an orphan permit.
    pub async fn join_timeout(&self, budget: std::time::Duration) -> Result<(), ()> {
        match &*self.inner {
            KillInner::CancelOnly { .. } => {
                // Inline drive: caller drops/polls the drive future; no join to await.
                Ok(())
            }
            KillInner::Process { child, owned_slot } => {
                // Drive-owned wait: poll try_wait until exit or budget (no mutex across await).
                let deadline = std::time::Instant::now() + budget;
                loop {
                    let done = {
                        let mut guard = child.lock().unwrap_or_else(|e| e.into_inner());
                        match guard.as_mut() {
                            Some(c) => match c.try_wait() {
                                Ok(Some(_)) => {
                                    let _ = guard.take();
                                    true
                                }
                                Ok(None) => false,
                                Err(_) => true,
                            },
                            None => true,
                        }
                    };
                    if done {
                        let _ = owned_slot.lock().unwrap_or_else(|e| e.into_inner()).take();
                        return Ok(());
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }
        }
    }

    /// Whether unfinished ProcessIsolated work is still owned (capacity hold).
    pub fn has_join(&self) -> bool {
        match &*self.inner {
            KillInner::Process { child, owned_slot } => {
                // Drive-owned wait: capacity stays held until the child is observed exited.
                if Self::process_still_alive(child) {
                    true
                } else {
                    let _ = owned_slot.lock().unwrap_or_else(|e| e.into_inner()).take();
                    false
                }
            }
            KillInner::CancelOnly { .. } => false,
        }
    }

    fn process_still_alive(child: &Mutex<Option<tokio::process::Child>>) -> bool {
        let mut guard = child.lock().unwrap_or_else(|e| e.into_inner());
        match guard.as_mut() {
            Some(c) => match c.try_wait() {
                Ok(None) => true,
                Ok(Some(_)) => {
                    // Reaped — drop the Child so Drop does not wait again.
                    let _ = guard.take();
                    false
                }
                Err(_) => true, // fail-closed: treat as still owned
            },
            None => false,
        }
    }

    /// True when this handle owns an OS process (ProcessIsolated).
    pub fn is_process_isolated(&self) -> bool {
        matches!(&*self.inner, KillInner::Process { .. })
    }

    /// OS PID of a live ProcessIsolated child, if still owned.
    ///
    /// Used by sacrificial proofs (D-048) to assert kill/reap without ambient heuristics.
    pub fn os_pid(&self) -> Option<u32> {
        match &*self.inner {
            KillInner::Process { child, .. } => {
                let guard = child.lock().unwrap_or_else(|e| e.into_inner());
                guard.as_ref().and_then(|c| c.id())
            }
            KillInner::CancelOnly { .. } => None,
        }
    }

    /// True when the body is driven inline on the caller task (no nested JoinHandle).
    pub fn is_cancel_only(&self) -> bool {
        matches!(&*self.inner, KillInner::CancelOnly { .. })
    }
}

/// Handle returned from [`ToolHandler::start`].
pub struct LinkedToolExecutionHandle {
    /// Stable execution id for this start.
    pub execution_id: ToolExecutionId,
    /// Cancellation control.
    pub control: ToolExecutionControl,
    /// Exactly-once completion.
    pub completion: ToolExecutionCompletion,
    /// Optional kill handle for escalate-after-grace (D-024).
    pub kill: Option<ToolKillHandle>,
    /// When `Some`, the dispatcher MUST poll this on the current task (M5.4).
    /// Completes by sending on [`Self::completion`]. No ambient `tokio::spawn`.
    pub drive: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl std::fmt::Debug for LinkedToolExecutionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkedToolExecutionHandle")
            .field("execution_id", &self.execution_id)
            .field("control", &self.control)
            .field("completion", &self.completion)
            .field("kill", &self.kill)
            .field("drive", &self.drive.as_ref().map(|_| "<drive>"))
            .finish()
    }
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
            drive: None,
        })
    }
}

type BoxFut = Pin<Box<dyn Future<Output = ToolCompletion> + Send>>;

/// Handler that runs an async body with abortable cancellation.
///
/// M5.4: body is returned as [`LinkedToolExecutionHandle::drive`] and polled on
/// the caller's task (the supervised ToolWorker dispatch path). No ambient
/// `tokio::spawn`. Cancel via [`ToolKillHandle::cancel_only`].
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
        // Drive inline on the dispatcher/ToolWorker task — Law 23 / M5.4.
        let drive = Box::pin(async move {
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
        let kill = ToolKillHandle::cancel_only(control.clone());
        Ok(LinkedToolExecutionHandle {
            execution_id: ToolExecutionId::generate(),
            control,
            completion: ToolExecutionCompletion::new(rx),
            kill: Some(kill),
            drive: Some(drive),
        })
    }

    fn supports_abort(&self) -> bool {
        true
    }

    fn runtime_owns_abortable_drive(&self) -> bool {
        true
    }
}

impl<F> abortable_seal::Sealed for AsyncToolHandler<F> where
    F: Fn(ToolCall, ToolCallContext, ToolExecutionControl) -> BoxFut + Send + Sync
{
}

impl<F> AbortableAtYieldHandler for AsyncToolHandler<F> where
    F: Fn(ToolCall, ToolCallContext, ToolExecutionControl) -> BoxFut + Send + Sync
{
}

/// Stubborn in-process worker for AbortableAtYield / legacy D-024 fixtures.
///
/// **Not** ProcessIsolated: termination is cancel + dropping the inline drive
/// (abort-at-yield of the caller task). [`Self::os_process_isolated`] is false.
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
        let control_body = control.clone();
        let fut = (self.f)(call, context);
        let (tx, rx) = oneshot::channel();
        // Inline drive: ignores cooperative cancel unless the body polls it;
        // deadline path drops this future (abort-at-yield of the caller task).
        let drive = Box::pin(async move {
            let _ = control_body;
            let result = fut.await;
            let _ = tx.send(result);
        });
        let kill = ToolKillHandle::cancel_only(control.clone());
        Ok(LinkedToolExecutionHandle {
            execution_id: ToolExecutionId::generate(),
            control,
            completion: ToolExecutionCompletion::new(rx),
            kill: Some(kill),
            drive: Some(drive),
        })
    }

    fn supports_abort(&self) -> bool {
        // Abort-at-yield of the caller/dispatch task — not OS isolation.
        true
    }

    fn supports_isolated_kill(&self) -> bool {
        // D-043: Tokio abort must not satisfy ProcessIsolated registration.
        false
    }

    fn runtime_owns_abortable_drive(&self) -> bool {
        true
    }
}

impl<F> abortable_seal::Sealed for IsolatedKillableToolHandler<F> where
    F: Fn(ToolCall, ToolCallContext) -> BoxFut + Send + Sync
{
}

impl<F> AbortableAtYieldHandler for IsolatedKillableToolHandler<F> where
    F: Fn(ToolCall, ToolCallContext) -> BoxFut + Send + Sync
{
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
            drive: None,
        })
    }
}
