//! Runtime bootstrap inputs (Transaction Runtime v2).

use super::channel_registry::ChannelRegistry;
use super::host_tools::HostToolRegistry;
use monoloop_contracts::TransactionLimits;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// Test/prod gate that defers the supervisor's `Stopped` transition until
/// [`StoppedGate::release`] (v2 §22.5 TimedOut determinism).
///
/// Uses [`watch`] so a release that races ahead of the waiter is not lost
/// (unlike `Notify`, which can miss a wakeup between check and await).
#[derive(Debug)]
pub struct StoppedGate {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
}

impl Default for StoppedGate {
    fn default() -> Self {
        let (tx, rx) = watch::channel(false);
        Self { tx, rx }
    }
}

impl StoppedGate {
    /// New unreleased gate.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow the supervisor to enter `Stopped`.
    pub fn release(&self) {
        let _ = self.tx.send(true);
    }

    /// Wait until [`Self::release`] has been called.
    pub async fn wait_released(&self) {
        let mut rx = self.rx.clone();
        // `wait_for` observes the current value first — no lost-wakeup race.
        let _ = rx.wait_for(|released| *released).await;
    }
}

/// Test-only gate that pauses supervisor drain of the start queue (D-040).
///
/// While held, `Start` commands remain queued so admission can observe
/// start-queue-full rollback without the supervisor racing to drain.
#[derive(Debug, Default)]
pub struct StartHoldGate {
    held: AtomicBool,
}

impl StartHoldGate {
    /// New gate, initially not holding (start drain enabled).
    pub fn new() -> Self {
        Self {
            held: AtomicBool::new(false),
        }
    }

    /// Pause start-queue drain.
    pub fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
    }

    /// Resume start-queue drain.
    pub fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
    }

    /// Whether start drain is currently paused.
    pub fn is_held(&self) -> bool {
        self.held.load(Ordering::SeqCst)
    }
}

/// Test-only gate that pauses supervisor drain of the control queue (§23).
///
/// While held, Cancel / ForceTerminate / BeginShutdown remain queued so
/// `TransactionLimits.max_actor_commands` plus-one can observe
/// `ControlCapacityExceeded` without the preferential control drain racing.
#[derive(Debug, Default)]
pub struct ControlHoldGate {
    held: AtomicBool,
}

impl ControlHoldGate {
    /// New gate, initially not holding (control drain enabled).
    pub fn new() -> Self {
        Self {
            held: AtomicBool::new(false),
        }
    }

    /// Pause control-queue drain.
    pub fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
    }

    /// Resume control-queue drain.
    pub fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
    }

    /// Whether control drain is currently paused.
    pub fn is_held(&self) -> bool {
        self.held.load(Ordering::SeqCst)
    }
}

/// Test-only gate that pauses the Finalizer between Seal and completion send (§22.2).
///
/// Proves shutdown / hard-grace cannot drop the ledger row or the one completion
/// attempt while Seal has already run.
#[derive(Debug)]
pub struct FinalizerHoldGate {
    released: AtomicBool,
    notify: tokio::sync::Notify,
}

impl Default for FinalizerHoldGate {
    fn default() -> Self {
        Self {
            released: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }
}

impl FinalizerHoldGate {
    /// New gate; Finalizer blocks after Seal until [`Self::release`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Allow Finalizer to publish completion.
    pub fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Wait until release (used by Finalizer).
    pub async fn wait_released(&self) {
        loop {
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            // Subscribe before re-check to avoid lost wakeup.
            let notified = self.notify.notified();
            if self.released.load(Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }
}

/// Test-only inject: TaskSupervisor-owned JoinOnly-style work (§22.4 / Law 23 / M5.4).
///
/// Registers a `RuntimeService` that parks the worker thread until
/// [`JoinOnlySpillInject::release`] (abort cannot join a non-awaiting park —
/// same shape as the §22.3 sacrificial). Proves `wait_stopped` stays Quiescing
/// with `owned_tasks > 0`, then reaches Stopped after release.
/// Production leaves [`RuntimeConfig::inject_join_only_spill`] as `None`.
///
/// Name retains “Spill” for API stability; ownership is TaskSupervisor, not
/// [`crate::transaction::dispatcher::OrphanToolPermitSet`].
#[derive(Debug)]
pub struct JoinOnlySpillInject {
    entered: AtomicBool,
    released: AtomicBool,
    parked_thread: std::sync::Mutex<Option<std::thread::Thread>>,
}

impl Default for JoinOnlySpillInject {
    fn default() -> Self {
        Self::new()
    }
}

impl JoinOnlySpillInject {
    /// New inject; JoinOnly wait starts blocked until [`Self::release`].
    pub fn new() -> Self {
        Self {
            entered: AtomicBool::new(false),
            released: AtomicBool::new(false),
            parked_thread: std::sync::Mutex::new(None),
        }
    }

    /// True once the supervised task has entered its park loop.
    pub fn is_entered(&self) -> bool {
        self.entered.load(Ordering::SeqCst)
    }

    pub(crate) fn is_released(&self) -> bool {
        self.released.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_entered(&self) {
        self.entered.store(true, Ordering::SeqCst);
    }

    pub(crate) fn store_parked_thread(&self, thread: std::thread::Thread) {
        *self.parked_thread.lock().unwrap_or_else(|e| e.into_inner()) = Some(thread);
    }

    /// Allow the supervised JoinOnly task to finish (unblocks Stopped).
    pub fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        if let Some(thread) = self
            .parked_thread
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            thread.unpark();
        }
    }
}

/// Runtime-wide configuration validated at startup.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Transaction / event / callback bounds.
    pub transaction_limits: TransactionLimits,
    /// When true, bind a loopback MCP gateway as TaskSupervisor RuntimeService.
    pub enable_mcp_listener: bool,
    /// Maximum time to wait for graceful drain during shutdown when not specified.
    pub default_shutdown_deadline: Duration,
    /// When `Some`, supervisor defers drain-complete until the gate is released.
    /// Production leaves this `None`; §22.5 TimedOut proofs set it.
    pub block_stopped: Option<Arc<StoppedGate>>,
    /// When `Some`, supervisor skips draining `Start` while the gate is held.
    /// Production leaves this `None`; D-040 parked-Start proofs set it.
    pub hold_start: Option<Arc<StartHoldGate>>,
    /// When `Some`, supervisor skips draining control while the gate is held.
    /// Production leaves this `None`; §23 `max_actor_commands` proofs set it.
    pub hold_control: Option<Arc<ControlHoldGate>>,
    /// Override start-queue capacity (tests). `None` ⇒ `max_active_transactions`.
    /// Use a value smaller than reservation capacity to prove start-full rollback
    /// while the reservation pool still has headroom (D-040 / §22.1).
    pub start_queue_capacity: Option<usize>,
    /// When `Some`, Finalizer waits after Seal before completion send (§22.2).
    /// Production leaves this `None`.
    pub hold_finalizer_after_seal: Option<Arc<FinalizerHoldGate>>,
    /// When `Some`, the executor OS thread waits here after supervisor drain and
    /// before `shutdown_timeout` (D-049). Production leaves this `None`.
    pub hold_executor_teardown: Option<Arc<StoppedGate>>,
    /// When `Some`, supervisor registers a never-awaiting `RuntimeService` that
    /// stores `true` on this flag immediately before parking (§22.3 sacrificial).
    /// Production leaves this `None`.
    pub inject_non_yielding_service: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// When `Some`, supervisor registers TaskSupervisor-owned JoinOnly-style
    /// work at start (Stopped-vs-owned-task proof). Production leaves this `None`.
    pub inject_join_only_spill: Option<Arc<JoinOnlySpillInject>>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            transaction_limits: TransactionLimits::default(),
            // Default off; hosts that need MCP set true at bootstrap.
            enable_mcp_listener: false,
            default_shutdown_deadline: Duration::from_secs(30),
            block_stopped: None,
            hold_start: None,
            hold_control: None,
            start_queue_capacity: None,
            hold_finalizer_after_seal: None,
            hold_executor_teardown: None,
            inject_non_yielding_service: None,
            inject_join_only_spill: None,
        }
    }
}

impl RuntimeConfig {
    /// Validate non-zero and consistent bounds.
    pub fn validate(&self) -> Result<(), super::StartupError> {
        self.transaction_limits.validate().map_err(|e| match e {
            monoloop_contracts::LimitsError::ZeroCapacity(f) => {
                super::StartupError::InvalidConfig(f)
            }
            monoloop_contracts::LimitsError::Inconsistent(_) => {
                super::StartupError::InvalidConfig("inconsistent transaction limits")
            }
        })?;
        if self.default_shutdown_deadline.is_zero() {
            return Err(super::StartupError::InvalidConfig(
                "default_shutdown_deadline",
            ));
        }
        // Reject durations that cannot form an Instant absolute deadline
        // (`Instant + Duration` would panic for Duration::MAX-class values).
        const MAX_TX_DEADLINE: std::time::Duration =
            std::time::Duration::from_secs(365 * 24 * 3600);
        if self.transaction_limits.transaction_deadline > MAX_TX_DEADLINE
            || std::time::Instant::now()
                .checked_add(self.transaction_limits.transaction_deadline)
                .is_none()
        {
            return Err(super::StartupError::InvalidConfig(
                "transaction_deadline exceeds Instant-representable bound",
            ));
        }
        if let Some(cap) = self.start_queue_capacity {
            if cap == 0 {
                return Err(super::StartupError::InvalidConfig(
                    "start_queue_capacity must be nonzero when set",
                ));
            }
        }
        Ok(())
    }
}

/// Production bootstrap for [`super::lifecycle::StartedRuntime::start`].
///
/// The runtime constructs and owns its Tokio executor (v2 §7.2). There is no
/// external `Handle` on this struct.
pub struct RuntimeBootstrap {
    /// Limits and feature flags.
    pub config: RuntimeConfig,
    /// Immutable Channel bindings (factories realized at start).
    pub channels: ChannelRegistry,
    /// Immutable host tool shell (empty allowed).
    pub tools: HostToolRegistry,
}
