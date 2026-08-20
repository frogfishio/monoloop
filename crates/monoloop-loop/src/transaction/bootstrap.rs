//! Runtime bootstrap inputs (Transaction Runtime v2).

use super::channel_registry::ChannelRegistry;
use super::host_tools::HostToolRegistry;
use monoloop_contracts::TransactionLimits;
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

/// Runtime-wide configuration validated at startup.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Transaction / event / callback bounds.
    pub transaction_limits: TransactionLimits,
    /// When true, bind a loopback MCP listener (deferred until M5).
    pub enable_mcp_listener: bool,
    /// Maximum time to wait for graceful drain during shutdown when not specified.
    pub default_shutdown_deadline: Duration,
    /// When `Some`, supervisor defers `Stopped` until the gate is released.
    /// Production leaves this `None`; §22.5 TimedOut proofs set it.
    pub block_stopped: Option<Arc<StoppedGate>>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            transaction_limits: TransactionLimits::default(),
            // MCP deferred until M5 — default off so start succeeds.
            enable_mcp_listener: false,
            default_shutdown_deadline: Duration::from_secs(30),
            block_stopped: None,
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
