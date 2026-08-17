//! Runtime bootstrap inputs.

use super::channel_registry::ChannelRegistry;
use super::host_tools::HostToolRegistry;
use monoloop_contracts::TransactionLimits;
use std::time::Duration;
use tokio::runtime::Handle;

/// Runtime-wide configuration validated at startup.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Transaction / event / callback bounds.
    pub transaction_limits: TransactionLimits,
    /// When true, bind a loopback MCP listener shell (WP-07 fills protocol).
    pub enable_mcp_listener: bool,
    /// Maximum time to wait for graceful drain during shutdown when not specified.
    pub default_shutdown_deadline: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            transaction_limits: TransactionLimits::default(),
            enable_mcp_listener: true,
            default_shutdown_deadline: Duration::from_secs(30),
        }
    }
}

impl RuntimeConfig {
    /// Validate non-zero and consistent bounds.
    pub fn validate(&self) -> Result<(), super::StartupError> {
        self.transaction_limits
            .validate()
            .map_err(|e| match e {
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

/// Only construction path for [`super::DefaultTransactionRuntime`].
pub struct RuntimeBootstrap {
    /// Limits and feature flags.
    pub config: RuntimeConfig,
    /// Immutable Channel bindings (factories not yet realized).
    pub channels: ChannelRegistry,
    /// Immutable host tool shell (empty allowed).
    pub tools: HostToolRegistry,
    /// Tokio multi-thread handle used for owned tasks.
    pub executor: Handle,
}
