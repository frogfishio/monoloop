//! Startup errors (typed, safe).

use monoloop_connector::ConnectorBuildError;
use monoloop_contracts::ChannelCapabilityError;
use thiserror::Error;

/// Failure starting the transaction runtime (no partially started runtime is exposed).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum StartupError {
    /// Runtime / transaction limits invalid.
    #[error("invalid runtime configuration: {0}")]
    InvalidConfig(&'static str),
    /// Channel registry validation failed.
    #[error("channel registry invalid: {0}")]
    ChannelRegistry(&'static str),
    /// Channel capability matrix violation.
    #[error("channel capability invalid: {0}")]
    ChannelCapability(#[from] ChannelCapabilityError),
    /// Host tool registry validation failed.
    #[error("host tool registry invalid: {0}")]
    ToolRegistry(&'static str),
    /// Connector instance construction failed.
    #[error("connector build failed: {0}")]
    ConnectorBuild(#[from] ConnectorBuildError),
    /// Connector instance session adapter mismatch with Channel kind.
    #[error("connector session adapter mismatch: {0}")]
    SessionAdapterMismatch(&'static str),
    /// MCP loopback listener failed to bind.
    #[error("MCP listener bind failed")]
    McpBindFailed,
    /// Tokio executor unavailable.
    #[error("executor unavailable")]
    ExecutorUnavailable,
    /// Internal invariant during startup.
    #[error("startup invariant failed: {0}")]
    InvariantFailed(&'static str),
}
