//! Transaction runtime composition (Component 3 outer layer).
//!
//! WP-03: bootstrap, registries, state, and startup cleanup.
//! Admission / actors land in later work packages.

mod bootstrap;
mod capacity;
mod channel_registry;
mod error;
mod fake_support;
mod host_tools;
mod mcp_shell;
mod runtime;
mod state;

pub use bootstrap::{RuntimeBootstrap, RuntimeConfig};
pub use capacity::CapacityManagers;
pub use channel_registry::{ChannelBinding, ChannelRegistry};
pub use error::StartupError;
pub use fake_support::{EmptyBytesEncoder, RejectEncoder};
pub use host_tools::HostToolRegistry;
pub use mcp_shell::McpListenerShell;
pub use runtime::{DefaultTransactionRuntime, LiveChannel, Startup};
pub use state::RuntimeState;
