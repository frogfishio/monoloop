//! Transaction runtime composition (Component 3 outer layer).
//!
//! WP-03: bootstrap / registries / startup.
//! WP-04: admission, events, finalization, callbacks, no-I/O actor.

mod active_registry;
mod actor;
mod admission;
mod bootstrap;
mod capacity;
mod channel_registry;
mod error;
mod events;
mod fake_support;
mod finalization;
mod host_tools;
mod mcp_shell;
mod resolved_tools;
mod runtime;
mod state;

pub use bootstrap::{RuntimeBootstrap, RuntimeConfig};
pub use capacity::CapacityManagers;
pub use channel_registry::{ChannelBinding, ChannelRegistry, LiveChannel};
pub use error::StartupError;
pub use fake_support::{EmptyBytesEncoder, RejectEncoder};
pub use finalization::{EventSequencer, FinalizationGuard};
pub use host_tools::HostToolRegistry;
pub use mcp_shell::McpListenerShell;
pub use resolved_tools::ResolvedToolSet;
pub use runtime::{DefaultTransactionRuntime, Startup};
pub use state::RuntimeState;
