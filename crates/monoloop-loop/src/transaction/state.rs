//! Runtime lifecycle states.

/// Lifecycle of [`super::DefaultTransactionRuntime`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RuntimeState {
    /// Startup in progress (runtime not yet exposed).
    Starting,
    /// Accepting `submit` (admission implemented from WP-04).
    Accepting,
    /// Shutdown draining; new submissions rejected.
    Draining,
    /// Fully stopped.
    Stopped,
}
