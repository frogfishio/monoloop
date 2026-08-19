//! Runtime lifecycle states (Transaction Runtime v2).

/// Lifecycle of [`super::lifecycle::RuntimeOwner`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RuntimeState {
    /// Startup in progress (runtime not yet exposed).
    Starting,
    /// Accepting `submit`.
    Accepting,
    /// Shutdown in progress; new submissions rejected. May remain here after a
    /// timed-out `wait_stopped` while ownership continues (v2: not false Stopped).
    Quiescing,
    /// Fully stopped; all owned work joined/reaped.
    Stopped,
}
