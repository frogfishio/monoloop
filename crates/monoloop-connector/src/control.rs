//! Out-of-band connection control.

use monoloop_contracts::ConnectorError;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Why cooperative cancellation was requested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CancellationReason {
    /// Caller requested cancel.
    CallerRequested,
    /// Run / higher-level shutdown.
    RunShutdown,
    /// Deadline exceeded upstream.
    DeadlineExceeded,
    /// Other bounded reason label.
    Other(String),
}

/// Why forced termination was requested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminationReason {
    /// Cancel grace period exhausted.
    CancelEscalation,
    /// Caller forced close.
    CallerForced,
    /// Process/supervisor teardown.
    SupervisorTeardown,
    /// Other bounded reason label.
    Other(String),
}

/// Immediate disposition of a control call (not the terminal outcome).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlDisposition {
    /// Signal recorded by the connection owner.
    Accepted,
    /// Same signal already recorded.
    AlreadyRequested,
    /// Connection already terminal.
    AlreadyTerminal,
    /// Control path not available.
    ControlUnavailable,
}

/// Shared connection control state (cancel / terminate / terminal race).
#[derive(Debug)]
pub struct ControlState {
    terminal: AtomicBool,
    cancel_requested: AtomicBool,
    terminate_requested: AtomicBool,
    /// 0 = none, 1 = cancel wins, 2 = terminate wins (terminal kind preference).
    preferred_end: AtomicU8,
    notify: Notify,
}

impl Default for ControlState {
    fn default() -> Self {
        Self {
            terminal: AtomicBool::new(false),
            cancel_requested: AtomicBool::new(false),
            terminate_requested: AtomicBool::new(false),
            preferred_end: AtomicU8::new(0),
            notify: Notify::new(),
        }
    }
}

impl ControlState {
    /// Create shared control state.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Whether a terminal outcome has been published.
    pub fn is_terminal(&self) -> bool {
        self.terminal.load(Ordering::SeqCst)
    }

    /// Mark terminal and wake waiters.
    pub fn mark_terminal(&self) {
        self.terminal.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Cancel requested?
    pub fn cancel_requested(&self) -> bool {
        self.cancel_requested.load(Ordering::SeqCst)
    }

    /// Terminate requested?
    pub fn terminate_requested(&self) -> bool {
        self.terminate_requested.load(Ordering::SeqCst)
    }

    pub(crate) fn preferred_end_kind(&self) -> Option<PreferredEnd> {
        match self.preferred_end.load(Ordering::SeqCst) {
            1 => Some(PreferredEnd::Cancelled),
            2 => Some(PreferredEnd::Terminated),
            _ => None,
        }
    }

    /// Notify handle for interrupt waits.
    pub fn notify(&self) -> &Notify {
        &self.notify
    }

    pub(crate) fn request_cancel(&self) -> ControlDisposition {
        if self.is_terminal() {
            return ControlDisposition::AlreadyTerminal;
        }
        if self.cancel_requested.swap(true, Ordering::SeqCst) {
            return ControlDisposition::AlreadyRequested;
        }
        // Terminate already preferred wins over later cancel.
        let _ = self.preferred_end.compare_exchange(
            0,
            1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        self.notify.notify_waiters();
        ControlDisposition::Accepted
    }

    pub(crate) fn request_terminate(&self) -> ControlDisposition {
        if self.is_terminal() {
            return ControlDisposition::AlreadyTerminal;
        }
        if self.terminate_requested.swap(true, Ordering::SeqCst) {
            return ControlDisposition::AlreadyRequested;
        }
        self.preferred_end.store(2, Ordering::SeqCst);
        self.notify.notify_waiters();
        ControlDisposition::Accepted
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PreferredEnd {
    Cancelled,
    Terminated,
}

/// Cloneable connection-scoped control handle (usable while open is pending).
#[derive(Clone, Debug)]
pub struct ConnectionControlHandle {
    state: Arc<ControlState>,
}

impl ConnectionControlHandle {
    /// Wrap shared control state.
    pub fn new(state: Arc<ControlState>) -> Self {
        Self { state }
    }

    /// Borrow shared state.
    pub fn state(&self) -> &Arc<ControlState> {
        &self.state
    }

    /// Request cooperative cancellation.
    pub fn cancel(&self, _reason: CancellationReason) -> ControlDisposition {
        self.state.request_cancel()
    }

    /// Request forced local transport closure.
    pub fn terminate(&self, _reason: TerminationReason) -> ControlDisposition {
        self.state.request_terminate()
    }

    /// Wait until cancel, terminate, or terminal is signalled.
    pub async fn interrupted(&self) {
        loop {
            if self.state.cancel_requested()
                || self.state.terminate_requested()
                || self.state.is_terminal()
            {
                return;
            }
            self.state.notify.notified().await;
        }
    }

    /// Map control preference to a connector error if interrupted.
    pub fn interrupt_error(&self) -> Option<ConnectorError> {
        if self.state.terminate_requested() {
            Some(ConnectorError::terminated())
        } else if self.state.cancel_requested() {
            Some(ConnectorError::cancelled())
        } else {
            None
        }
    }
}
