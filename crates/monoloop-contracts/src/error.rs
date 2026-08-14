//! Closed connector error families (safe diagnostics only).

use thiserror::Error;

/// High-level connector error classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectorErrorKind {
    /// Invalid open/config parameters.
    ConfigurationInvalid,
    /// Required dialect unavailable or ambiguous.
    DialectUnavailable,
    /// Credential reference could not be resolved (no secret material in error).
    CredentialUnavailable,
    /// Transport open/connect failed.
    ConnectionFailed,
    /// Write path failed.
    WriteFailed,
    /// Read path failed.
    ReadFailed,
    /// Remote closed the connection.
    RemoteClosed,
    /// Deadline exceeded.
    DeadlineExceeded,
    /// Cooperative cancellation.
    Cancelled,
    /// Forced termination.
    Terminated,
    /// Local resource (queue/task) failure or limit.
    LocalResourceFailed,
    /// Internal invariant broken.
    InvariantViolation,
    /// Session create/load/routing failure (profile-specific).
    SessionFailed,
    /// Protocol/JSON-RPC framing failure (bounded classification).
    ProtocolFailed,
}

/// Connector error with safe, bounded diagnostics.
///
/// Never contains credentials, raw bodies, prompts, or unrestricted endpoints.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{kind:?}: {message}")]
pub struct ConnectorError {
    /// Closed error family.
    pub kind: ConnectorErrorKind,
    /// Bounded human-safe message (no secrets).
    pub message: String,
    /// Optional connection correlation (may be absent during open failures).
    pub connection_id: Option<String>,
}

impl ConnectorError {
    /// Construct a typed error.
    pub fn new(kind: ConnectorErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            connection_id: None,
        }
    }

    /// Attach a connection id for correlation (not a secret).
    pub fn with_connection_id(mut self, id: impl Into<String>) -> Self {
        self.connection_id = Some(id.into());
        self
    }

    /// Configuration invalid.
    pub fn configuration_invalid(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::ConfigurationInvalid, message)
    }

    /// Cancelled.
    pub fn cancelled() -> Self {
        Self::new(ConnectorErrorKind::Cancelled, "connection cancelled")
    }

    /// Terminated.
    pub fn terminated() -> Self {
        Self::new(ConnectorErrorKind::Terminated, "connection terminated")
    }

    /// Resource / bound exceeded.
    pub fn resource(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::LocalResourceFailed, message)
    }

    /// Protocol failure.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::ProtocolFailed, message)
    }

    /// Connection failed.
    pub fn connection_failed(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::ConnectionFailed, message)
    }

    /// Session operation failed.
    pub fn session_failed(message: impl Into<String>) -> Self {
        Self::new(ConnectorErrorKind::SessionFailed, message)
    }
}
