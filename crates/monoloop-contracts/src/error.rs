//! Closed connector and interpreter error families (safe diagnostics only).

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

/// Interpreter error classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InterpreterErrorKind {
    /// Unsupported dialect.
    UnsupportedDialect,
    /// Dialect binding mismatch.
    DialectBindingMismatch,
    /// Malformed frame.
    MalformedFrame,
    /// Frame/buffer limit exceeded.
    FrameLimitExceeded,
    /// Unsupported semantic event.
    UnsupportedSemanticEvent,
    /// Malformed semantic payload.
    MalformedSemanticPayload,
    /// Sentence assembly limit.
    SentenceLimitExceeded,
    /// Structure limit.
    StructureLimitExceeded,
    /// Tool identity missing/conflict.
    ToolIdentityError,
    /// Tool limit exceeded.
    ToolLimitExceeded,
    /// Output backpressure exceeded.
    OutputBackpressureExceeded,
    /// Connector ended unexpectedly.
    ConnectorEndedUnexpectedly,
    /// Cancelled.
    Cancelled,
    /// Invariant violation.
    InvariantViolation,
    /// Configuration invalid.
    ConfigurationInvalid,
}

/// Interpreter error with safe diagnostics.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{kind:?}: {message}")]
pub struct InterpreterError {
    /// Closed error family.
    pub kind: InterpreterErrorKind,
    /// Bounded safe message.
    pub message: String,
}

impl InterpreterError {
    /// Construct a typed error.
    pub fn new(kind: InterpreterErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Unsupported dialect.
    pub fn unsupported_dialect(message: impl Into<String>) -> Self {
        Self::new(InterpreterErrorKind::UnsupportedDialect, message)
    }

    /// Malformed frame.
    pub fn malformed_frame(message: impl Into<String>) -> Self {
        Self::new(InterpreterErrorKind::MalformedFrame, message)
    }

    /// Limit exceeded.
    pub fn limit(message: impl Into<String>) -> Self {
        Self::new(InterpreterErrorKind::FrameLimitExceeded, message)
    }

    /// Cancelled.
    pub fn cancelled() -> Self {
        Self::new(InterpreterErrorKind::Cancelled, "interpretation cancelled")
    }

    /// Output backpressure.
    pub fn backpressure() -> Self {
        Self::new(
            InterpreterErrorKind::OutputBackpressureExceeded,
            "canonical output queue full",
        )
    }
}
