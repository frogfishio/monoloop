//! Antigravity connector errors (bounded, no secrets/bodies).

use monoloop_contracts::{ConnectorError, ConnectorErrorKind};

/// Antigravity / agy connector failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgyConnectorError {
    /// Closed kind.
    pub kind: ConnectorErrorKind,
    /// Safe message.
    pub message: String,
}

impl AgyConnectorError {
    /// Configuration invalid.
    pub fn configuration(msg: impl Into<String>) -> Self {
        Self {
            kind: ConnectorErrorKind::ConfigurationInvalid,
            message: msg.into(),
        }
    }

    /// Process spawn / I/O failure.
    pub fn connection(msg: impl Into<String>) -> Self {
        Self {
            kind: ConnectorErrorKind::ConnectionFailed,
            message: msg.into(),
        }
    }

    /// Protocol / dialect framing failure.
    pub fn protocol(msg: impl Into<String>) -> Self {
        Self {
            kind: ConnectorErrorKind::ProtocolFailed,
            message: msg.into(),
        }
    }

    /// Deadline exceeded.
    pub fn deadline(msg: impl Into<String>) -> Self {
        Self {
            kind: ConnectorErrorKind::DeadlineExceeded,
            message: msg.into(),
        }
    }

    /// Session create/load/prompt failure.
    pub fn session(msg: impl Into<String>) -> Self {
        Self {
            kind: ConnectorErrorKind::SessionFailed,
            message: msg.into(),
        }
    }

    /// Cancelled.
    pub fn cancelled() -> Self {
        Self {
            kind: ConnectorErrorKind::Cancelled,
            message: "agy session cancelled".into(),
        }
    }

    /// Map to contracts ConnectorError.
    pub fn into_connector_error(self) -> ConnectorError {
        ConnectorError::new(self.kind, self.message)
    }
}

impl From<AgyConnectorError> for ConnectorError {
    fn from(e: AgyConnectorError) -> Self {
        e.into_connector_error()
    }
}

impl std::fmt::Display for AgyConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for AgyConnectorError {}
