//! Grok connector errors (safe diagnostics; no secrets).

use monoloop_contracts::{ConnectorError, ConnectorErrorKind};
use std::fmt;

/// Profile-level error wrapper mapped onto [`ConnectorError`] families.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrokConnectorError {
    /// Underlying closed error.
    pub inner: ConnectorError,
}

impl GrokConnectorError {
    /// Wrap a connector error.
    pub fn from_connector(inner: ConnectorError) -> Self {
        Self { inner }
    }

    /// Configuration invalid.
    pub fn configuration(message: impl Into<String>) -> Self {
        Self {
            inner: ConnectorError::configuration_invalid(message),
        }
    }

    /// Connection failed.
    pub fn connection(message: impl Into<String>) -> Self {
        Self {
            inner: ConnectorError::connection_failed(message),
        }
    }

    /// Protocol failed.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self {
            inner: ConnectorError::protocol(message),
        }
    }

    /// Session failed.
    pub fn session(message: impl Into<String>) -> Self {
        Self {
            inner: ConnectorError::session_failed(message),
        }
    }

    /// Credential unavailable (no secret material).
    pub fn credential_unavailable() -> Self {
        Self {
            inner: ConnectorError::new(
                ConnectorErrorKind::CredentialUnavailable,
                "credential reference could not be resolved",
            ),
        }
    }

    /// Cancelled.
    pub fn cancelled() -> Self {
        Self {
            inner: ConnectorError::cancelled(),
        }
    }

    /// Resource / bound.
    pub fn resource(message: impl Into<String>) -> Self {
        Self {
            inner: ConnectorError::resource(message),
        }
    }

    /// Into generic connector error.
    pub fn into_connector(self) -> ConnectorError {
        self.inner
    }
}

impl fmt::Display for GrokConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl std::error::Error for GrokConnectorError {}

impl From<ConnectorError> for GrokConnectorError {
    fn from(inner: ConnectorError) -> Self {
        Self { inner }
    }
}
