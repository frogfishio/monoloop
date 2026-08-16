//! Claude Code connector errors (bounded, no secrets/bodies).

use monoloop_contracts::{ConnectorError, ConnectorErrorKind};

/// Claude Code CLI connector failure.
#[derive(Debug, thiserror::Error)]
pub enum ClaudeConnectorError {
    /// Process spawn / I/O.
    #[error("claude process: {0}")]
    Process(String),
    /// Timeout waiting for headless completion.
    #[error("claude timeout: {0}")]
    Timeout(String),
    /// Session / run failure.
    #[error("claude run: {0}")]
    Run(String),
}

impl ClaudeConnectorError {
    /// Map into product connector error vocabulary.
    pub fn into_connector_error(self) -> ConnectorError {
        let (kind, msg) = match &self {
            Self::Process(m) => (ConnectorErrorKind::ConnectionFailed, m.clone()),
            Self::Timeout(m) => (ConnectorErrorKind::DeadlineExceeded, m.clone()),
            Self::Run(m) => (ConnectorErrorKind::ReadFailed, m.clone()),
        };
        ConnectorError::new(kind, msg)
    }

    pub(crate) fn process(msg: impl Into<String>) -> Self {
        Self::Process(msg.into())
    }

    pub(crate) fn timeout(msg: impl Into<String>) -> Self {
        Self::Timeout(msg.into())
    }

    pub(crate) fn run(msg: impl Into<String>) -> Self {
        Self::Run(msg.into())
    }
}
