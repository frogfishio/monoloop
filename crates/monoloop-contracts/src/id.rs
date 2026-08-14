//! Correlation identities. Opaque wrappers — no ambient current identity.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Local logical transport attachment identity for one connection scope.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionId(String);

impl ConnectionId {
    /// Create a connection id from an explicit caller-supplied string.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Allocate a random connection id (for tests and callers without an injector).
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Monoloop run correlation identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MonoloopRunId(String);

impl MonoloopRunId {
    /// Create from an explicit value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Allocate a random run id.
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MonoloopRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Opaque externally owned session identity (e.g. Grok `sessionId`).
///
/// Monoloop compares and routes this value; it does not invent a competing ID
/// or derive authority from its contents.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExternalSessionId(String);

impl ExternalSessionId {
    /// Wrap an external system's authoritative session id.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrow the opaque value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExternalSessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display redacts by default in logs via Debug; Display is for tests only.
        f.write_str("<external-session>")
    }
}

/// Grok Build's authoritative `sessionId` — the sole session correlation identity
/// for the Grok connector profile.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GrokSessionId(ExternalSessionId);

impl GrokSessionId {
    /// Wrap a Grok-returned session id.
    pub fn new(value: impl Into<String>) -> Self {
        Self(ExternalSessionId::new(value))
    }

    /// Borrow the opaque session id string (for protocol routing only).
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// View as a generic external session id.
    pub fn as_external(&self) -> &ExternalSessionId {
        &self.0
    }

    /// Convert into a generic external session id.
    pub fn into_external(self) -> ExternalSessionId {
        self.0
    }
}

impl From<GrokSessionId> for ExternalSessionId {
    fn from(value: GrokSessionId) -> Self {
        value.0
    }
}

/// Caller/request correlation identity (opaque, no authority).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(String);

impl RequestId {
    /// Create from an explicit value.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Allocate a random request id.
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
