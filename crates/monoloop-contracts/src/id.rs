//! Correlation identities. Opaque wrappers — no ambient current identity.
//!
//! Public string newtypes reject empty, oversized, and control-character values.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

/// Maximum bytes for opaque string identities and tool names.
pub const MAX_IDENTITY_BYTES: usize = 256;

/// Identity construction failure (safe, closed).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum IdentityError {
    /// Empty string rejected.
    #[error("identity must be non-empty")]
    Empty,
    /// Exceeds [`MAX_IDENTITY_BYTES`].
    #[error("identity exceeds maximum length of {MAX_IDENTITY_BYTES} bytes")]
    TooLong,
    /// Contains a Unicode control character.
    #[error("identity must not contain control characters")]
    ControlCharacter,
}

/// Validate a bounded opaque identity string.
pub fn validate_identity_string(value: &str) -> Result<(), IdentityError> {
    if value.is_empty() {
        return Err(IdentityError::Empty);
    }
    if value.len() > MAX_IDENTITY_BYTES {
        return Err(IdentityError::TooLong);
    }
    if value.chars().any(|c| c.is_control()) {
        return Err(IdentityError::ControlCharacter);
    }
    Ok(())
}

fn validated_string(value: impl Into<String>) -> Result<String, IdentityError> {
    let s = value.into();
    validate_identity_string(&s)?;
    Ok(s)
}

/// Local logical transport attachment identity for one connection scope.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionId(String);

impl ConnectionId {
    /// Create a connection id from an explicit caller-supplied string.
    ///
    /// # Panics
    ///
    /// Panics if `value` fails identity validation. Prefer [`Self::try_new`].
    pub fn new(value: impl Into<String>) -> Self {
        Self::try_new(value).expect("ConnectionId::new requires a valid identity string")
    }

    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        Ok(Self(validated_string(value)?))
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
    ///
    /// # Panics
    ///
    /// Panics if `value` fails identity validation. Prefer [`Self::try_new`].
    pub fn new(value: impl Into<String>) -> Self {
        Self::try_new(value).expect("MonoloopRunId::new requires a valid identity string")
    }

    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        Ok(Self(validated_string(value)?))
    }

    /// Allocate a random run id.
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// One-to-one derivation from a transaction id (internal component correlation).
    pub fn from_transaction(id: &TransactionId) -> Self {
        Self(format!("txn:{}", id.as_uuid()))
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for MonoloopRunId {
    fn default() -> Self {
        Self::generate()
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
    ///
    /// # Panics
    ///
    /// Panics if `value` fails identity validation. Prefer [`Self::try_new`].
    pub fn new(value: impl Into<String>) -> Self {
        Self::try_new(value).expect("ExternalSessionId::new requires a valid identity string")
    }

    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        Ok(Self(validated_string(value)?))
    }

    /// Borrow the opaque value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExternalSessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display redacts by default; Display is for tests/safe logs only.
        f.write_str("<external-session>")
    }
}

/// Grok Build's authoritative `sessionId` — the sole session correlation identity
/// for the Grok connector profile.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GrokSessionId(ExternalSessionId);

impl GrokSessionId {
    /// Wrap a Grok-returned session id.
    ///
    /// # Panics
    ///
    /// Panics if `value` fails identity validation. Prefer [`Self::try_new`].
    pub fn new(value: impl Into<String>) -> Self {
        Self::try_new(value).expect("GrokSessionId::new requires a valid identity string")
    }

    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        Ok(Self(ExternalSessionId::try_new(value)?))
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
    ///
    /// # Panics
    ///
    /// Panics if `value` fails identity validation. Prefer [`Self::try_new`].
    pub fn new(value: impl Into<String>) -> Self {
        Self::try_new(value).expect("RequestId::new requires a valid identity string")
    }

    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        Ok(Self(validated_string(value)?))
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

/// Admitted transaction identity (Monoloop-generated, never reused).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId(Uuid);

impl TransactionId {
    /// Allocate a fresh transaction id.
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wrap an existing UUID (admission / tests).
    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// Borrow the UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Stable string form (not a secret).
    pub fn as_str(&self) -> String {
        self.0.to_string()
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One provider request/response exchange inside a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExchangeId(Uuid);

impl ExchangeId {
    /// Allocate a fresh exchange id.
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wrap an existing UUID.
    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// Borrow the UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Caller-visible session / correlation identity for transaction routing.
///
/// For external-agent Channels this is the validated external session string.
/// For direct-LLM Channels it is ephemeral routing only (no provider history).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(String);

impl SessionId {
    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        Ok(Self(validated_string(value)?))
    }

    /// Allocate a random direct-LLM session id.
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// View as an external session id with identical bytes.
    pub fn as_external(&self) -> ExternalSessionId {
        ExternalSessionId(self.0.clone())
    }

    /// Convert into an external session id with identical bytes.
    pub fn into_external(self) -> ExternalSessionId {
        ExternalSessionId(self.0)
    }

    /// Build from an external session id with identical bytes.
    pub fn from_external(id: &ExternalSessionId) -> Self {
        Self(id.0.clone())
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<session>")
    }
}

/// Channel identity (caller-selected; never ambient).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChannelId(String);

impl ChannelId {
    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        Ok(Self(validated_string(value)?))
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Session exclusion and session-directed control key.
///
/// Equal session strings on different Channels are distinct keys.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionKey {
    /// Selected Channel.
    pub channel_id: ChannelId,
    /// Session correlation identity on that Channel.
    pub session_id: SessionId,
}

impl SessionKey {
    /// Construct a session key from validated components.
    pub fn new(channel_id: ChannelId, session_id: SessionId) -> Self {
        Self {
            channel_id,
            session_id,
        }
    }
}

/// Stable host-registry tool identity (selection key on requests).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToolId(String);

impl ToolId {
    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        Ok(Self(validated_string(value)?))
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ToolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Tool name as exposed to models / MCP (distinct from [`ToolId`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ToolName(String);

impl ToolName {
    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        Ok(Self(validated_string(value)?))
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ToolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_and_control() {
        assert_eq!(SessionId::try_new(""), Err(IdentityError::Empty));
        assert_eq!(
            ChannelId::try_new("a\nb"),
            Err(IdentityError::ControlCharacter)
        );
        assert!(ToolId::try_new("x".repeat(MAX_IDENTITY_BYTES + 1)).is_err());
    }

    #[test]
    fn session_key_isolates_channels() {
        let a = SessionKey::new(
            ChannelId::try_new("ch-a").unwrap(),
            SessionId::try_new("same-sess").unwrap(),
        );
        let b = SessionKey::new(
            ChannelId::try_new("ch-b").unwrap(),
            SessionId::try_new("same-sess").unwrap(),
        );
        assert_ne!(a, b);
        assert_eq!(a.session_id.as_str(), b.session_id.as_str());
    }

    #[test]
    fn session_external_round_trip_bytes() {
        let ext = ExternalSessionId::try_new("provider-abc").unwrap();
        let sid = SessionId::from_external(&ext);
        assert_eq!(sid.as_str(), ext.as_str());
        assert_eq!(sid.into_external().as_str(), "provider-abc");
    }

    #[test]
    fn transaction_id_serializes() {
        let id = TransactionId::generate();
        let json = serde_json::to_string(&id).unwrap();
        let back: TransactionId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }
}
