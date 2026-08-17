//! Connector instance identity, factory, and matched Connector/SessionAdapter pair.

use crate::session::SessionAdapter;
use crate::traits::Connector;
use monoloop_contracts::IdentityError;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

/// Identity of one constructed Connector instance (one per Channel at runtime).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConnectorInstanceId(String);

impl ConnectorInstanceId {
    /// Fallible constructor.
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let s = value.into();
        monoloop_contracts::validate_identity_string(&s)?;
        Ok(Self(s))
    }

    /// Allocate a random instance id.
    pub fn generate() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConnectorInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One matched Connector plus optional SessionAdapter for a Channel.
pub struct ConnectorInstance {
    /// Instance identity used as session-attachment owner.
    pub instance_id: ConnectorInstanceId,
    /// Transport connector.
    pub connector: Arc<dyn Connector>,
    /// Present for external-agent Channels; `None` for direct LLM.
    pub sessions: Option<Arc<dyn SessionAdapter>>,
}

impl ConnectorInstance {
    /// Construct an instance with the given parts.
    pub fn new(
        instance_id: ConnectorInstanceId,
        connector: Arc<dyn Connector>,
        sessions: Option<Arc<dyn SessionAdapter>>,
    ) -> Self {
        Self {
            instance_id,
            connector,
            sessions,
        }
    }
}

/// Factory that produces matched Connector/SessionAdapter instances.
pub trait ConnectorFactory: Send + Sync {
    /// Create one instance (called once per Channel at runtime startup).
    fn create(&self) -> Result<ConnectorInstance, ConnectorBuildError>;
}

/// Failure constructing a Connector instance.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ConnectorBuildError {
    /// Configuration invalid.
    #[error("connector configuration invalid: {0}")]
    ConfigurationInvalid(&'static str),
    /// Resource unavailable.
    #[error("connector resource unavailable: {0}")]
    ResourceUnavailable(&'static str),
    /// Internal invariant.
    #[error("connector build invariant failed: {0}")]
    InvariantFailed(&'static str),
}
