//! External session attachment ownership (SessionAdapter seam).

use crate::control::ControlDisposition;
use crate::instance::ConnectorInstanceId;
use monoloop_contracts::{
    ChannelId, ExternalSessionId, SessionConfig, SessionId, TransactionId,
};
use secrecy::{ExposeSecret, SecretString};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;

/// Opaque local routing authority for an attached external session.
pub trait SessionRoute: Send + Sync {
    /// Owning Connector instance.
    fn owner(&self) -> &ConnectorInstanceId;
}

/// Successful external session attachment (immutable after construction).
pub struct SessionAttachment {
    /// Connector instance that created this attachment.
    pub owner: ConnectorInstanceId,
    /// Authoritative external session id.
    pub external_session_id: ExternalSessionId,
    /// Effective immutable session configuration after normalize/validate.
    pub effective_session_config: SessionConfig,
    /// Opaque route; only meaningful to the owning instance.
    pub route: Arc<dyn SessionRoute>,
}

impl fmt::Debug for SessionAttachment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionAttachment")
            .field("owner", &self.owner)
            .field("external_session_id", &"<redacted>")
            .field("effective_session_config", &self.effective_session_config)
            .field("route_owner", self.route.owner())
            .finish()
    }
}

impl SessionAttachment {
    /// Construct an attachment.
    pub fn new(
        owner: ConnectorInstanceId,
        external_session_id: ExternalSessionId,
        effective_session_config: SessionConfig,
        route: Arc<dyn SessionRoute>,
    ) -> Self {
        Self {
            owner,
            external_session_id,
            effective_session_config,
            route,
        }
    }
}

/// MCP server descriptor offered to an external agent (secret capability URL).
pub struct McpServerDescriptor {
    /// Bounded server name.
    pub server_name: String,
    /// Protocol version string.
    pub protocol_version: String,
    capability_url: SecretString,
}

impl McpServerDescriptor {
    /// Maximum server name bytes.
    pub const MAX_NAME_BYTES: usize = 128;
    /// Maximum protocol version bytes.
    pub const MAX_PROTOCOL_BYTES: usize = 64;
    /// Maximum capability URL bytes.
    pub const MAX_URL_BYTES: usize = 512;

    /// Construct a redacted descriptor.
    pub fn try_new(
        server_name: impl Into<String>,
        protocol_version: impl Into<String>,
        capability_url: impl Into<String>,
    ) -> Result<Self, SessionAttachError> {
        let server_name = server_name.into();
        let protocol_version = protocol_version.into();
        let url = capability_url.into();
        if server_name.is_empty()
            || server_name.len() > Self::MAX_NAME_BYTES
            || server_name.chars().any(|c| c.is_control())
        {
            return Err(SessionAttachError::InvalidMcpDescriptor);
        }
        if protocol_version.is_empty()
            || protocol_version.len() > Self::MAX_PROTOCOL_BYTES
            || protocol_version.chars().any(|c| c.is_control())
        {
            return Err(SessionAttachError::InvalidMcpDescriptor);
        }
        if url.is_empty() || url.len() > Self::MAX_URL_BYTES {
            return Err(SessionAttachError::InvalidMcpDescriptor);
        }
        Ok(Self {
            server_name,
            protocol_version,
            capability_url: SecretString::from(url),
        })
    }

    /// Expose the capability URL only to a SessionAdapter serialization path.
    pub fn expose_capability_url(&self) -> &str {
        self.capability_url.expose_secret()
    }
}

impl fmt::Debug for McpServerDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpServerDescriptor")
            .field("server_name", &self.server_name)
            .field("protocol_version", &self.protocol_version)
            .field("capability_url", &"<redacted>")
            .finish()
    }
}

/// Request to create or load an external session.
#[derive(Clone, Debug)]
pub struct SessionAttachRequest {
    /// Owning transaction (correlation only).
    pub transaction_id: TransactionId,
    /// Selected Channel.
    pub channel_id: ChannelId,
    /// Existing session to load; `None` creates a new session.
    pub requested_session_id: Option<SessionId>,
    /// Requested session configuration (no prompt/secrets).
    pub session_config: SessionConfig,
    /// Optional initial MCP descriptor for create-time install.
    pub initial_mcp: Option<McpServerDescriptor>,
    /// Absolute deadline.
    pub deadline: Instant,
}

// Manual Clone for SessionAttachRequest because McpServerDescriptor is not Clone by default.
impl Clone for McpServerDescriptor {
    fn clone(&self) -> Self {
        Self {
            server_name: self.server_name.clone(),
            protocol_version: self.protocol_version.clone(),
            capability_url: SecretString::from(self.capability_url.expose_secret().to_string()),
        }
    }
}

/// Immediate control for a pending session operation.
pub trait PendingOperationControl: Send + Sync {
    /// Cooperative cancel.
    fn cancel(&self) -> ControlDisposition;
    /// Forced terminate.
    fn force_terminate(&self) -> ControlDisposition;
}

/// Future completing a session attach/create/load.
pub type SessionAttachmentCompletion = Pin<
    Box<dyn Future<Output = Result<Arc<SessionAttachment>, SessionAttachError>> + Send + 'static>,
>;

/// Pending attach with immediate control.
pub struct PendingSessionAttachment {
    /// Cancel / force control (available before I/O completes).
    pub control: Arc<dyn PendingOperationControl>,
    /// Exactly-one completion.
    pub completion: SessionAttachmentCompletion,
}

/// Future completing MCP refresh / removal.
pub type SessionConfigurationCompletion = Pin<
    Box<dyn Future<Output = Result<(), SessionConfigurationError>> + Send + 'static>,
>;

/// Pending MCP configuration operation.
pub struct PendingSessionConfiguration {
    /// Cancel / force control.
    pub control: Arc<dyn PendingOperationControl>,
    /// Exactly-one completion.
    pub completion: SessionConfigurationCompletion,
}

/// External session attach/create/load port.
pub trait SessionAdapter: Send + Sync {
    /// Begin create (`requested_session_id == None`) or load (Some).
    fn begin_attach(
        &self,
        request: SessionAttachRequest,
    ) -> Result<PendingSessionAttachment, SessionAttachError>;

    /// Install (`Some`) or remove (`None`) Monoloop MCP configuration on an attachment.
    fn begin_refresh_mcp(
        &self,
        attachment: Arc<SessionAttachment>,
        descriptor: Option<McpServerDescriptor>,
    ) -> Result<PendingSessionConfiguration, SessionConfigurationError>;
}

/// Session attach failure (safe, closed).
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SessionAttachError {
    /// Cooperative cancel won.
    #[error("session attach cancelled")]
    Cancelled,
    /// Forced terminate won.
    #[error("session attach terminated")]
    Terminated,
    /// Deadline exceeded.
    #[error("session attach deadline exceeded")]
    DeadlineExceeded,
    /// Immutable configuration mismatch on load.
    #[error("session configuration mismatch")]
    ConfigurationMismatch,
    /// Setting unsupported for attach/load.
    #[error("session configuration unsupported")]
    UnsupportedConfiguration,
    /// Provider session create/load failed.
    #[error("session operation failed")]
    SessionFailed,
    /// Invalid MCP descriptor bounds.
    #[error("invalid MCP descriptor")]
    InvalidMcpDescriptor,
    /// Dropped completion / impossible state.
    #[error("session attach invariant failed")]
    InvariantFailed,
    /// Capacity exceeded.
    #[error("session capacity exceeded")]
    CapacityExceeded,
    /// Requested session id bytes must match returned external id.
    #[error("session id mismatch")]
    SessionIdMismatch,
}

/// MCP configuration refresh failure.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SessionConfigurationError {
    /// Cancelled.
    #[error("session configuration cancelled")]
    Cancelled,
    /// Terminated.
    #[error("session configuration terminated")]
    Terminated,
    /// Deadline exceeded.
    #[error("session configuration deadline exceeded")]
    DeadlineExceeded,
    /// Attachment owner mismatch.
    #[error("session attachment owner mismatch")]
    OwnerMismatch,
    /// Profile does not support refresh/removal.
    #[error("MCP configuration not supported")]
    Unsupported,
    /// Provider failed.
    #[error("session configuration failed")]
    ConfigurationFailed,
    /// Invariant (dropped completion).
    #[error("session configuration invariant failed")]
    InvariantFailed,
}

/// Validate that an open request's session attachment (if any) is owned by `instance_id`.
pub fn validate_open_attachment_owner(
    instance_id: &ConnectorInstanceId,
    attachment: Option<&SessionAttachment>,
) -> Result<(), monoloop_contracts::ConnectorError> {
    if let Some(att) = attachment {
        if &att.owner != instance_id {
            return Err(monoloop_contracts::ConnectorError::new(
                monoloop_contracts::ConnectorErrorKind::ConfigurationInvalid,
                "session attachment owner does not match connector instance",
            ));
        }
        if att.route.owner() != instance_id {
            return Err(monoloop_contracts::ConnectorError::new(
                monoloop_contracts::ConnectorErrorKind::InvariantViolation,
                "session route owner does not match attachment owner",
            ));
        }
    }
    Ok(())
}

/// Enforce: when a SessionId is supplied, attachment external id bytes must match.
pub fn validate_session_id_match(
    requested: Option<&SessionId>,
    external: &ExternalSessionId,
) -> Result<(), SessionAttachError> {
    if let Some(req) = requested {
        if req.as_str() != external.as_str() {
            return Err(SessionAttachError::SessionIdMismatch);
        }
    }
    Ok(())
}
