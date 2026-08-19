//! Open request and connection handle types.

use crate::control::ConnectionControlHandle;
use crate::handles::{ConnectionCompletionHandle, RawInputHandle, RawOutputHandle};
use crate::session::SessionAttachment;
use monoloop_contracts::{ConnectionId, ConnectorLimits, DialectBinding, ExternalSessionId};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Unspawned connection-local owner future (v2 §15).
///
/// The Loop MUST spawn this via `TaskSupervisor` as `ConnectorOwner` before
/// relying on input/output. Transport semantic completion does not imply this
/// future has finished unless the profile documents that equivalence.
pub struct ConnectionOwnerWork {
    run: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl ConnectionOwnerWork {
    /// Build owner work from an unspawned future.
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self {
            run: Box::pin(future),
        }
    }

    /// No-op owner (open failed before any owner ran, or inert placeholder).
    pub fn noop() -> Self {
        Self {
            run: Box::pin(async {}),
        }
    }

    /// Consume into a spawnable future.
    pub fn into_future(self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.run
    }
}

/// Transport-only open request (no prompt, task, or UI types).
#[derive(Clone, Debug)]
pub struct OpenConnection {
    /// Caller-supplied or allocated connection identity.
    pub connection_id: ConnectionId,
    /// Opaque configuration / endpoint reference for the connector.
    pub endpoint_ref: String,
    /// Optional externally owned session identity to attach.
    pub external_session_id: Option<ExternalSessionId>,
    /// Optional session attachment from the matched SessionAdapter (ownership-checked).
    pub session_attachment: Option<Arc<SessionAttachment>>,
    /// Opaque credential reference (resolved inside the connector boundary).
    pub credential_ref: Option<String>,
    /// Required dialect family/version range (descriptive; connector validates).
    pub required_dialect: Option<String>,
    /// Limits for this open.
    pub limits: ConnectorLimits,
}

impl OpenConnection {
    /// Build a minimal open request for tests.
    pub fn new(connection_id: ConnectionId, endpoint_ref: impl Into<String>) -> Self {
        Self {
            connection_id,
            endpoint_ref: endpoint_ref.into(),
            external_session_id: None,
            session_attachment: None,
            credential_ref: None,
            required_dialect: None,
            limits: ConnectorLimits::default(),
        }
    }

    /// Attach a session attachment (runtime path after SessionAdapter success).
    ///
    /// Load (`create_mode == false`): sets `external_session_id` for provider load.
    /// Create (`create_mode == true`): leaves `external_session_id` unset so the
    /// Connector performs provider create and returns the authoritative id.
    pub fn with_session_attachment(mut self, attachment: Arc<SessionAttachment>) -> Self {
        if !attachment.create_mode {
            self.external_session_id = Some(attachment.external_session_id.clone());
        }
        self.session_attachment = Some(attachment);
        self
    }
}

// SessionAttachment is not Debug by default on route — provide limited Debug for OpenConnection.
// OpenConnection derives Debug; SessionAttachment needs Debug.

/// Open in progress: control is available immediately.
pub struct PendingRawConnection {
    /// Connection identity.
    pub connection_id: ConnectionId,
    /// Same connection-scoped control as the eventual opened connection.
    pub control: ConnectionControlHandle,
    /// Completes with opened handles or a typed open error.
    pub opened: OpenCompletion,
}

/// Future resolving to an opened connection or error.
pub type OpenCompletion = Pin<
    Box<
        dyn Future<Output = Result<OpenedRawConnection, monoloop_contracts::ConnectorError>> + Send,
    >,
>;

/// Successfully opened raw connection.
pub struct OpenedRawConnection {
    /// Connection identity.
    pub connection_id: ConnectionId,
    /// Present when attached to an externally identified session.
    pub external_session_id: Option<ExternalSessionId>,
    /// Frozen dialect binding for this connection.
    pub dialect: DialectBinding,
    /// Ordered encoded input.
    pub input: RawInputHandle,
    /// Ordered raw output.
    pub output: Arc<RawOutputHandle>,
    /// Out-of-band control.
    pub control: ConnectionControlHandle,
    /// Exactly-one terminal outcome.
    pub completion: ConnectionCompletionHandle,
    /// Connection owner work the Loop MUST spawn via TaskSupervisor (v2 §15).
    ///
    /// `None` is a temporary migration residual for profiles not yet converted
    /// off ambient spawn; v2 exchange fails closed when this is missing.
    pub owner_work: Option<ConnectionOwnerWork>,
}

impl OpenedRawConnection {
    /// Take owner work for supervised spawn (exactly once).
    pub fn take_owner_work(&mut self) -> Option<ConnectionOwnerWork> {
        self.owner_work.take()
    }
}
