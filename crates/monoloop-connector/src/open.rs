//! Open request and connection handle types.

use crate::control::ConnectionControlHandle;
use crate::handles::{ConnectionCompletionHandle, RawInputHandle, RawOutputHandle};
use monoloop_contracts::{
    ConnectionId, ConnectorLimits, DialectBinding, ExternalSessionId,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Transport-only open request (no prompt, task, or UI types).
#[derive(Clone, Debug)]
pub struct OpenConnection {
    /// Caller-supplied or allocated connection identity.
    pub connection_id: ConnectionId,
    /// Opaque configuration / endpoint reference for the connector.
    pub endpoint_ref: String,
    /// Optional externally owned session identity to attach.
    pub external_session_id: Option<ExternalSessionId>,
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
            credential_ref: None,
            required_dialect: None,
            limits: ConnectorLimits::default(),
        }
    }
}

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
pub type OpenCompletion =
    Pin<Box<dyn Future<Output = Result<OpenedRawConnection, monoloop_contracts::ConnectorError>> + Send>>;

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
}
