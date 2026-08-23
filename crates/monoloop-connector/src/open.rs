//! Open request and connection handle types.

use crate::control::ConnectionControlHandle;
use crate::handles::{ConnectionCompletionHandle, RawInputHandle, RawOutputHandle};
use crate::session::SessionAttachment;
use monoloop_contracts::{
    ConnectionId, ConnectorError, ConnectorLimits, DialectBinding, ExternalSessionId,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Unspawned connection-local owner future (v2 §15).
///
/// The Loop MUST spawn this via `TaskSupervisor` as `ConnectorOwner` **before**
/// polling [`PendingRawConnection::opened`]. The owner future drives open I/O
/// and post-open transport; transport semantic completion does not imply this
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

    /// No-op owner (open failed before any owner I/O, or inert placeholder).
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

/// Open in progress: control and owner identity are available immediately (D-051 / v2 §15).
///
/// The Loop MUST [`Self::take_owner_work`] and register `ConnectorOwner` **before**
/// the first poll of [`Self::opened`]. Open I/O runs inside that owner future.
pub struct PendingRawConnection {
    /// Connection identity.
    pub connection_id: ConnectionId,
    /// Same connection-scoped control as the eventual opened connection.
    pub control: ConnectionControlHandle,
    /// Completes with opened handles or a typed open error.
    ///
    /// Signaled by the owner future after open setup; do not poll until the
    /// owner is registered under `TaskSupervisor`.
    pub opened: OpenCompletion,
    /// Owner work: open I/O + post-open transport (taken exactly once).
    owner_work: Option<ConnectionOwnerWork>,
}

impl PendingRawConnection {
    /// Construct pending open with required owner work (D-051).
    pub fn new(
        connection_id: ConnectionId,
        control: ConnectionControlHandle,
        opened: OpenCompletion,
        owner_work: ConnectionOwnerWork,
    ) -> Self {
        Self {
            connection_id,
            control,
            opened,
            owner_work: Some(owner_work),
        }
    }

    /// Wrap an open-setup future that returns handles + post-open transport work.
    ///
    /// The combined owner future performs setup I/O, signals [`Self::opened`],
    /// then runs transport until completion. Spawn this owner before polling
    /// `opened`.
    pub fn open_owned<F>(
        connection_id: ConnectionId,
        control: ConnectionControlHandle,
        open_and_own: F,
    ) -> Self
    where
        F: Future<Output = Result<(OpenedRawConnection, ConnectionOwnerWork), ConnectorError>>
            + Send
            + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let owner_work = ConnectionOwnerWork::new(async move {
            match open_and_own.await {
                Ok((opened, transport)) => {
                    let _ = tx.send(Ok(opened));
                    transport.into_future().await;
                }
                Err(e) => {
                    let _ = tx.send(Err(e));
                }
            }
        });
        Self {
            connection_id,
            control,
            opened: Box::pin(async move {
                rx.await
                    .unwrap_or_else(|_| Err(ConnectorError::cancelled()))
            }),
            owner_work: Some(owner_work),
        }
    }

    /// Fail-closed pending (no open I/O). Owner is a noop so spawn/join stays honest.
    pub fn failed(
        connection_id: ConnectionId,
        control: ConnectionControlHandle,
        err: ConnectorError,
    ) -> Self {
        Self {
            connection_id,
            control,
            opened: Box::pin(async move { Err(err) }),
            owner_work: Some(ConnectionOwnerWork::noop()),
        }
    }

    /// Take owner work for supervised spawn (exactly once).
    ///
    /// Panics if called twice — profiles and the Loop must transfer ownership once.
    pub fn take_owner_work(&mut self) -> ConnectionOwnerWork {
        self.owner_work
            .take()
            .expect("PendingRawConnection owner_work already taken")
    }
}

/// Future resolving to an opened connection or error.
pub type OpenCompletion = Pin<
    Box<
        dyn Future<Output = Result<OpenedRawConnection, monoloop_contracts::ConnectorError>> + Send,
    >,
>;

/// Successfully opened raw connection (handles only — no owner work; D-051).
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
