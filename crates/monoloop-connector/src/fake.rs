//! Deterministic in-memory FakeConnector for tests and architecture gates.

use crate::control::{ConnectionControlHandle, ControlState};
use crate::descriptor::ConnectorDescriptor;
use crate::handles::{
    ConnectionCompletionHandle, ConnectionEndKind, ConnectionOwner, EndInitiator, RawInputHandle,
    RawInputMessage, RawOutputHandle,
};
use crate::instance::ConnectorInstanceId;
use crate::open::{OpenConnection, OpenedRawConnection, PendingRawConnection};
use crate::session::validate_open_attachment_owner;
use crate::traits::Connector;
use bytes::Bytes;
use monoloop_contracts::{
    ConnectionId, ConnectorError, ConnectorErrorKind, DialectBinding, DialectDescriptor,
    ExternalSessionId,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

/// Behaviour of a fake endpoint.
#[derive(Clone, Debug)]
pub enum FakeEndpoint {
    /// Echo each input chunk back on output; finish closes output after drain.
    Echo,
    /// Scripted output chunks delivered after open (independent of input).
    Scripted {
        /// Chunks to emit on output in order.
        chunks: Vec<Bytes>,
    },
    /// Pair two connection opens with the same `pair_key`: A's input → B's output.
    Pair {
        /// Shared pairing key.
        pair_key: String,
    },
    /// Accept input but never produce output until cancel/terminate (D-012 response-wait).
    Hang,
}

/// Configuration for [`FakeConnector`].
#[derive(Clone, Debug)]
pub struct FakeConnectorConfig {
    /// Default endpoint when `endpoint_ref` is empty or `"default"`.
    pub default_endpoint: FakeEndpoint,
    /// Named endpoints keyed by `OpenConnection.endpoint_ref`.
    pub endpoints: HashMap<String, FakeEndpoint>,
    /// Artificial open delay (tests cancel-during-open).
    pub open_delay: Duration,
    /// If true, open fails with connection_failed.
    pub fail_open: bool,
    /// When true, create_mode open returns no external session id (D-026 tests).
    pub omit_created_session_id: bool,
}

impl Default for FakeConnectorConfig {
    fn default() -> Self {
        Self {
            default_endpoint: FakeEndpoint::Echo,
            endpoints: HashMap::new(),
            open_delay: Duration::ZERO,
            fail_open: false,
            omit_created_session_id: false,
        }
    }
}

#[derive(Default)]
struct PairState {
    waiting: HashMap<String, PairHalf>,
}

struct PairHalf {
    out_tx: mpsc::Sender<Bytes>,
    ready: oneshot::Sender<mpsc::Sender<Bytes>>,
}

/// Deterministic connector: no network, no semantic parsing.
pub struct FakeConnector {
    descriptor: ConnectorDescriptor,
    config: FakeConnectorConfig,
    pairs: Arc<Mutex<PairState>>,
    /// Instance identity for session-attachment ownership checks.
    instance_id: ConnectorInstanceId,
}

impl FakeConnector {
    /// Create a fake connector with the given config (fresh instance id).
    pub fn new(config: FakeConnectorConfig) -> Self {
        Self::with_instance_id_and_config(ConnectorInstanceId::generate(), config)
    }

    /// Create with an explicit instance id (matched SessionAdapter ownership).
    pub fn with_instance_id(instance_id: ConnectorInstanceId) -> Self {
        Self::with_instance_id_and_config(instance_id, FakeConnectorConfig::default())
    }

    /// Create with instance id and config.
    pub fn with_instance_id_and_config(
        instance_id: ConnectorInstanceId,
        config: FakeConnectorConfig,
    ) -> Self {
        Self {
            descriptor: ConnectorDescriptor::fake(),
            config,
            pairs: Arc::new(Mutex::new(PairState::default())),
            instance_id,
        }
    }

    /// Echo-only defaults.
    pub fn echo() -> Self {
        Self::new(FakeConnectorConfig::default())
    }

    /// Borrow instance id.
    pub fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn resolve_endpoint(&self, endpoint_ref: &str) -> FakeEndpoint {
        if endpoint_ref.is_empty() || endpoint_ref == "default" {
            return self.config.default_endpoint.clone();
        }
        self.config
            .endpoints
            .get(endpoint_ref)
            .cloned()
            .unwrap_or_else(|| self.config.default_endpoint.clone())
    }
}

impl Connector for FakeConnector {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }

    fn begin_open(&self, request: OpenConnection) -> PendingRawConnection {
        let control_state = ControlState::new();
        let control = ConnectionControlHandle::new(Arc::clone(&control_state));
        let connection_id = request.connection_id.clone();
        let config = self.config.clone();
        let endpoint = self.resolve_endpoint(&request.endpoint_ref);
        let pairs = Arc::clone(&self.pairs);
        let control_for_open = control.clone();
        let control_state_for_open = Arc::clone(&control_state);
        let instance_id = self.instance_id.clone();

        let opened = Box::pin(async move {
            open_fake(
                request,
                config,
                endpoint,
                pairs,
                control_for_open,
                control_state_for_open,
                instance_id,
            )
            .await
        });

        PendingRawConnection {
            connection_id,
            control,
            opened,
        }
    }
}

async fn open_fake(
    request: OpenConnection,
    config: FakeConnectorConfig,
    endpoint: FakeEndpoint,
    pairs: Arc<Mutex<PairState>>,
    control: ConnectionControlHandle,
    control_state: Arc<ControlState>,
    instance_id: ConnectorInstanceId,
) -> Result<OpenedRawConnection, ConnectorError> {
    validate_open_attachment_owner(&instance_id, request.session_attachment.as_deref())?;

    if config.fail_open {
        return Err(
            ConnectorError::connection_failed("fake configured to fail open")
                .with_connection_id(request.connection_id.as_str()),
        );
    }

    if config.open_delay > Duration::ZERO {
        tokio::select! {
            _ = tokio::time::sleep(config.open_delay) => {}
            _ = control.interrupted() => {
                return Err(control
                    .interrupt_error()
                    .unwrap_or_else(ConnectorError::cancelled)
                    .with_connection_id(request.connection_id.as_str()));
            }
        }
    }

    if let Some(err) = control.interrupt_error() {
        return Err(err.with_connection_id(request.connection_id.as_str()));
    }

    let buf = request.limits.buffers.max_queued_input_bytes.max(1);
    // Channel capacity in messages (approx); enforce chunk size on send.
    let capacity = (buf / request.limits.buffers.max_chunk_bytes.max(1)).max(1);

    let (in_tx, in_rx) = mpsc::channel::<RawInputMessage>(capacity);
    let (out_tx, out_rx) = mpsc::channel::<Bytes>(capacity);
    let (end_tx, end_rx) = oneshot::channel();

    let connection_id = request.connection_id.clone();
    let max_chunk = request.limits.buffers.max_chunk_bytes;
    // D-013: create_mode → allocate authoritative id; load uses attachment/request id.
    let external_session_id = if request
        .session_attachment
        .as_ref()
        .is_some_and(|a| a.create_mode)
    {
        if config.omit_created_session_id {
            None
        } else {
            Some(ExternalSessionId::new(format!(
                "fake-created-{}",
                uuid::Uuid::new_v4()
            )))
        }
    } else {
        request
            .session_attachment
            .as_ref()
            .map(|a| a.external_session_id.clone())
            .or(request.external_session_id.clone())
    };

    let input = RawInputHandle::new(
        connection_id.clone(),
        in_tx,
        Arc::clone(&control_state),
        max_chunk,
    );
    let output = Arc::new(RawOutputHandle::new(
        connection_id.clone(),
        out_rx,
        Arc::clone(&control_state),
    ));
    let completion = ConnectionCompletionHandle::new(end_rx);

    let owner_work = match endpoint {
        FakeEndpoint::Echo => crate::open::ConnectionOwnerWork::new(run_echo_owner(
            connection_id.clone(),
            control_state,
            in_rx,
            out_tx,
            end_tx,
        )),
        FakeEndpoint::Scripted { chunks } => {
            crate::open::ConnectionOwnerWork::new(run_scripted_owner(
                connection_id.clone(),
                control_state,
                in_rx,
                out_tx,
                end_tx,
                chunks,
            ))
        }
        FakeEndpoint::Pair { pair_key } => {
            let peer_tx = register_pair(&pairs, &pair_key, out_tx.clone()).await?;
            crate::open::ConnectionOwnerWork::new(run_pair_owner(
                connection_id.clone(),
                control_state,
                in_rx,
                peer_tx,
                out_tx,
                end_tx,
            ))
        }
        FakeEndpoint::Hang => {
            // Never emit output; hold until local cancel/terminate.
            drop(out_tx);
            crate::open::ConnectionOwnerWork::new(run_hang_owner(
                connection_id.clone(),
                control_state,
                in_rx,
                end_tx,
            ))
        }
    };

    Ok(OpenedRawConnection {
        connection_id,
        external_session_id,
        dialect: DialectBinding::fixed(DialectDescriptor::test_raw()),
        input,
        output,
        control,
        completion,
        owner_work: Some(owner_work),
    })
}

async fn register_pair(
    pairs: &Arc<Mutex<PairState>>,
    pair_key: &str,
    my_out_tx: mpsc::Sender<Bytes>,
) -> Result<mpsc::Sender<Bytes>, ConnectorError> {
    let waiter = {
        let mut state = pairs.lock().map_err(|_| {
            ConnectorError::new(ConnectorErrorKind::InvariantViolation, "pair lock")
        })?;
        if let Some(half) = state.waiting.remove(pair_key) {
            // Second half: give first half our out_tx; take their out_tx as peer.
            let _ = half.ready.send(my_out_tx);
            return Ok(half.out_tx);
        }
        let (ready_tx, ready_rx) = oneshot::channel();
        state.waiting.insert(
            pair_key.to_string(),
            PairHalf {
                out_tx: my_out_tx,
                ready: ready_tx,
            },
        );
        ready_rx
    };
    timeout(Duration::from_secs(5), waiter)
        .await
        .map_err(|_| ConnectorError::connection_failed("pair timeout"))?
        .map_err(|_| ConnectorError::connection_failed("pair cancelled"))
}

async fn run_echo_owner(
    connection_id: ConnectionId,
    control: Arc<ControlState>,
    mut in_rx: mpsc::Receiver<RawInputMessage>,
    out_tx: mpsc::Sender<Bytes>,
    end_tx: oneshot::Sender<crate::handles::ConnectionEnd>,
) {
    let mut owner = ConnectionOwner::new(connection_id, Arc::clone(&control), end_tx);
    loop {
        tokio::select! {
            biased;
            _ = wait_control(&control) => {
                let kind = if control.terminate_requested() {
                    ConnectionEndKind::Terminated
                } else {
                    ConnectionEndKind::Cancelled
                };
                owner.finish(kind, EndInitiator::LocalControl, None);
                return;
            }
            msg = in_rx.recv() => {
                match msg {
                    Some(RawInputMessage::Bytes(bytes)) => {
                        owner.bytes_accepted += bytes.len() as u64;
                        let len = bytes.len() as u64;
                        if out_tx.send(bytes).await.is_err() {
                            owner.finish(
                                ConnectionEndKind::TransportFailure,
                                EndInitiator::LocalTransport,
                                Some("output closed".into()),
                            );
                            return;
                        }
                        owner.bytes_received += len;
                    }
                    Some(RawInputMessage::Finish) | None => {
                        owner.finish(
                            ConnectionEndKind::RemoteEof,
                            EndInitiator::LocalTransport,
                            None,
                        );
                        return;
                    }
                }
            }
        }
    }
}

async fn run_scripted_owner(
    connection_id: ConnectionId,
    control: Arc<ControlState>,
    mut in_rx: mpsc::Receiver<RawInputMessage>,
    out_tx: mpsc::Sender<Bytes>,
    end_tx: oneshot::Sender<crate::handles::ConnectionEnd>,
    chunks: Vec<Bytes>,
) {
    let mut owner = ConnectionOwner::new(connection_id, Arc::clone(&control), end_tx);
    for chunk in chunks {
        if control.cancel_requested() || control.terminate_requested() {
            break;
        }
        let len = chunk.len() as u64;
        if out_tx.send(chunk).await.is_err() {
            owner.finish(
                ConnectionEndKind::TransportFailure,
                EndInitiator::LocalTransport,
                Some("output closed".into()),
            );
            return;
        }
        owner.bytes_received += len;
    }
    // Drain input until finish/cancel while leaving output closed after script.
    drop(out_tx);
    loop {
        tokio::select! {
            biased;
            _ = wait_control(&control) => {
                let kind = if control.terminate_requested() {
                    ConnectionEndKind::Terminated
                } else {
                    ConnectionEndKind::Cancelled
                };
                owner.finish(kind, EndInitiator::LocalControl, None);
                return;
            }
            msg = in_rx.recv() => {
                match msg {
                    Some(RawInputMessage::Bytes(bytes)) => {
                        owner.bytes_accepted += bytes.len() as u64;
                    }
                    Some(RawInputMessage::Finish) | None => {
                        owner.finish(
                            ConnectionEndKind::RemoteEof,
                            EndInitiator::Remote,
                            None,
                        );
                        return;
                    }
                }
            }
        }
    }
}

async fn run_pair_owner(
    connection_id: ConnectionId,
    control: Arc<ControlState>,
    mut in_rx: mpsc::Receiver<RawInputMessage>,
    peer_tx: mpsc::Sender<Bytes>,
    _my_out_tx: mpsc::Sender<Bytes>,
    end_tx: oneshot::Sender<crate::handles::ConnectionEnd>,
) {
    let mut owner = ConnectionOwner::new(connection_id, Arc::clone(&control), end_tx);
    loop {
        tokio::select! {
            biased;
            _ = wait_control(&control) => {
                let kind = if control.terminate_requested() {
                    ConnectionEndKind::Terminated
                } else {
                    ConnectionEndKind::Cancelled
                };
                owner.finish(kind, EndInitiator::LocalControl, None);
                return;
            }
            msg = in_rx.recv() => {
                match msg {
                    Some(RawInputMessage::Bytes(bytes)) => {
                        owner.bytes_accepted += bytes.len() as u64;
                        if peer_tx.send(bytes).await.is_err() {
                            owner.finish(
                                ConnectionEndKind::RemoteEof,
                                EndInitiator::Remote,
                                None,
                            );
                            return;
                        }
                    }
                    Some(RawInputMessage::Finish) | None => {
                        owner.finish(
                            ConnectionEndKind::LocalShutdown,
                            EndInitiator::LocalTransport,
                            None,
                        );
                        return;
                    }
                }
            }
        }
    }
}

/// Drain input while producing no output; finish only on cancel/terminate (D-012).
async fn run_hang_owner(
    connection_id: ConnectionId,
    control: Arc<ControlState>,
    mut in_rx: mpsc::Receiver<RawInputMessage>,
    end_tx: oneshot::Sender<crate::handles::ConnectionEnd>,
) {
    let mut owner = ConnectionOwner::new(connection_id, Arc::clone(&control), end_tx);
    loop {
        tokio::select! {
            biased;
            _ = wait_control(&control) => {
                let kind = if control.terminate_requested() {
                    ConnectionEndKind::Terminated
                } else {
                    ConnectionEndKind::Cancelled
                };
                owner.finish(kind, EndInitiator::LocalControl, None);
                return;
            }
            msg = in_rx.recv() => {
                match msg {
                    Some(RawInputMessage::Bytes(bytes)) => {
                        owner.bytes_accepted += bytes.len() as u64;
                        // Deliberately do not emit on output.
                    }
                    Some(RawInputMessage::Finish) | None => {
                        // Stay open without EOF: hang until cancel/terminate only.
                        wait_control(&control).await;
                        let kind = if control.terminate_requested() {
                            ConnectionEndKind::Terminated
                        } else {
                            ConnectionEndKind::Cancelled
                        };
                        owner.finish(kind, EndInitiator::LocalControl, None);
                        return;
                    }
                }
            }
        }
    }
}

async fn wait_control(control: &ControlState) {
    loop {
        if control.cancel_requested() || control.terminate_requested() {
            return;
        }
        control.notify().notified().await;
    }
}

// Silence unused import warning path for ExternalSessionId in docs.
#[allow(dead_code)]
fn _types() {
    let _: Option<ExternalSessionId> = None;
}
