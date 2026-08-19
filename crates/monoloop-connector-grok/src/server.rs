//! Grok server connection, reader demux, and Connector trait bridge.

use crate::config::{GrokServerConfig, GrokSessionConfig, GrokSessionLoadConfig};
use crate::error::GrokConnectorError;
use crate::jsonrpc::{session_id_from_params, JsonRpcMessage, JsonRpcRequest, RpcId};
use crate::secret::SecretResolver;
use crate::session::{
    GrokSessionFactory, GrokSessionHandle, GrokSessionHealth, PendingGrokSession,
    SessionCompletion, SessionInner, SessionMap,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use monoloop_connector::{
    CancellationReason, ConnectionCompletionHandle, ConnectionControlHandle, ConnectionEnd,
    ConnectionEndKind, ConnectionId, ConnectionOwner, Connector, ConnectorDescriptor,
    ControlDisposition, ControlState, DialectBinding, DialectDescriptor, EndInitiator,
    OpenConnection, OpenedRawConnection, PendingRawConnection, RawInputHandle, RawInputMessage,
    RawOutputHandle, TerminationReason,
};
use monoloop_contracts::{ExternalSessionId, GrokSessionId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock};
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, warn};

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Factory for Grok Build server connections.
pub struct GrokConnector {
    descriptor: ConnectorDescriptor,
    secrets: Arc<dyn SecretResolver>,
}

impl GrokConnector {
    /// Create a connector with an injected secret resolver.
    pub fn new(secrets: Arc<dyn SecretResolver>) -> Self {
        Self {
            descriptor: ConnectorDescriptor::grok_build(),
            secrets,
        }
    }

    /// Connect to one Grok Build server. Returns immediately with a pending handle.
    pub fn connect(
        &self,
        config: GrokServerConfig,
    ) -> Result<PendingGrokServer, GrokConnectorError> {
        config.validate_endpoint_security()?;
        let secret = self.secrets.resolve(&config.authentication_secret_ref)?;
        let control_flag = Arc::new(ServerControlFlags::new());
        let control = GrokServerControl {
            flags: Arc::clone(&control_flag),
        };
        let (tx, rx) = oneshot::channel();
        let secrets_ok = true;
        let _ = secrets_ok;
        tokio::spawn(async move {
            let result = connect_server(config, secret, Arc::clone(&control_flag)).await;
            let _ = tx.send(result);
        });
        Ok(PendingGrokServer {
            opened: rx,
            control,
        })
    }
}

/// Bridge: treat `endpoint_ref` as `ws://host:port` and open a single-session raw connection
/// via session/new. Credential ref name is resolved through the same secret resolver.
///
/// Prefer [`GrokConnector::connect`] + session factory for multi-session use.
impl Connector for GrokConnector {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }

    fn begin_open(&self, request: OpenConnection) -> PendingRawConnection {
        let secrets = Arc::clone(&self.secrets);
        let connection_id = request.connection_id.clone();
        let control_state = ControlState::new();
        let control = ConnectionControlHandle::new(Arc::clone(&control_state));
        let control_open = control.clone();
        let opened = Box::pin(async move {
            open_as_raw_connection(secrets, request, control_open, control_state).await
        });
        PendingRawConnection {
            connection_id,
            control,
            opened,
        }
    }
}

async fn open_as_raw_connection(
    secrets: Arc<dyn SecretResolver>,
    request: OpenConnection,
    control: ConnectionControlHandle,
    control_state: Arc<ControlState>,
) -> Result<OpenedRawConnection, monoloop_contracts::ConnectorError> {
    use crate::secret::SecretRef;
    use url::Url;

    if let Some(err) = control.interrupt_error() {
        return Err(err.with_connection_id(request.connection_id.as_str()));
    }

    let endpoint = Url::parse(&request.endpoint_ref).map_err(|_| {
        monoloop_contracts::ConnectorError::configuration_invalid(
            "endpoint_ref must be a websocket URL",
        )
    })?;
    let cred = request.credential_ref.as_deref().ok_or_else(|| {
        monoloop_contracts::ConnectorError::new(
            monoloop_contracts::ConnectorErrorKind::CredentialUnavailable,
            "credential_ref required for grok connector",
        )
    })?;
    let secret_ref = SecretRef::new(cred);
    let mut config = GrokServerConfig {
        websocket_endpoint: endpoint,
        authentication_secret_ref: secret_ref,
        expected_acp_version: "1".into(),
        allow_non_loopback: false,
        limits: crate::config::GrokConnectorLimits {
            connect_deadline: request.limits.connect_deadline,
            ..Default::default()
        },
        raw_dump: None,
    };
    // Allow non-loopback only if explicitly... we keep fail-closed defaults.
    let _ = &mut config;

    let connector = GrokConnector::new(secrets);
    let pending = connector.connect(config).map_err(|e| e.into_connector())?;
    let server = timeout(request.limits.connect_deadline, pending.opened)
        .await
        .map_err(|_| {
            monoloop_contracts::ConnectorError::new(
                monoloop_contracts::ConnectorErrorKind::DeadlineExceeded,
                "connect deadline exceeded",
            )
        })?
        .map_err(|_| {
            monoloop_contracts::ConnectorError::connection_failed("server open channel dropped")
        })?
        .map_err(|e| e.into_connector())?;

    let session = if let Some(ref ext) = request.external_session_id {
        let pending = server
            .sessions
            .begin_load(
                GrokSessionId::new(ext.as_str()),
                GrokSessionLoadConfig::default(),
            )
            .map_err(|e| e.into_connector())?;
        timeout(request.limits.connect_deadline, pending.opened)
            .await
            .map_err(|_| {
                monoloop_contracts::ConnectorError::new(
                    monoloop_contracts::ConnectorErrorKind::DeadlineExceeded,
                    "session load deadline exceeded",
                )
            })?
            .map_err(|_| {
                monoloop_contracts::ConnectorError::session_failed("session load channel dropped")
            })?
            .map_err(|e| e.into_connector())?
    } else {
        // D-026: serialize CreationOnly MCP descriptor into provider session/new.
        let mut session_cfg = GrokSessionConfig::default();
        if let Some(mcp) = request
            .session_attachment
            .as_ref()
            .and_then(|a| a.initial_mcp.as_ref())
        {
            session_cfg.mcp_servers.push(serde_json::json!({
                "name": mcp.server_name,
                "type": "http",
                "url": mcp.expose_capability_url(),
            }));
        }
        let pending = server
            .sessions
            .begin_new(session_cfg)
            .map_err(|e| e.into_connector())?;
        timeout(request.limits.connect_deadline, pending.opened)
            .await
            .map_err(|_| {
                monoloop_contracts::ConnectorError::new(
                    monoloop_contracts::ConnectorErrorKind::DeadlineExceeded,
                    "session new deadline exceeded",
                )
            })?
            .map_err(|_| {
                monoloop_contracts::ConnectorError::session_failed("session new channel dropped")
            })?
            .map_err(|e| e.into_connector())?
    };

    // Bridge session output to RawOutput; input sends as session/prompt-shaped raw JSON-RPC params object.
    let (in_tx, mut in_rx) = mpsc::channel::<RawInputMessage>(32);
    let (end_tx, end_rx) = oneshot::channel();
    let session_id = session.session_id.clone();
    let input_handle = session.input.clone();
    let session_control = session.control.clone();
    let out = Arc::clone(&session.output);
    let cid = request.connection_id.clone();

    let input = RawInputHandle::new(
        cid.clone(),
        in_tx,
        Arc::clone(&control_state),
        request.limits.buffers.max_chunk_bytes,
    );

    // Forward control cancel to session.
    let ctrl_state = Arc::clone(&control_state);
    let sc = session_control.clone();
    tokio::spawn(async move {
        loop {
            if ctrl_state.cancel_requested() {
                sc.cancel(CancellationReason::CallerRequested);
                return;
            }
            if ctrl_state.terminate_requested() {
                sc.terminate(TerminationReason::CallerForced);
                return;
            }
            if ctrl_state.is_terminal() {
                return;
            }
            ctrl_state.notify().notified().await;
        }
    });

    let owner_control = Arc::clone(&control_state);
    let owner_cid = cid.clone();
    tokio::spawn(async move {
        let mut owner = ConnectionOwner::new(owner_cid, Arc::clone(&owner_control), end_tx);
        loop {
            tokio::select! {
                biased;
                _ = async {
                    loop {
                        if owner_control.cancel_requested() || owner_control.terminate_requested() {
                            return;
                        }
                        owner_control.notify().notified().await;
                    }
                } => {
                    let kind = if owner_control.terminate_requested() {
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
                            // Expect raw JSON object params for session/prompt, or full method envelope.
                            let params: serde_json::Value = match serde_json::from_slice(&bytes) {
                                Ok(v) => v,
                                Err(_) => {
                                    owner.finish(
                                        ConnectionEndKind::TransportFailure,
                                        EndInitiator::LocalTransport,
                                        Some("input is not JSON".into()),
                                    );
                                    return;
                                }
                            };
                            let (method, params) = if let Some(m) = params.get("method").and_then(|m| m.as_str()) {
                                let p = params.get("params").cloned().unwrap_or(serde_json::Value::Null);
                                (m.to_string(), p)
                            } else {
                                ("session/prompt".into(), params)
                            };
                            let pending = match input_handle.begin_send(crate::session::EncodedAcpSessionMessage {
                                method,
                                params,
                            }) {
                                Ok(p) => p,
                                Err(e) => {
                                    owner.finish(
                                        ConnectionEndKind::TransportFailure,
                                        EndInitiator::LocalTransport,
                                        Some(e.to_string()),
                                    );
                                    return;
                                }
                            };
                            match pending.response.await {
                                Ok(Ok(_)) => {}
                                Ok(Err(e)) => {
                                    owner.finish(
                                        ConnectionEndKind::TransportFailure,
                                        EndInitiator::LocalTransport,
                                        Some(e.to_string()),
                                    );
                                    return;
                                }
                                Err(_) => {
                                    owner.finish(
                                        ConnectionEndKind::TransportFailure,
                                        EndInitiator::LocalTransport,
                                        Some("exchange dropped".into()),
                                    );
                                    return;
                                }
                            }
                        }
                        Some(RawInputMessage::Finish) | None => {
                            session_control.cancel(CancellationReason::CallerRequested);
                            owner.finish(ConnectionEndKind::LocalShutdown, EndInitiator::LocalTransport, None);
                            return;
                        }
                    }
                }
            }
        }
    });

    Ok(OpenedRawConnection {
        connection_id: cid,
        external_session_id: Some(ExternalSessionId::new(session_id.as_str())),
        dialect: DialectBinding::negotiated(DialectDescriptor::acp_json_rpc("1")),
        input,
        output: out,
        control,
        completion: ConnectionCompletionHandle::new(end_rx),
        owner_work: None,
    })
}

/// Pending server connection.
pub struct PendingGrokServer {
    /// Completes with server handle.
    pub opened: oneshot::Receiver<Result<GrokServerHandle, GrokConnectorError>>,
    /// Server-level control.
    pub control: GrokServerControl,
}

/// Connected Grok server handle.
pub struct GrokServerHandle {
    /// Session factory (many sessions).
    pub sessions: GrokSessionFactory,
    /// Server control.
    pub control: GrokServerControl,
    /// Health.
    pub health: GrokServerHealth,
    /// Server completion (connection-wide).
    pub completion: GrokServerCompletion,
    /// Opt-in raw dump collector (shared with config if enabled).
    pub raw_dump: Option<std::sync::Arc<crate::raw_dump::RawDumpCollector>>,
    #[allow(dead_code)]
    inner: Arc<ServerInner>,
}

/// Server health (content-free).
#[derive(Clone, Debug, Default)]
pub struct GrokServerHealth {
    /// Active sessions.
    pub active_sessions: Arc<AtomicU64>,
    /// RPC calls completed.
    pub rpc_completed: Arc<AtomicU64>,
}

/// Server control.
#[derive(Clone)]
pub struct GrokServerControl {
    flags: Arc<ServerControlFlags>,
}

impl GrokServerControl {
    /// Cancel the server connection (affects all sessions on this connection).
    pub fn cancel(&self, _reason: CancellationReason) -> ControlDisposition {
        self.flags.request_cancel()
    }

    /// Terminate the server connection.
    pub fn terminate(&self, _reason: TerminationReason) -> ControlDisposition {
        self.flags.request_terminate()
    }
}

/// Server connection completion.
pub struct GrokServerCompletion {
    rx: Mutex<Option<oneshot::Receiver<ConnectionEnd>>>,
}

impl GrokServerCompletion {
    /// Wait for server connection terminal.
    pub async fn wait(self) -> ConnectionEnd {
        let mut guard = self.rx.lock().await;
        let rx = guard.take().expect("GrokServerCompletion polled twice");
        rx.await.unwrap_or(ConnectionEnd {
            connection_id: ConnectionId::new("grok-server"),
            kind: ConnectionEndKind::TransportFailure,
            initiated_by: EndInitiator::LocalTransport,
            bytes_accepted: 0,
            bytes_received: 0,
            safe_transport_error: Some("server completion dropped".into()),
        })
    }
}

struct ServerControlFlags {
    cancel: AtomicBool,
    terminate: AtomicBool,
    terminal: AtomicBool,
    notify: Notify,
}

impl ServerControlFlags {
    fn new() -> Self {
        Self {
            cancel: AtomicBool::new(false),
            terminate: AtomicBool::new(false),
            terminal: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn request_cancel(&self) -> ControlDisposition {
        if self.terminal.load(Ordering::SeqCst) {
            return ControlDisposition::AlreadyTerminal;
        }
        if self.cancel.swap(true, Ordering::SeqCst) {
            return ControlDisposition::AlreadyRequested;
        }
        self.notify.notify_waiters();
        ControlDisposition::Accepted
    }

    fn request_terminate(&self) -> ControlDisposition {
        if self.terminal.load(Ordering::SeqCst) {
            return ControlDisposition::AlreadyTerminal;
        }
        if self.terminate.swap(true, Ordering::SeqCst) {
            return ControlDisposition::AlreadyRequested;
        }
        self.cancel.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
        ControlDisposition::Accepted
    }

    fn mark_terminal(&self) {
        self.terminal.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst) || self.terminate.load(Ordering::SeqCst)
    }
}

/// Internal server state shared by sessions and reader.
pub(crate) struct ServerInner {
    write_tx: mpsc::Sender<WriteCmd>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<serde_json::Value, GrokConnectorError>>>>,
    next_id: AtomicU64,
    pub(crate) sessions: SessionMap,
    pub(crate) limits: crate::config::GrokConnectorLimits,
    health: GrokServerHealth,
    control: Arc<ServerControlFlags>,
    closed: AtomicBool,
    /// Opt-in exact inbound wire dump.
    raw_dump: Option<std::sync::Arc<crate::raw_dump::RawDumpCollector>>,
}

enum WriteCmd {
    Text(Bytes),
}

impl ServerInner {
    pub(crate) fn detach_session(&self, session_id: &str) {
        if let Ok(mut map) = self.sessions.try_write() {
            map.remove(session_id);
            self.health
                .active_sessions
                .store(map.len() as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn begin_session_new(
        self: &Arc<Self>,
        config: GrokSessionConfig,
    ) -> Result<PendingGrokSession, GrokConnectorError> {
        self.ensure_capacity()?;
        let (tx, rx) = oneshot::channel();
        // Control for pending uses a temporary session id placeholder.
        let placeholder = Arc::new(SessionInner {
            session_id: GrokSessionId::new("pending"),
            connection_id: ConnectionId::generate(),
            server: Arc::clone(self),
            out_tx: mpsc::channel(1).0,
            cancelled: AtomicBool::new(false),
            terminated: AtomicBool::new(false),
            detached: AtomicBool::new(false),
            notify: Notify::new(),
            end_tx: Mutex::new(None),
            health: GrokSessionHealth::default(),
            prompt_lock: tokio::sync::Mutex::new(()),
            request_deadline: self.limits.request_deadline,
        });
        let control = placeholder.control_handle();
        let server = Arc::clone(self);
        tokio::spawn(async move {
            let result = server.create_session(config).await;
            let _ = tx.send(result);
        });
        Ok(PendingGrokSession {
            opened: rx,
            control,
        })
    }

    pub(crate) fn begin_session_load(
        self: &Arc<Self>,
        session_id: GrokSessionId,
        config: GrokSessionLoadConfig,
    ) -> Result<PendingGrokSession, GrokConnectorError> {
        self.ensure_capacity()?;
        let (tx, rx) = oneshot::channel();
        let placeholder = Arc::new(SessionInner {
            session_id: session_id.clone(),
            connection_id: ConnectionId::generate(),
            server: Arc::clone(self),
            out_tx: mpsc::channel(1).0,
            cancelled: AtomicBool::new(false),
            terminated: AtomicBool::new(false),
            detached: AtomicBool::new(false),
            notify: Notify::new(),
            end_tx: Mutex::new(None),
            health: GrokSessionHealth::default(),
            prompt_lock: tokio::sync::Mutex::new(()),
            request_deadline: self.limits.request_deadline,
        });
        let control = placeholder.control_handle();
        let server = Arc::clone(self);
        tokio::spawn(async move {
            let result = server.load_session(session_id, config).await;
            let _ = tx.send(result);
        });
        Ok(PendingGrokSession {
            opened: rx,
            control,
        })
    }

    fn ensure_capacity(&self) -> Result<(), GrokConnectorError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(GrokConnectorError::connection("server connection closed"));
        }
        // Approximate; precise count under write lock at insert.
        Ok(())
    }

    async fn create_session(
        self: &Arc<Self>,
        config: GrokSessionConfig,
    ) -> Result<GrokSessionHandle, GrokConnectorError> {
        let result = self
            .rpc_call(
                "session/new",
                Some(config.to_params()),
                self.limits.request_deadline,
            )
            .await?;
        let session_id = result
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| GrokConnectorError::protocol("session/new missing sessionId"))?
            .to_string();
        self.admit_session(GrokSessionId::new(session_id)).await
    }

    async fn load_session(
        self: &Arc<Self>,
        session_id: GrokSessionId,
        config: GrokSessionLoadConfig,
    ) -> Result<GrokSessionHandle, GrokConnectorError> {
        let _result = self
            .rpc_call(
                "session/load",
                Some(config.to_params(session_id.as_str())),
                self.limits.request_deadline,
            )
            .await?;
        // Grok's id remains authoritative; we never invent a replacement.
        self.admit_session(session_id).await
    }

    async fn admit_session(
        self: &Arc<Self>,
        session_id: GrokSessionId,
    ) -> Result<GrokSessionHandle, GrokConnectorError> {
        let mut map = self.sessions.write().await;
        if map.len() >= self.limits.max_sessions {
            return Err(GrokConnectorError::resource("max_sessions exceeded"));
        }
        if map.contains_key(session_id.as_str()) {
            return Err(GrokConnectorError::session(
                "session already attached locally",
            ));
        }
        let (out_tx, out_rx) = mpsc::channel(self.limits.max_queued_inbound_per_session.max(1));
        let (end_tx, end_rx) = oneshot::channel();
        let connection_id = ConnectionId::generate();
        // RawOutputHandle needs ControlState — use a dedicated one for session output.
        let out_control = ControlState::new();
        let output = Arc::new(RawOutputHandle::new(
            connection_id.clone(),
            out_rx,
            out_control,
        ));
        let inner = Arc::new(SessionInner {
            session_id: session_id.clone(),
            connection_id: connection_id.clone(),
            server: Arc::clone(self),
            out_tx,
            cancelled: AtomicBool::new(false),
            terminated: AtomicBool::new(false),
            detached: AtomicBool::new(false),
            notify: Notify::new(),
            end_tx: Mutex::new(Some(end_tx)),
            health: GrokSessionHealth::default(),
            prompt_lock: tokio::sync::Mutex::new(()),
            request_deadline: self.limits.request_deadline,
        });
        map.insert(session_id.as_str().to_string(), Arc::clone(&inner));
        self.health
            .active_sessions
            .store(map.len() as u64, Ordering::Relaxed);
        Ok(GrokSessionHandle {
            session_id,
            connection_id,
            input: inner.input_handle(),
            output,
            control: inner.control_handle(),
            health: inner.health.clone(),
            completion: SessionCompletion::new(end_rx),
        })
    }

    pub(crate) async fn rpc_call(
        self: &Arc<Self>,
        method: impl Into<String>,
        params: Option<serde_json::Value>,
        deadline: Duration,
    ) -> Result<serde_json::Value, GrokConnectorError> {
        if self.closed.load(Ordering::SeqCst) || self.control.is_cancelled() {
            return Err(GrokConnectorError::cancelled());
        }
        {
            let pending = self.pending.lock().await;
            if pending.len() >= self.limits.max_pending_rpc {
                return Err(GrokConnectorError::resource("max_pending_rpc exceeded"));
            }
        }
        let id_num = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            pending.insert(id_num, tx);
        }
        let req = JsonRpcRequest::new(RpcId::number(id_num), method, params);
        let bytes = req.to_bytes(self.limits.max_message_bytes)?;
        self.write_tx
            .send(WriteCmd::Text(bytes))
            .await
            .map_err(|_| GrokConnectorError::connection("write path closed"))?;

        match timeout(deadline, rx).await {
            Ok(Ok(result)) => {
                self.health.rpc_completed.fetch_add(1, Ordering::Relaxed);
                result
            }
            Ok(Err(_)) => Err(GrokConnectorError::connection("rpc waiter dropped")),
            Err(_) => {
                let mut pending = self.pending.lock().await;
                pending.remove(&id_num);
                Err(GrokConnectorError::from_connector(
                    monoloop_contracts::ConnectorError::new(
                        monoloop_contracts::ConnectorErrorKind::DeadlineExceeded,
                        "rpc request deadline exceeded",
                    ),
                ))
            }
        }
    }
}

async fn connect_server(
    config: GrokServerConfig,
    secret: String,
    control: Arc<ServerControlFlags>,
) -> Result<GrokServerHandle, GrokConnectorError> {
    if control.is_cancelled() {
        return Err(GrokConnectorError::cancelled());
    }

    let mut url = config.websocket_endpoint.clone();
    // Auth: query token + headers (Grok agent serve accepts both styles).
    {
        let mut pairs = url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect::<Vec<_>>();
        pairs.retain(|(k, _)| k != "token" && k != "secret");
        pairs.push(("token".into(), secret.clone()));
        let query = pairs
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        url.set_query(Some(&query));
    }

    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| GrokConnectorError::configuration("invalid websocket endpoint for request"))?;
    if let Ok(val) = secret.parse() {
        request.headers_mut().insert("X-Secret-Key", val);
    }
    if let Ok(val) = format!("Bearer {secret}").parse() {
        request.headers_mut().insert("Authorization", val);
    }
    // Drop secret from stack ASAP — do not log url/request (contains secret).
    drop(secret);
    let _ = config.websocket_endpoint.host_str();

    let connect = connect_async(request);
    let (ws, _resp) = timeout(config.limits.connect_deadline, connect)
        .await
        .map_err(|_| {
            GrokConnectorError::from_connector(monoloop_contracts::ConnectorError::new(
                monoloop_contracts::ConnectorErrorKind::DeadlineExceeded,
                "websocket connect deadline exceeded",
            ))
        })?
        .map_err(|e| GrokConnectorError::connection(format!("websocket connect failed: {e}")))?;

    let (write_tx, write_rx) = mpsc::channel::<WriteCmd>(64);
    let (end_tx, end_rx) = oneshot::channel();
    let health = GrokServerHealth::default();
    let raw_dump = config.raw_dump.clone();
    let inner = Arc::new(ServerInner {
        write_tx: write_tx.clone(),
        pending: Mutex::new(HashMap::new()),
        next_id: AtomicU64::new(1),
        sessions: RwLock::new(HashMap::new()),
        limits: config.limits.clone(),
        health: health.clone(),
        control: Arc::clone(&control),
        closed: AtomicBool::new(false),
        raw_dump,
    });

    tokio::spawn(run_connection(
        ws,
        write_rx,
        Arc::clone(&inner),
        end_tx,
        config.limits.max_message_bytes,
        config.expected_acp_version.clone(),
    ));

    // initialize handshake — ACP uses numeric protocolVersion (e.g. 1).
    let protocol_version: serde_json::Value = config
        .expected_acp_version
        .parse::<u64>()
        .map(serde_json::Value::from)
        .unwrap_or_else(|_| serde_json::Value::String(config.expected_acp_version.clone()));
    let init_params = serde_json::json!({
        "protocolVersion": protocol_version,
        "clientCapabilities": {
            "fs": { "readTextFile": true, "writeTextFile": true },
            "terminal": true
        },
        "clientInfo": {
            "name": "monoloop-connector-grok",
            "version": env!("CARGO_PKG_VERSION")
        }
    });
    let _init_result = inner
        .rpc_call(
            "initialize",
            Some(init_params),
            config.limits.request_deadline,
        )
        .await?;

    let control_handle = GrokServerControl {
        flags: Arc::clone(&control),
    };
    Ok(GrokServerHandle {
        sessions: GrokSessionFactory {
            inner: Arc::clone(&inner),
        },
        control: control_handle,
        health,
        completion: GrokServerCompletion {
            rx: Mutex::new(Some(end_rx)),
        },
        raw_dump: inner.raw_dump.clone(),
        inner,
    })
}

async fn run_connection(
    ws: WsStream,
    mut write_rx: mpsc::Receiver<WriteCmd>,
    inner: Arc<ServerInner>,
    end_tx: oneshot::Sender<ConnectionEnd>,
    max_message_bytes: usize,
    _expected_version: String,
) {
    let (mut sink, mut stream) = ws.split();
    let mut owner = ConnectionOwner::new(
        ConnectionId::new("grok-server"),
        // Dummy control state for ConnectionOwner API reuse
        ControlState::new(),
        end_tx,
    );

    // Writer task coordination: single writer.
    loop {
        tokio::select! {
            biased;
            _ = async {
                loop {
                    if inner.control.is_cancelled() {
                        return;
                    }
                    inner.control.notify.notified().await;
                }
            } => {
                let _ = sink.send(Message::Close(None)).await;
                fail_all_pending(&inner, GrokConnectorError::cancelled()).await;
                detach_all_sessions(&inner, ConnectionEndKind::Cancelled).await;
                inner.closed.store(true, Ordering::SeqCst);
                inner.control.mark_terminal();
                let kind = if inner.control.terminate.load(Ordering::SeqCst) {
                    ConnectionEndKind::Terminated
                } else {
                    ConnectionEndKind::Cancelled
                };
                owner.finish(kind, EndInitiator::LocalControl, None);
                return;
            }
            cmd = write_rx.recv() => {
                match cmd {
                    Some(WriteCmd::Text(bytes)) => {
                        if bytes.len() > max_message_bytes {
                            warn!("dropping oversized outbound message");
                            continue;
                        }
                        owner.bytes_accepted += bytes.len() as u64;
                        let text = match String::from_utf8(bytes.to_vec()) {
                            Ok(t) => t,
                            Err(_) => {
                                owner.finish(
                                    ConnectionEndKind::TransportFailure,
                                    EndInitiator::LocalTransport,
                                    Some("outbound not utf-8".into()),
                                );
                                inner.closed.store(true, Ordering::SeqCst);
                                inner.control.mark_terminal();
                                return;
                            }
                        };
                        if sink.send(Message::Text(text.into())).await.is_err() {
                            fail_all_pending(&inner, GrokConnectorError::connection("write failed")).await;
                            detach_all_sessions(&inner, ConnectionEndKind::TransportFailure).await;
                            inner.closed.store(true, Ordering::SeqCst);
                            inner.control.mark_terminal();
                            owner.finish(
                                ConnectionEndKind::TransportFailure,
                                EndInitiator::LocalTransport,
                                Some("write failed".into()),
                            );
                            return;
                        }
                    }
                    None => {
                        let _ = sink.send(Message::Close(None)).await;
                        fail_all_pending(&inner, GrokConnectorError::connection("writer closed")).await;
                        detach_all_sessions(&inner, ConnectionEndKind::LocalShutdown).await;
                        inner.closed.store(true, Ordering::SeqCst);
                        inner.control.mark_terminal();
                        owner.finish(ConnectionEndKind::LocalShutdown, EndInitiator::LocalTransport, None);
                        return;
                    }
                }
            }
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let bytes = Bytes::from(text.as_bytes().to_vec());
                        if bytes.len() > max_message_bytes {
                            debug!("inbound message exceeds max_message_bytes");
                            continue;
                        }
                        // Exact wire dump of what Grok sent (opt-in).
                        if let Some(dump) = inner.raw_dump.as_ref() {
                            dump.record_inbound(&bytes);
                        }
                        owner.bytes_received += bytes.len() as u64;
                        if let Err(e) = handle_inbound(&inner, bytes).await {
                            debug!(error = %e, "inbound handling error");
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        if bin.len() > max_message_bytes {
                            continue;
                        }
                        if let Some(dump) = inner.raw_dump.as_ref() {
                            dump.record_inbound(&bin);
                        }
                        owner.bytes_received += bin.len() as u64;
                        let bytes = Bytes::from(bin.to_vec());
                        if let Err(e) = handle_inbound(&inner, bytes).await {
                            debug!(error = %e, "inbound handling error");
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = sink.send(Message::Pong(p)).await;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => {
                        fail_all_pending(&inner, GrokConnectorError::connection("remote closed")).await;
                        detach_all_sessions(&inner, ConnectionEndKind::RemoteEof).await;
                        inner.closed.store(true, Ordering::SeqCst);
                        inner.control.mark_terminal();
                        owner.finish(ConnectionEndKind::RemoteEof, EndInitiator::Remote, None);
                        return;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(_)) => {
                        fail_all_pending(&inner, GrokConnectorError::connection("read failed")).await;
                        detach_all_sessions(&inner, ConnectionEndKind::TransportFailure).await;
                        inner.closed.store(true, Ordering::SeqCst);
                        inner.control.mark_terminal();
                        owner.finish(
                            ConnectionEndKind::TransportFailure,
                            EndInitiator::LocalTransport,
                            Some("read failed".into()),
                        );
                        return;
                    }
                }
            }
        }
    }
}

async fn handle_inbound(inner: &Arc<ServerInner>, bytes: Bytes) -> Result<(), GrokConnectorError> {
    let msg = JsonRpcMessage::parse(&bytes)?;
    if msg.is_response() {
        let id = msg
            .id
            .ok_or_else(|| GrokConnectorError::protocol("response missing id"))?;
        let id_num = match id {
            RpcId::Number(n) => n,
            RpcId::String(s) => s
                .parse()
                .map_err(|_| GrokConnectorError::protocol("string rpc id not numeric"))?,
        };
        let waiter = {
            let mut pending = inner.pending.lock().await;
            pending.remove(&id_num)
        };
        if let Some(tx) = waiter {
            if let Some(err) = msg.error {
                let _ = tx.send(Err(GrokConnectorError::protocol(format!(
                    "rpc error {}: {}",
                    err.code,
                    // message only; do not include data payloads
                    truncate_safe(&err.message, 200)
                ))));
            } else {
                let _ = tx.send(Ok(msg.result.unwrap_or(serde_json::Value::Null)));
            }
        } else {
            return Err(GrokConnectorError::protocol("response for unknown rpc id"));
        }
        return Ok(());
    }

    if msg.is_notification_or_request() {
        // Server → client request (has method + id): must answer or the agent hangs.
        // Grok Build uses this for fs/* when we advertise clientCapabilities.fs.
        if msg.id.is_some() && msg.method.is_some() {
            return answer_client_request(inner, &msg).await;
        }

        // Notification (method, no id): demux by sessionId into the session stream.
        if let Some(sid) = session_id_from_params(&msg.params) {
            let map = inner.sessions.read().await;
            if let Some(session) = map.get(&sid) {
                session.push_inbound(bytes).await?;
                return Ok(());
            }
            return Err(GrokConnectorError::protocol(
                "notification for unknown sessionId",
            ));
        }
        // Unscoped notification — ignore (e.g. global capability noise).
        return Ok(());
    }

    Err(GrokConnectorError::protocol(
        "unrecognized json-rpc message",
    ))
}

/// Max bytes returned for a single `fs/read_text_file` (fail closed beyond).
const MAX_CLIENT_FS_READ_BYTES: usize = 1024 * 1024;

/// Answer ACP client methods Grok invokes over the reverse channel.
async fn answer_client_request(
    inner: &Arc<ServerInner>,
    msg: &JsonRpcMessage,
) -> Result<(), GrokConnectorError> {
    let id = msg
        .id
        .clone()
        .ok_or_else(|| GrokConnectorError::protocol("client request missing id"))?;
    let method = msg.method.as_deref().unwrap_or("");
    let params = msg.params.clone().unwrap_or(serde_json::Value::Null);

    let response_bytes = match method {
        "fs/read_text_file" => match client_fs_read(&params).await {
            Ok(content) => json_rpc_result(id, serde_json::json!({ "content": content }))?,
            Err((code, message)) => json_rpc_error(id, code, message)?,
        },
        "fs/write_text_file" => match client_fs_write(&params).await {
            Ok(()) => json_rpc_result(id, serde_json::json!({}))?,
            Err((code, message)) => json_rpc_error(id, code, message)?,
        },
        other => json_rpc_error(
            id,
            -32601,
            format!("method not implemented by monoloop-connector-grok: {other}"),
        )?,
    };

    if response_bytes.len() > inner.limits.max_message_bytes {
        return Err(GrokConnectorError::resource(
            "client response exceeds max_message_bytes",
        ));
    }
    inner
        .write_tx
        .send(WriteCmd::Text(response_bytes))
        .await
        .map_err(|_| GrokConnectorError::connection("write path closed"))?;
    Ok(())
}

async fn client_fs_read(params: &serde_json::Value) -> Result<String, (i64, String)> {
    let path = params
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or((-32602, "fs/read_text_file missing path".into()))?;
    validate_client_fs_path(path)?;

    let data = tokio::fs::read(path)
        .await
        .map_err(|e| (-32000, format!("read failed: {e}")))?;
    if data.len() > MAX_CLIENT_FS_READ_BYTES {
        return Err((
            -32000,
            format!("file exceeds max read size ({MAX_CLIENT_FS_READ_BYTES} bytes)"),
        ));
    }
    let mut text = String::from_utf8_lossy(&data).into_owned();

    // Optional line window (1-based), as used by some ACP clients.
    let line = params.get("line").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
    let limit = params.get("limit").and_then(|v| v.as_u64());
    if line > 1 || limit.is_some() {
        let lines: Vec<&str> = text.lines().collect();
        let start = line.saturating_sub(1).min(lines.len());
        let end = match limit {
            Some(n) => (start + n as usize).min(lines.len()),
            None => lines.len(),
        };
        text = lines[start..end].join("\n");
        if end > start && text.as_bytes().last() != Some(&b'\n') && data.ends_with(b"\n") {
            // keep simple; trailing newline optional
        }
    }
    Ok(text)
}

async fn client_fs_write(params: &serde_json::Value) -> Result<(), (i64, String)> {
    let path = params
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or((-32602, "fs/write_text_file missing path".into()))?;
    validate_client_fs_path(path)?;
    let content = params
        .get("content")
        .or_else(|| params.get("text"))
        .and_then(|v| v.as_str())
        .ok_or((-32602, "fs/write_text_file missing content".into()))?;
    if content.len() > MAX_CLIENT_FS_READ_BYTES {
        return Err((-32000, "content exceeds max write size".into()));
    }
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| (-32000, format!("create parent dirs: {e}")))?;
        }
    }
    tokio::fs::write(path, content.as_bytes())
        .await
        .map_err(|e| (-32000, format!("write failed: {e}")))?;
    Ok(())
}

fn validate_client_fs_path(path: &str) -> Result<(), (i64, String)> {
    if path.is_empty() || path.contains('\0') {
        return Err((-32602, "invalid path".into()));
    }
    let p = std::path::Path::new(path);
    if !p.is_absolute() {
        return Err((-32602, "path must be absolute".into()));
    }
    Ok(())
}

fn json_rpc_result(
    id: RpcId,
    result: serde_json::Value,
) -> Result<bytes::Bytes, GrokConnectorError> {
    let v = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    });
    let raw = serde_json::to_vec(&v)
        .map_err(|e| GrokConnectorError::protocol(format!("serialize result: {e}")))?;
    Ok(bytes::Bytes::from(raw))
}

fn json_rpc_error(
    id: RpcId,
    code: i64,
    message: impl Into<String>,
) -> Result<bytes::Bytes, GrokConnectorError> {
    let v = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message.into() },
    });
    let raw = serde_json::to_vec(&v)
        .map_err(|e| GrokConnectorError::protocol(format!("serialize error: {e}")))?;
    Ok(bytes::Bytes::from(raw))
}

async fn fail_all_pending(inner: &ServerInner, err: GrokConnectorError) {
    let mut pending = inner.pending.lock().await;
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err(err.clone()));
    }
}

async fn detach_all_sessions(inner: &ServerInner, kind: ConnectionEndKind) {
    let mut map = inner.sessions.write().await;
    for (_, session) in map.drain() {
        session.finish(kind, Some("server connection ended".into()));
    }
    inner.health.active_sessions.store(0, Ordering::Relaxed);
}

fn truncate_safe(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}
