//! Generic streaming HTTP Connector (reqwest + Rustls).
//!
//! Transport only: no OpenAI/SSE/JSON interpretation. SendAndFinish collects
//! the complete request body, POSTs it, and streams response body chunks.

use crate::control::{ConnectionControlHandle, ControlState};
use crate::credential::{CredentialResolver, ResolvedCredential};
use crate::descriptor::ConnectorDescriptor;
use crate::handles::{
    ConnectionCompletionHandle, ConnectionEndKind, ConnectionOwner, EndInitiator, RawInputHandle,
    RawInputMessage, RawOutputHandle,
};
use crate::instance::{
    ConnectorBuildError, ConnectorFactory, ConnectorInstance, ConnectorInstanceId,
};
use crate::open::{ConnectionOwnerWork, OpenConnection, OpenedRawConnection, PendingRawConnection};
use crate::session::validate_open_attachment_owner;
use crate::traits::Connector;
use bytes::Bytes;
use futures_util::StreamExt;
use monoloop_contracts::{ConnectionId, ConnectorError, DialectBinding, DialectDescriptor};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

/// Configuration for [`StreamingHttpConnector`] (no secrets).
#[derive(Clone, Debug)]
pub struct StreamingHttpConfig {
    /// HTTP method (default POST).
    pub method: HttpMethod,
    /// Extra request headers (name, value). Values must not contain secrets.
    pub headers: Vec<(String, String)>,
    /// DNS/connect timeout for the shared client.
    pub connect_timeout: Duration,
    /// Overall deadline for request+response after body is ready.
    pub request_timeout: Duration,
    /// Maximum idle time between response body chunks.
    pub idle_timeout: Duration,
    /// Maximum request body bytes.
    pub max_request_bytes: usize,
    /// Maximum total response body bytes.
    pub max_response_bytes: usize,
    /// Maximum individual response chunk forwarded to RawOutputHandle.
    pub max_chunk_bytes: usize,
    /// Dialect stamped on opened connections (provider-neutral stamp only).
    pub dialect: DialectBinding,
    /// Optional HTTP proxy URL (no credentials in URL).
    pub proxy_url: Option<String>,
    /// When true, only `https` endpoints are accepted.
    pub require_https: bool,
}

impl Default for StreamingHttpConfig {
    fn default() -> Self {
        Self {
            method: HttpMethod::Post,
            headers: vec![("content-type".into(), "application/json".into())],
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            idle_timeout: Duration::from_secs(60),
            max_request_bytes: 4 * 1024 * 1024,
            max_response_bytes: 16 * 1024 * 1024,
            max_chunk_bytes: 256 * 1024,
            dialect: DialectBinding::fixed(DialectDescriptor::test_raw()),
            proxy_url: None,
            require_https: false,
        }
    }
}

/// Allowed HTTP methods for the streaming connector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpMethod {
    /// POST (typical chat completions).
    Post,
    /// PUT.
    Put,
}

impl HttpMethod {
    fn as_reqwest(self) -> reqwest::Method {
        match self {
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
        }
    }
}

/// Shared reqwest client + credential resolver HTTP Connector.
pub struct StreamingHttpConnector {
    descriptor: ConnectorDescriptor,
    config: StreamingHttpConfig,
    client: reqwest::Client,
    credentials: Arc<dyn CredentialResolver>,
    instance_id: ConnectorInstanceId,
}

impl fmt::Debug for StreamingHttpConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingHttpConnector")
            .field("descriptor", &self.descriptor)
            .field("config", &self.config)
            .field("credentials", &"<injected>")
            .field("instance_id", &self.instance_id)
            .finish()
    }
}

impl StreamingHttpConnector {
    /// Build a connector with an injected credential resolver.
    pub fn try_new(
        config: StreamingHttpConfig,
        credentials: Arc<dyn CredentialResolver>,
    ) -> Result<Self, ConnectorBuildError> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .pool_max_idle_per_host(8)
            .http2_adaptive_window(true);
        if let Some(ref proxy) = config.proxy_url {
            let p = reqwest::Proxy::all(proxy)
                .map_err(|_| ConnectorBuildError::ConfigurationInvalid("invalid proxy_url"))?;
            builder = builder.proxy(p);
        }
        let client = builder
            .build()
            .map_err(|_| ConnectorBuildError::ResourceUnavailable("http client build failed"))?;

        Ok(Self {
            descriptor: ConnectorDescriptor {
                connector_kind: crate::descriptor::ConnectorKind::LlmHttp,
                implementation_id: "monoloop.streaming_http".into(),
                implementation_version: env!("CARGO_PKG_VERSION").into(),
                transport_kind: "http_body".into(),
                supported_dialects: vec!["http/body".into()],
                raw_boundary: crate::descriptor::RawBoundary::HttpBody,
                control_capabilities: crate::descriptor::ControlCapabilities::default(),
            },
            config,
            client,
            credentials,
            instance_id: ConnectorInstanceId::generate(),
        })
    }

    /// Borrow instance id (for matched factories).
    pub fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }
}

impl Connector for StreamingHttpConnector {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }

    fn begin_open(&self, request: OpenConnection) -> PendingRawConnection {
        let control_state = ControlState::new();
        let control = ConnectionControlHandle::new(Arc::clone(&control_state));
        let connection_id = request.connection_id.clone();
        let client = self.client.clone();
        let config = self.config.clone();
        let credentials = Arc::clone(&self.credentials);
        let control_for_open = control.clone();
        let control_state_for_open = Arc::clone(&control_state);
        let instance_id = self.instance_id.clone();

        // D-051: open I/O + HTTP owner run inside ConnectorOwner before `opened` is polled.
        PendingRawConnection::open_owned(connection_id, control, async move {
            open_http(
                request,
                client,
                config,
                credentials,
                control_for_open,
                control_state_for_open,
                instance_id,
            )
            .await
        })
    }
}

/// Factory producing [`StreamingHttpConnector`] instances (no SessionAdapter).
pub struct StreamingHttpConnectorFactory {
    config: StreamingHttpConfig,
    credentials: Arc<dyn CredentialResolver>,
}

impl StreamingHttpConnectorFactory {
    /// Construct a factory.
    pub fn new(config: StreamingHttpConfig, credentials: Arc<dyn CredentialResolver>) -> Self {
        Self {
            config,
            credentials,
        }
    }
}

impl fmt::Debug for StreamingHttpConnectorFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingHttpConnectorFactory")
            .field("config", &self.config)
            .field("credentials", &"<injected>")
            .finish()
    }
}

impl ConnectorFactory for StreamingHttpConnectorFactory {
    fn create(&self) -> Result<ConnectorInstance, ConnectorBuildError> {
        let connector =
            StreamingHttpConnector::try_new(self.config.clone(), Arc::clone(&self.credentials))?;
        let id = connector.instance_id().clone();
        Ok(ConnectorInstance::new(id, Arc::new(connector), None))
    }
}

/// Validate `endpoint_ref` as an absolute http(s) URL without userinfo.
pub fn validate_endpoint_url(
    endpoint_ref: &str,
    require_https: bool,
) -> Result<reqwest::Url, ConnectorError> {
    if endpoint_ref.is_empty() || endpoint_ref.len() > 2048 {
        return Err(ConnectorError::configuration_invalid(
            "endpoint_ref empty or too long",
        ));
    }
    if endpoint_ref.chars().any(|c| c.is_control()) {
        return Err(ConnectorError::configuration_invalid(
            "endpoint_ref contains control characters",
        ));
    }
    let url = reqwest::Url::parse(endpoint_ref).map_err(|_| {
        ConnectorError::configuration_invalid("endpoint_ref is not a valid absolute URL")
    })?;
    match url.scheme() {
        "https" => {}
        "http" if !require_https => {}
        "http" => {
            return Err(ConnectorError::configuration_invalid(
                "https required for endpoint_ref",
            ));
        }
        _ => {
            return Err(ConnectorError::configuration_invalid(
                "endpoint_ref scheme must be http or https",
            ));
        }
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ConnectorError::configuration_invalid(
            "endpoint_ref must not embed credentials",
        ));
    }
    if url.host_str().is_none() {
        return Err(ConnectorError::configuration_invalid(
            "endpoint_ref missing host",
        ));
    }
    Ok(url)
}

async fn open_http(
    request: OpenConnection,
    client: reqwest::Client,
    config: StreamingHttpConfig,
    credentials: Arc<dyn CredentialResolver>,
    control: ConnectionControlHandle,
    control_state: Arc<ControlState>,
    instance_id: ConnectorInstanceId,
) -> Result<(OpenedRawConnection, ConnectionOwnerWork), ConnectorError> {
    validate_open_attachment_owner(&instance_id, request.session_attachment.as_deref())?;

    let url = validate_endpoint_url(&request.endpoint_ref, config.require_https)
        .map_err(|e| e.with_connection_id(request.connection_id.as_str()))?;

    // Resolve credentials at open (fail closed before accepting body).
    let resolved = if let Some(ref cred_ref) = request.credential_ref {
        credentials
            .resolve(cred_ref)
            .map_err(|e| e.with_connection_id(request.connection_id.as_str()))?
    } else {
        ResolvedCredential::none()
    };

    if let Some(err) = control.interrupt_error() {
        return Err(err.with_connection_id(request.connection_id.as_str()));
    }

    let in_buf = request.limits.buffers.max_queued_input_bytes.max(1);
    let in_capacity = (in_buf / request.limits.buffers.max_chunk_bytes.max(1)).max(1);
    // D-033: output queue capacity from output-byte budget, not input buffers.
    let out_buf = request
        .limits
        .buffers
        .max_queued_output_bytes
        .min(config.max_response_bytes)
        .max(1);
    let out_capacity = (out_buf / request.limits.buffers.max_chunk_bytes.max(1)).max(1);
    let max_chunk = request
        .limits
        .buffers
        .max_chunk_bytes
        .min(config.max_chunk_bytes)
        .max(1);

    let (in_tx, in_rx) = mpsc::channel::<RawInputMessage>(in_capacity);
    let (out_tx, out_rx) = mpsc::channel::<Bytes>(out_capacity);
    let (end_tx, end_rx) = oneshot::channel();

    let connection_id = request.connection_id.clone();
    let external_session_id = request
        .session_attachment
        .as_ref()
        .map(|a| a.external_session_id.clone())
        .or(request.external_session_id.clone());

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

    // Overall open deadline for connect path is applied inside the owner on send.
    let connect_deadline = request.limits.connect_deadline;
    let dialect = config.dialect.clone();

    let owner_work = ConnectionOwnerWork::new(run_http_owner(
        connection_id.clone(),
        control_state,
        in_rx,
        out_tx,
        end_tx,
        client,
        config,
        url,
        resolved,
        connect_deadline,
    ));

    Ok((
        OpenedRawConnection {
            connection_id,
            external_session_id,
            dialect,
            input,
            output,
            control,
            completion,
        },
        owner_work,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_http_owner(
    connection_id: ConnectionId,
    control: Arc<ControlState>,
    mut in_rx: mpsc::Receiver<RawInputMessage>,
    out_tx: mpsc::Sender<Bytes>,
    end_tx: oneshot::Sender<crate::handles::ConnectionEnd>,
    client: reqwest::Client,
    config: StreamingHttpConfig,
    url: reqwest::Url,
    credential: ResolvedCredential,
    connect_deadline: Duration,
) {
    let mut owner = ConnectionOwner::new(connection_id.clone(), Arc::clone(&control), end_tx);
    let mut body: Vec<u8> = Vec::new();

    // Phase 1: collect complete request body until Finish (SendAndFinish).
    let body_ready = loop {
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
                        if body.len().saturating_add(bytes.len()) > config.max_request_bytes {
                            owner.finish(
                                ConnectionEndKind::TransportFailure,
                                EndInitiator::LocalTransport,
                                Some("request body exceeds max_request_bytes".into()),
                            );
                            return;
                        }
                        owner.bytes_accepted += bytes.len() as u64;
                        body.extend_from_slice(&bytes);
                    }
                    Some(RawInputMessage::Finish) => break true,
                    None => break false,
                }
            }
        }
    };

    if !body_ready {
        // Input closed without finish — treat as local shutdown with empty send skip.
        owner.finish(
            ConnectionEndKind::LocalShutdown,
            EndInitiator::LocalTransport,
            None,
        );
        return;
    }

    if let Some(err_kind) = control_end_kind(&control) {
        owner.finish(err_kind, EndInitiator::LocalControl, None);
        return;
    }

    // Phase 2: POST body and stream response.
    // D-033: one absolute request deadline covering send+headers+body; never reset.
    let deadline = Instant::now() + config.request_timeout;
    let mut builder = client.request(config.method.as_reqwest(), url);
    for (name, value) in &config.headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    if let Some(auth) = credential.expose_authorization() {
        builder = builder.header(reqwest::header::AUTHORIZATION, auth);
    }
    builder = builder.body(body);

    let send_fut = builder.send();
    let send_budget = deadline
        .saturating_duration_since(Instant::now())
        .min(connect_deadline);
    if send_budget.is_zero() {
        owner.finish(
            ConnectionEndKind::TransportFailure,
            EndInitiator::LocalTransport,
            Some("http request deadline exceeded".into()),
        );
        return;
    }
    let response = tokio::select! {
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
        r = timeout(send_budget, send_fut) => {
            match r {
                Ok(Ok(resp)) => resp,
                Ok(Err(_)) => {
                    owner.finish(
                        ConnectionEndKind::TransportFailure,
                        EndInitiator::LocalTransport,
                        Some("http request failed".into()),
                    );
                    return;
                }
                Err(_) => {
                    owner.finish(
                        ConnectionEndKind::TransportFailure,
                        EndInitiator::LocalTransport,
                        Some("http request deadline exceeded".into()),
                    );
                    return;
                }
            }
        }
    };

    let status = response.status();
    if !status.is_success() {
        // D-019: do not buffer unbounded error bodies; discard without retain.
        drop(response);
        owner.finish(
            ConnectionEndKind::TransportFailure,
            EndInitiator::Remote,
            Some(format!("http status {}", status.as_u16())),
        );
        return;
    }

    let mut stream = response.bytes_stream();
    let mut total_response: usize = 0;

    loop {
        if Instant::now() >= deadline {
            owner.finish(
                ConnectionEndKind::TransportFailure,
                EndInitiator::LocalTransport,
                Some("http overall response deadline exceeded".into()),
            );
            return;
        }
        if let Some(kind) = control_end_kind(&control) {
            owner.finish(kind, EndInitiator::LocalControl, None);
            return;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let idle = config.idle_timeout.min(remaining);
        let next = timeout(idle, stream.next());

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
            item = next => {
                match item {
                    Ok(Some(Ok(chunk))) => {
                        if chunk.is_empty() {
                            continue;
                        }
                        if chunk.len() > config.max_chunk_bytes {
                            owner.finish(
                                ConnectionEndKind::TransportFailure,
                                EndInitiator::LocalTransport,
                                Some("response chunk exceeds max_chunk_bytes".into()),
                            );
                            return;
                        }
                        if total_response.saturating_add(chunk.len()) > config.max_response_bytes {
                            owner.finish(
                                ConnectionEndKind::TransportFailure,
                                EndInitiator::LocalTransport,
                                Some("response exceeds max_response_bytes".into()),
                            );
                            return;
                        }
                        total_response += chunk.len();
                        let len = chunk.len() as u64;
                        // D-033: blocked enqueue selects cancel, idle, and overall deadline.
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            owner.finish(
                                ConnectionEndKind::TransportFailure,
                                EndInitiator::LocalTransport,
                                Some("http overall response deadline exceeded".into()),
                            );
                            return;
                        }
                        let enqueue_budget = config.idle_timeout.min(remaining);
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
                            _ = tokio::time::sleep(enqueue_budget) => {
                                let msg = if Instant::now() >= deadline {
                                    "http overall response deadline exceeded"
                                } else {
                                    "http idle timeout"
                                };
                                owner.finish(
                                    ConnectionEndKind::TransportFailure,
                                    EndInitiator::LocalTransport,
                                    Some(msg.into()),
                                );
                                return;
                            }
                            send_res = out_tx.send(chunk) => {
                                if send_res.is_err() {
                                    owner.finish(
                                        ConnectionEndKind::TransportFailure,
                                        EndInitiator::LocalTransport,
                                        Some("output closed".into()),
                                    );
                                    return;
                                }
                            }
                        }
                        owner.bytes_received += len;
                    }
                    Ok(Some(Err(_))) => {
                        owner.finish(
                            ConnectionEndKind::TransportFailure,
                            EndInitiator::LocalTransport,
                            Some("response stream error".into()),
                        );
                        return;
                    }
                    Ok(None) => {
                        owner.finish(
                            ConnectionEndKind::RemoteEof,
                            EndInitiator::Remote,
                            None,
                        );
                        return;
                    }
                    Err(_) => {
                        owner.finish(
                            ConnectionEndKind::TransportFailure,
                            EndInitiator::LocalTransport,
                            Some("http idle timeout".into()),
                        );
                        return;
                    }
                }
            }
        }
    }
}

async fn wait_control(control: &ControlState) {
    loop {
        if control.cancel_requested() || control.terminate_requested() || control.is_terminal() {
            return;
        }
        control.notify().notified().await;
    }
}

fn control_end_kind(control: &ControlState) -> Option<ConnectionEndKind> {
    if control.terminate_requested() {
        Some(ConnectionEndKind::Terminated)
    } else if control.cancel_requested() {
        Some(ConnectionEndKind::Cancelled)
    } else {
        None
    }
}
