//! Grok session factory, handles, and per-session I/O.

use crate::config::{GrokSessionConfig, GrokSessionLoadConfig};
use crate::error::GrokConnectorError;
use crate::server::ServerInner;
use bytes::Bytes;
use monoloop_connector::{
    CancellationReason, ConnectionEndKind, ConnectionId, ControlDisposition, RawOutputHandle,
    TerminationReason,
};
use monoloop_contracts::GrokSessionId;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock};

/// Complete bounded outbound ACP session message from the dialect encoder.
///
/// Contains method + parameters only. The connector allocates JSON-RPC ids and
/// binds the authoritative sessionId.
#[derive(Clone, Debug)]
pub struct EncodedAcpSessionMessage {
    /// ACP method (e.g. `session/prompt`).
    pub method: String,
    /// Complete encoded parameters **without** wire request id.
    ///
    /// May omit `sessionId`; the connector inserts the handle's session id.
    pub params: serde_json::Value,
}

/// Pending session create/load.
pub struct PendingGrokSession {
    /// Completes with session handle or error.
    pub opened: tokio::sync::oneshot::Receiver<Result<GrokSessionHandle, GrokConnectorError>>,
    /// Control available while pending.
    pub control: GrokSessionControl,
}

/// Factory for sessions on one connected server.
#[derive(Clone)]
pub struct GrokSessionFactory {
    pub(crate) inner: Arc<ServerInner>,
}

impl GrokSessionFactory {
    /// Begin `session/new`. Returns immediately.
    pub fn begin_new(
        &self,
        config: GrokSessionConfig,
    ) -> Result<PendingGrokSession, GrokConnectorError> {
        self.inner.begin_session_new(config)
    }

    /// Begin `session/load` with an **explicit** known Grok session id.
    ///
    /// Never selects a most-recent session.
    pub fn begin_load(
        &self,
        session_id: GrokSessionId,
        config: GrokSessionLoadConfig,
    ) -> Result<PendingGrokSession, GrokConnectorError> {
        self.inner.begin_session_load(session_id, config)
    }
}

/// Live session handle.
pub struct GrokSessionHandle {
    /// Authoritative Grok session id (sole correlation identity).
    pub session_id: GrokSessionId,
    /// Local connection identity for this logical attachment.
    pub connection_id: ConnectionId,
    /// Session-scoped input (JSON-RPC routed).
    pub input: GrokSessionInput,
    /// Ordered inbound dialect bytes (complete JSON-RPC messages for this session).
    pub output: Arc<RawOutputHandle>,
    /// Session control.
    pub control: GrokSessionControl,
    /// Health snapshot handle.
    pub health: GrokSessionHealth,
    /// Terminal completion for this local attachment.
    pub completion: SessionCompletion,
}

/// Session-scoped input.
#[derive(Clone)]
pub struct GrokSessionInput {
    inner: Arc<SessionInner>,
}

impl GrokSessionInput {
    /// Begin sending a complete encoded ACP message for this session.
    pub fn begin_send(
        &self,
        message: EncodedAcpSessionMessage,
    ) -> Result<PendingGrokExchange, GrokConnectorError> {
        self.inner.begin_send(message)
    }
}

/// Pending prompt/exchange result (JSON-RPC response bytes routed to caller).
pub struct PendingGrokExchange {
    /// Completes with response payload value or error.
    pub response: oneshot::Receiver<Result<serde_json::Value, GrokConnectorError>>,
}

/// Session control (scoped; cannot cancel sibling sessions).
#[derive(Clone)]
pub struct GrokSessionControl {
    inner: Arc<SessionInner>,
}

impl GrokSessionControl {
    /// Cancel this session attachment / in-flight work.
    pub fn cancel(&self, reason: CancellationReason) -> ControlDisposition {
        self.inner.cancel(reason)
    }

    /// Terminate this session attachment.
    pub fn terminate(&self, reason: TerminationReason) -> ControlDisposition {
        self.inner.terminate(reason)
    }
}

/// Session health counters (content-free).
#[derive(Clone, Debug, Default)]
pub struct GrokSessionHealth {
    /// Messages sent.
    pub messages_sent: Arc<AtomicU64>,
    /// Updates received.
    pub updates_received: Arc<AtomicU64>,
}

/// Session completion.
pub struct SessionCompletion {
    rx: Mutex<Option<oneshot::Receiver<SessionEnd>>>,
}

impl SessionCompletion {
    pub(crate) fn new(rx: oneshot::Receiver<SessionEnd>) -> Self {
        Self {
            rx: Mutex::new(Some(rx)),
        }
    }

    /// Wait for local session detachment terminal.
    pub async fn wait(self) -> SessionEnd {
        let mut guard = self.rx.lock().await;
        let rx = guard.take().expect("SessionCompletion polled twice");
        rx.await.unwrap_or(SessionEnd {
            session_id: GrokSessionId::new("unknown"),
            kind: ConnectionEndKind::TransportFailure,
            safe_error: Some("session completion dropped".into()),
        })
    }
}

/// Local session attachment end (not proof Grok deleted the session).
#[derive(Clone, Debug)]
pub struct SessionEnd {
    /// Session id.
    pub session_id: GrokSessionId,
    /// Terminal kind.
    pub kind: ConnectionEndKind,
    /// Safe error.
    pub safe_error: Option<String>,
}

pub(crate) struct SessionInner {
    pub(crate) session_id: GrokSessionId,
    pub(crate) connection_id: ConnectionId,
    pub(crate) server: Arc<ServerInner>,
    pub(crate) out_tx: mpsc::Sender<Bytes>,
    pub(crate) cancelled: AtomicBool,
    pub(crate) terminated: AtomicBool,
    pub(crate) detached: AtomicBool,
    pub(crate) notify: Notify,
    pub(crate) end_tx: Mutex<Option<oneshot::Sender<SessionEnd>>>,
    pub(crate) health: GrokSessionHealth,
    pub(crate) prompt_lock: tokio::sync::Mutex<()>,
    pub(crate) request_deadline: Duration,
}

impl SessionInner {
    pub(crate) fn control_handle(self: &Arc<Self>) -> GrokSessionControl {
        GrokSessionControl {
            inner: Arc::clone(self),
        }
    }

    pub(crate) fn input_handle(self: &Arc<Self>) -> GrokSessionInput {
        GrokSessionInput {
            inner: Arc::clone(self),
        }
    }

    fn cancel(&self, _reason: CancellationReason) -> ControlDisposition {
        if self.detached.load(Ordering::SeqCst) {
            return ControlDisposition::AlreadyTerminal;
        }
        if self.cancelled.swap(true, Ordering::SeqCst) {
            return ControlDisposition::AlreadyRequested;
        }
        self.notify.notify_waiters();
        self.finish(ConnectionEndKind::Cancelled, None);
        ControlDisposition::Accepted
    }

    fn terminate(&self, _reason: TerminationReason) -> ControlDisposition {
        if self.detached.load(Ordering::SeqCst) {
            return ControlDisposition::AlreadyTerminal;
        }
        if self.terminated.swap(true, Ordering::SeqCst) {
            return ControlDisposition::AlreadyRequested;
        }
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
        self.finish(ConnectionEndKind::Terminated, None);
        ControlDisposition::Accepted
    }

    pub(crate) fn finish(&self, kind: ConnectionEndKind, safe_error: Option<String>) {
        if self.detached.swap(true, Ordering::SeqCst) {
            return;
        }
        self.notify.notify_waiters();
        // Close inbound so RawOutputHandle.receive returns None.
        // Dropping the sender end of the session output channel wakes waiters.
        // (out_tx is cloned only for push_inbound; dropping this struct's clone
        // alone is not enough — we replace with a dummy closed channel.)
        // Best-effort: leave a sentinel by draining capacity is not needed;
        // detach removes routing and future pushes fail.
        if let Ok(mut guard) = self.end_tx.try_lock() {
            if let Some(tx) = guard.take() {
                let _ = tx.send(SessionEnd {
                    session_id: self.session_id.clone(),
                    kind,
                    safe_error,
                });
            }
        }
        self.server.detach_session(self.session_id.as_str());
    }

    fn begin_send(
        self: &Arc<Self>,
        message: EncodedAcpSessionMessage,
    ) -> Result<PendingGrokExchange, GrokConnectorError> {
        if self.detached.load(Ordering::SeqCst) {
            return Err(GrokConnectorError::session("session detached"));
        }
        if self.cancelled.load(Ordering::SeqCst) || self.terminated.load(Ordering::SeqCst) {
            return Err(GrokConnectorError::cancelled());
        }

        let (tx, rx) = oneshot::channel();
        let session = Arc::clone(self);
        tokio::spawn(async move {
            let result = session.send_rpc(message).await;
            let _ = tx.send(result);
        });
        Ok(PendingGrokExchange { response: rx })
    }

    async fn send_rpc(
        self: &Arc<Self>,
        message: EncodedAcpSessionMessage,
    ) -> Result<serde_json::Value, GrokConnectorError> {
        // Serialize prompts within one session by default.
        let _guard = self.prompt_lock.lock().await;
        if self.detached.load(Ordering::SeqCst) {
            return Err(GrokConnectorError::session("session detached"));
        }

        let mut params = message.params;
        if let Some(obj) = params.as_object_mut() {
            obj.insert(
                "sessionId".into(),
                serde_json::Value::String(self.session_id.as_str().to_string()),
            );
        } else if params.is_null() {
            params = serde_json::json!({ "sessionId": self.session_id.as_str() });
        } else {
            return Err(GrokConnectorError::protocol(
                "session message params must be a JSON object or null",
            ));
        }

        let result = self
            .server
            .rpc_call(message.method, Some(params), self.request_deadline)
            .await?;
        self.health.messages_sent.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    pub(crate) async fn push_inbound(&self, bytes: Bytes) -> Result<(), GrokConnectorError> {
        if self.detached.load(Ordering::SeqCst) {
            return Err(GrokConnectorError::session("session detached"));
        }
        self.health.updates_received.fetch_add(1, Ordering::Relaxed);
        self.out_tx
            .send(bytes)
            .await
            .map_err(|_| GrokConnectorError::resource("session inbound queue full or closed"))
    }
}

/// Shared registry entry.
pub(crate) type SessionMap = RwLock<std::collections::HashMap<String, Arc<SessionInner>>>;
