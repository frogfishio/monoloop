//! Raw I/O and completion handles.

use crate::control::{ControlState, PreferredEnd};
use bytes::Bytes;
use monoloop_contracts::{ConnectionId, ConnectorError, ConnectorErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

/// Terminal transport outcome for one connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionEnd {
    /// Connection identity.
    pub connection_id: ConnectionId,
    /// Terminal kind.
    pub kind: ConnectionEndKind,
    /// Who initiated the end when known.
    pub initiated_by: EndInitiator,
    /// Bytes accepted on input.
    pub bytes_accepted: u64,
    /// Bytes delivered on output.
    pub bytes_received: u64,
    /// Safe transport classification (no secrets/bodies).
    pub safe_transport_error: Option<String>,
}

/// Closed terminal kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionEndKind {
    /// Remote EOF / clean remote close.
    RemoteEof,
    /// Cooperative cancel won the terminal race.
    Cancelled,
    /// Forced terminate won the terminal race.
    Terminated,
    /// Transport failure.
    TransportFailure,
    /// Local shutdown.
    LocalShutdown,
}

/// Who initiated the terminal transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndInitiator {
    /// Local caller control.
    LocalControl,
    /// Remote peer.
    Remote,
    /// Local transport/runtime.
    LocalTransport,
}

/// Cloneable raw input handle (bounded, ordered bytes already dialect-encoded).
#[derive(Clone, Debug)]
pub struct RawInputHandle {
    connection_id: ConnectionId,
    tx: mpsc::Sender<RawInputMessage>,
    finished: Arc<AtomicBool>,
    control: Arc<ControlState>,
    max_chunk_bytes: usize,
}

/// Message on the raw input pipe (for connection owners / adapters).
#[derive(Debug)]
pub enum RawInputMessage {
    /// Encoded dialect bytes.
    Bytes(Bytes),
    /// Input half-close.
    Finish,
}

impl RawInputHandle {
    /// Create an input handle bound to a channel and control state.
    pub fn new(
        connection_id: ConnectionId,
        tx: mpsc::Sender<RawInputMessage>,
        control: Arc<ControlState>,
        max_chunk_bytes: usize,
    ) -> Self {
        Self {
            connection_id,
            tx,
            finished: Arc::new(AtomicBool::new(false)),
            control,
            max_chunk_bytes,
        }
    }

    /// Send ordered encoded bytes.
    pub async fn send(&self, bytes: Bytes) -> Result<(), ConnectorError> {
        if let Some(err) = self.control_interrupt() {
            return Err(err);
        }
        if self.finished.load(Ordering::SeqCst) || self.control.is_terminal() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::WriteFailed,
                "send after finish or terminal",
            )
            .with_connection_id(self.connection_id.as_str()));
        }
        if bytes.len() > self.max_chunk_bytes {
            return Err(ConnectorError::resource("chunk exceeds max_chunk_bytes")
                .with_connection_id(self.connection_id.as_str()));
        }
        self.tx
            .send(RawInputMessage::Bytes(bytes))
            .await
            .map_err(|_| {
                ConnectorError::new(ConnectorErrorKind::WriteFailed, "input channel closed")
                    .with_connection_id(self.connection_id.as_str())
            })
    }

    /// Input half-close when supported.
    pub async fn finish(&self) -> Result<(), ConnectorError> {
        if let Some(err) = self.control_interrupt() {
            return Err(err);
        }
        if self.finished.swap(true, Ordering::SeqCst) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::WriteFailed,
                "input already finished",
            )
            .with_connection_id(self.connection_id.as_str()));
        }
        self.tx.send(RawInputMessage::Finish).await.map_err(|_| {
            ConnectorError::new(ConnectorErrorKind::WriteFailed, "input channel closed")
                .with_connection_id(self.connection_id.as_str())
        })
    }

    fn control_interrupt(&self) -> Option<ConnectorError> {
        if self.control.terminate_requested() {
            Some(ConnectorError::terminated().with_connection_id(self.connection_id.as_str()))
        } else if self.control.cancel_requested() {
            Some(ConnectorError::cancelled().with_connection_id(self.connection_id.as_str()))
        } else {
            None
        }
    }
}

/// Cloneable raw output handle.
#[derive(Debug)]
pub struct RawOutputHandle {
    connection_id: ConnectionId,
    rx: Mutex<mpsc::Receiver<Bytes>>,
    control: Arc<ControlState>,
}

impl RawOutputHandle {
    /// Create an output handle bound to a channel and control state.
    pub fn new(
        connection_id: ConnectionId,
        rx: mpsc::Receiver<Bytes>,
        control: Arc<ControlState>,
    ) -> Self {
        Self {
            connection_id,
            rx: Mutex::new(rx),
            control,
        }
    }

    /// Receive the next ordered transport chunk.
    ///
    /// `Ok(None)` means the output side closed; consult connection completion
    /// for the authoritative terminal reason.
    pub async fn receive(&self) -> Result<Option<Bytes>, ConnectorError> {
        if let Some(err) = self.control_interrupt_if_no_data() {
            // Prefer draining already-queued bytes before surfacing cancel.
            let mut guard = self.rx.lock().await;
            match guard.try_recv() {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(mpsc::error::TryRecvError::Empty) => return Err(err),
                Err(mpsc::error::TryRecvError::Disconnected) => return Ok(None),
            }
        }

        let mut guard = self.rx.lock().await;
        tokio::select! {
            biased;
            msg = guard.recv() => Ok(msg),
            _ = self.wait_interrupt() => {
                match guard.try_recv() {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(mpsc::error::TryRecvError::Empty) => {
                        Err(self.control_interrupt_if_no_data().unwrap_or_else(|| {
                            ConnectorError::cancelled()
                                .with_connection_id(self.connection_id.as_str())
                        }))
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => Ok(None),
                }
            }
        }
    }

    async fn wait_interrupt(&self) {
        loop {
            if self.control.cancel_requested()
                || self.control.terminate_requested()
                || self.control.is_terminal()
            {
                return;
            }
            self.control.notify().notified().await;
        }
    }

    fn control_interrupt_if_no_data(&self) -> Option<ConnectorError> {
        if self.control.terminate_requested() {
            Some(ConnectorError::terminated().with_connection_id(self.connection_id.as_str()))
        } else if self.control.cancel_requested() {
            Some(ConnectorError::cancelled().with_connection_id(self.connection_id.as_str()))
        } else {
            None
        }
    }
}

/// One-shot connection completion handle.
#[derive(Debug)]
pub struct ConnectionCompletionHandle {
    rx: Mutex<Option<oneshot::Receiver<ConnectionEnd>>>,
}

impl ConnectionCompletionHandle {
    /// Create a completion handle from a oneshot receiver.
    pub fn new(rx: oneshot::Receiver<ConnectionEnd>) -> Self {
        Self {
            rx: Mutex::new(Some(rx)),
        }
    }

    /// Wait for exactly one terminal outcome. Safe to call only once.
    pub async fn wait(self) -> ConnectionEnd {
        let mut guard = self.rx.lock().await;
        let rx = guard
            .take()
            .expect("ConnectionCompletionHandle polled twice");
        match rx.await {
            Ok(end) => end,
            Err(_) => ConnectionEnd {
                connection_id: ConnectionId::new("unknown"),
                kind: ConnectionEndKind::TransportFailure,
                initiated_by: EndInitiator::LocalTransport,
                bytes_accepted: 0,
                bytes_received: 0,
                safe_transport_error: Some("completion channel dropped".into()),
            },
        }
    }
}

/// Shared counters and terminal publisher for a connection owner task.
pub struct ConnectionOwner {
    /// Connection identity.
    pub connection_id: ConnectionId,
    /// Shared control state.
    pub control: Arc<ControlState>,
    /// Bytes accepted on input.
    pub bytes_accepted: u64,
    /// Bytes delivered on output.
    pub bytes_received: u64,
    end_tx: Option<oneshot::Sender<ConnectionEnd>>,
}

impl ConnectionOwner {
    /// Create an owner that will publish exactly one [`ConnectionEnd`].
    pub fn new(
        connection_id: ConnectionId,
        control: Arc<ControlState>,
        end_tx: oneshot::Sender<ConnectionEnd>,
    ) -> Self {
        Self {
            connection_id,
            control,
            bytes_accepted: 0,
            bytes_received: 0,
            end_tx: Some(end_tx),
        }
    }

    /// Publish exactly one terminal outcome using the terminal-race rule.
    pub fn finish(
        &mut self,
        observed: ConnectionEndKind,
        initiated_by: EndInitiator,
        safe_error: Option<String>,
    ) {
        if self.control.is_terminal() {
            return;
        }
        let kind = match self.control.preferred_end_kind() {
            Some(PreferredEnd::Terminated) => ConnectionEndKind::Terminated,
            Some(PreferredEnd::Cancelled) => ConnectionEndKind::Cancelled,
            None => observed,
        };
        let initiated = match kind {
            ConnectionEndKind::Cancelled | ConnectionEndKind::Terminated => {
                EndInitiator::LocalControl
            }
            _ => initiated_by,
        };
        self.control.mark_terminal();
        if let Some(tx) = self.end_tx.take() {
            let _ = tx.send(ConnectionEnd {
                connection_id: self.connection_id.clone(),
                kind,
                initiated_by: initiated,
                bytes_accepted: self.bytes_accepted,
                bytes_received: self.bytes_received,
                safe_transport_error: safe_error,
            });
        }
    }
}
