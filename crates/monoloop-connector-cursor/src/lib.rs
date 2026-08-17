//! Cursor Agent ACP connector profile.
//!
//! Spawns `agent acp` and speaks JSON-RPC 2.0 over **stdio NDJSON**.
//! Moves dialect-labelled envelopes only — does not interpret tool semantics
//! or execute host tools (permission replies are configurable for unattended runs).
//!
//! Session correlation identity is Cursor's `sessionId` (explicit create/load only).
//!
//! Prefer [`CursorAgentHandle`] for multi-step session control. The [`Connector`]
//! trait bridge opens one session and maps prompt text → `session/prompt`.

#![deny(missing_docs)]

mod channel_binding;
mod config;
mod error;
mod process;
mod raw_dump;
mod session;

pub use channel_binding::{cursor_channel_binding, CursorConnectorFactory};
pub use config::{CursorAgentConfig, CursorSessionConfig};
pub use error::CursorConnectorError;
pub use raw_dump::CursorRawDump;
pub use session::{CursorAgentHandle, CursorSession};

use monoloop_connector::{
    ConnectionCompletionHandle, ConnectionControlHandle, ConnectionEnd, ConnectionEndKind,
    Connector, ConnectorDescriptor, ControlState, EndInitiator, OpenConnection,
    OpenedRawConnection, PendingRawConnection, RawInputHandle, RawInputMessage, RawOutputHandle,
};
use monoloop_contracts::{DialectBinding, DialectDescriptor, ExternalSessionId};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Factory for Cursor ACP process connections.
pub struct CursorConnector {
    descriptor: ConnectorDescriptor,
    default_config: CursorAgentConfig,
}

impl CursorConnector {
    /// Create with default agent binary discovery.
    pub fn new() -> Self {
        Self {
            descriptor: ConnectorDescriptor::cursor_acp(),
            default_config: CursorAgentConfig::default(),
        }
    }

    /// Create with an explicit base config (binary path, cwd, dump, …).
    pub fn with_config(config: CursorAgentConfig) -> Self {
        Self {
            descriptor: ConnectorDescriptor::cursor_acp(),
            default_config: config,
        }
    }

    /// High-level connect (preferred for multi-step sessions).
    pub async fn connect(
        &self,
        config: CursorAgentConfig,
    ) -> Result<CursorAgentHandle, CursorConnectorError> {
        CursorAgentHandle::connect(config).await
    }
}

impl Default for CursorConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Connector for CursorConnector {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }

    /// `endpoint_ref` forms: `cursor:stdio`, `stdio`, or `cursor:/path/to/agent`.
    fn begin_open(&self, request: OpenConnection) -> PendingRawConnection {
        let mut config = self.default_config.clone();
        if let Some(path) = parse_agent_bin(&request.endpoint_ref) {
            config.agent_bin = path;
        }
        let connection_id = request.connection_id.clone();
        let control_state = ControlState::new();
        let control = ConnectionControlHandle::new(Arc::clone(&control_state));
        let control_open = control.clone();
        let opened =
            Box::pin(async move { open_raw(config, request, control_open, control_state).await });
        PendingRawConnection {
            connection_id,
            control,
            opened,
        }
    }
}

fn parse_agent_bin(endpoint_ref: &str) -> Option<std::path::PathBuf> {
    let s = endpoint_ref.strip_prefix("cursor:").unwrap_or(endpoint_ref);
    if s == "stdio" || s.is_empty() {
        None
    } else {
        Some(std::path::PathBuf::from(s))
    }
}

async fn open_raw(
    config: CursorAgentConfig,
    request: OpenConnection,
    control: ConnectionControlHandle,
    control_state: Arc<ControlState>,
) -> Result<OpenedRawConnection, monoloop_contracts::ConnectorError> {
    let mut agent = CursorAgentHandle::connect(config.clone())
        .await
        .map_err(|e| e.into_connector_error())?;

    let session = if let Some(ref ext) = request.external_session_id {
        agent
            .session_load(ext.as_str(), &config.cwd)
            .await
            .map_err(|e| e.into_connector_error())?
    } else {
        agent
            .session_new(CursorSessionConfig::new(&config.cwd))
            .await
            .map_err(|e| e.into_connector_error())?
    };

    let dialect = DialectBinding::negotiated(DialectDescriptor::cursor_acp("1"));
    // Enforce configured chunk size exactly (do not silently raise the floor).
    let max_chunk = request.limits.buffers.max_chunk_bytes.max(1);
    let in_capacity = (request.limits.buffers.max_queued_input_bytes.max(1) / max_chunk).max(1);
    let out_capacity = (request.limits.buffers.max_queued_output_bytes.max(1) / max_chunk)
        .max(1)
        .min(config.max_output_queue.max(1));
    let (in_tx, mut in_rx) = mpsc::channel::<RawInputMessage>(in_capacity);
    let (out_tx, out_rx) = mpsc::channel(out_capacity);
    let (end_tx, end_rx) = oneshot::channel::<ConnectionEnd>();

    let input = RawInputHandle::new(
        request.connection_id.clone(),
        in_tx,
        Arc::clone(&control_state),
        max_chunk,
    );
    let output = Arc::new(RawOutputHandle::new(
        request.connection_id.clone(),
        out_rx,
        Arc::clone(&control_state),
    ));
    let completion = ConnectionCompletionHandle::new(end_rx);

    let connection_id = request.connection_id.clone();
    let external_session_id = Some(ExternalSessionId::new(session.session_id.clone()));

    // Pump session/update NDJSON → raw output.
    let mut updates = agent.take_updates();
    let out_pump = out_tx;
    tokio::spawn(async move {
        while let Some(bytes) = updates.recv().await {
            if out_pump.send(bytes).await.is_err() {
                break;
            }
        }
    });

    // Input → session/prompt; honour cancel/terminate without waiting for drop.
    let control_wait = control.clone();
    tokio::spawn(async move {
        let mut bytes_accepted = 0u64;
        let (end_kind, initiated, safe_err) = loop {
            tokio::select! {
                biased;
                _ = control_wait.interrupted() => {
                    let kind = if control_state.terminate_requested() {
                        ConnectionEndKind::Terminated
                    } else {
                        ConnectionEndKind::Cancelled
                    };
                    let _ = session.cancel().await;
                    break (kind, EndInitiator::LocalControl, None);
                }
                msg = in_rx.recv() => {
                    match msg {
                        Some(RawInputMessage::Bytes(b)) => {
                            bytes_accepted += b.len() as u64;
                            let text = String::from_utf8_lossy(&b).into_owned();
                            if text.trim().is_empty() {
                                continue;
                            }
                            if let Err(e) = session.prompt_text(text).await {
                                break (
                                    ConnectionEndKind::TransportFailure,
                                    EndInitiator::LocalTransport,
                                    Some(safe_prompt_error(&e)),
                                );
                            }
                        }
                        Some(RawInputMessage::Finish) | None => {
                            break (
                                ConnectionEndKind::LocalShutdown,
                                EndInitiator::LocalControl,
                                None,
                            );
                        }
                    }
                }
            }
        };

        agent.shutdown().await;
        control_state.mark_terminal();
        let _ = end_tx.send(ConnectionEnd {
            connection_id: connection_id.clone(),
            kind: end_kind,
            initiated_by: initiated,
            bytes_accepted,
            bytes_received: 0,
            safe_transport_error: safe_err,
        });
    });

    Ok(OpenedRawConnection {
        connection_id: request.connection_id,
        external_session_id,
        dialect,
        input,
        output,
        control,
        completion,
    })
}

fn safe_prompt_error(e: &CursorConnectorError) -> String {
    // Closed vocabulary only — no prompts, paths, or credentials.
    match e.kind {
        monoloop_contracts::ConnectorErrorKind::DeadlineExceeded => {
            "prompt_rpc_deadline_exceeded".into()
        }
        monoloop_contracts::ConnectorErrorKind::Cancelled => "prompt_cancelled".into(),
        monoloop_contracts::ConnectorErrorKind::ConnectionFailed => {
            "prompt_connection_failed".into()
        }
        monoloop_contracts::ConnectorErrorKind::ProtocolFailed => "prompt_protocol_failed".into(),
        monoloop_contracts::ConnectorErrorKind::SessionFailed => "prompt_session_failed".into(),
        _ => "prompt_failed".into(),
    }
}
