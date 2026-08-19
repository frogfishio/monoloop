//! SPDX-License-Identifier: AGPL-3.0-or-later
//! Copyright (C) Alexander R. Croft
//!
//! Google Antigravity (`agy`) ACP connector profile.
//!
//! Speaks JSON-RPC 2.0 over **stdio NDJSON**, same client shape as Cursor ACP.
//!
//! **Reality (2026):** native `agy` does not yet expose `--acp`. The practical
//! server is the community `agy-acp` bridge (or a future native binary). Configure
//! via `AGY_ACP_BIN` or default discovery (`agy-acp` → `npx --yes agy-acp`).
//!
//! Session correlation identity is the ACP `sessionId` (explicit create/load only).

#![deny(missing_docs)]

mod channel_binding;
mod config;
mod error;
mod process;
mod raw_dump;
mod session;

pub use channel_binding::{agy_channel_binding, AgyConnectorFactory};
pub use config::{AgyAgentConfig, AgySessionConfig};
pub use error::AgyConnectorError;
pub use raw_dump::AgyRawDump;
pub use session::{AgyAgentHandle, AgySession};

use monoloop_connector::{
    ConnectionCompletionHandle, ConnectionControlHandle, ConnectionEnd, ConnectionEndKind,
    ConnectionOwnerWork, Connector, ConnectorDescriptor, ControlState, EndInitiator,
    OpenConnection, OpenedRawConnection, PendingRawConnection, RawInputHandle, RawInputMessage,
    RawOutputHandle,
};
use monoloop_contracts::{DialectBinding, DialectDescriptor, ExternalSessionId};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Factory for Antigravity ACP process connections.
pub struct AgyConnector {
    descriptor: ConnectorDescriptor,
    default_config: AgyAgentConfig,
}

impl AgyConnector {
    /// Create with default ACP bridge discovery.
    pub fn new() -> Self {
        Self {
            descriptor: ConnectorDescriptor::agy_acp(),
            default_config: AgyAgentConfig::default(),
        }
    }

    /// Create with an explicit base config.
    pub fn with_config(config: AgyAgentConfig) -> Self {
        Self {
            descriptor: ConnectorDescriptor::agy_acp(),
            default_config: config,
        }
    }

    /// High-level connect (preferred for multi-step sessions).
    pub async fn connect(
        &self,
        config: AgyAgentConfig,
    ) -> Result<AgyAgentHandle, AgyConnectorError> {
        AgyAgentHandle::connect(config).await
    }
}

impl Default for AgyConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Connector for AgyConnector {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }

    /// `endpoint_ref`: `agy:stdio` / `stdio` or path to ACP bridge binary.
    fn begin_open(&self, request: OpenConnection) -> PendingRawConnection {
        let mut config = self.default_config.clone();
        if let Some(path) = parse_endpoint(&request.endpoint_ref) {
            config.command = path;
            config.args.clear();
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

fn parse_endpoint(endpoint_ref: &str) -> Option<std::path::PathBuf> {
    let s = endpoint_ref
        .strip_prefix("agy:")
        .or_else(|| endpoint_ref.strip_prefix("antigravity:"))
        .unwrap_or(endpoint_ref);
    if s == "stdio" || s.is_empty() {
        None
    } else {
        Some(std::path::PathBuf::from(s))
    }
}

async fn open_raw(
    config: AgyAgentConfig,
    request: OpenConnection,
    control: ConnectionControlHandle,
    control_state: Arc<ControlState>,
) -> Result<OpenedRawConnection, monoloop_contracts::ConnectorError> {
    let mut agent = AgyAgentHandle::connect(config.clone())
        .await
        .map_err(|e| e.into_connector_error())?;

    let session = if let Some(ref ext) = request.external_session_id {
        agent
            .session_load(ext.as_str(), &config.cwd)
            .await
            .map_err(|e| e.into_connector_error())?
    } else {
        // D-026: CreationOnly MCP descriptor must reach provider session/new.
        let mut session_cfg = AgySessionConfig::new(&config.cwd);
        if let Some(mcp) = request
            .session_attachment
            .as_ref()
            .and_then(|a| a.initial_mcp.as_ref())
        {
            session_cfg.mcp_servers = serde_json::json!([{
                "name": mcp.server_name,
                "type": "http",
                "url": mcp.expose_capability_url(),
            }]);
        }
        agent
            .session_new(session_cfg)
            .await
            .map_err(|e| e.into_connector_error())?
    };

    let dialect = DialectBinding::negotiated(DialectDescriptor::agy_acp("1"));
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

    // Update pump must run concurrently with prompt_text awaits (LAW 23 joinable
    // via JoinSet owned by this ConnectionOwnerWork — not fused into the input
    // select, which deadlocks when prompt RPC waits on stdout).
    let mut updates = agent.take_updates();
    let control_wait = control.clone();
    let owner_work = ConnectionOwnerWork::new(async move {
        let mut joins = tokio::task::JoinSet::new();
        let out_pump = out_tx;
        joins.spawn(async move {
            while let Some(bytes) = updates.recv().await {
                if out_pump.send(bytes).await.is_err() {
                    break;
                }
            }
        });

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
                            // Do not exit before shutdown joins the update pump —
                            // Finish only ends input; updates may still be in flight.
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
        while joins.join_next().await.is_some() {}
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
        owner_work: Some(owner_work),
    })
}

fn safe_prompt_error(e: &AgyConnectorError) -> String {
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
