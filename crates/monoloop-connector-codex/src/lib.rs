//! OpenAI Codex ACP connector profile.
//!
//! Speaks JSON-RPC 2.0 over **stdio NDJSON**, same client shape as Cursor / Agy ACP.
//!
//! **Practical path:** official adapter `@agentclientprotocol/codex-acp` (starts
//! Codex App Server, maps ACP ↔ Codex). Configure via `CODEX_ACP_BIN` or default
//! discovery (`codex-acp` → `npx --yes @agentclientprotocol/codex-acp`).
//! Host auth: existing `codex login`, or `OPENAI_API_KEY` / `CODEX_API_KEY`.
//!
//! Session correlation identity is the ACP `sessionId` (explicit create/load only).
//!
//! The TypeScript/Python Codex SDK and `codex app-server` are alternate control
//! planes; Monoloop folds Codex in via ACP so the shared Interpreter path applies.

#![deny(missing_docs)]

mod config;
mod error;
mod process;
mod raw_dump;
mod session;

pub use config::{CodexAgentConfig, CodexSessionConfig};
pub use error::CodexConnectorError;
pub use raw_dump::CodexRawDump;
pub use session::{CodexAgentHandle, CodexSession};

use monoloop_connector::{
    ConnectionCompletionHandle, ConnectionControlHandle, ConnectionEnd, ConnectionEndKind,
    Connector, ConnectorDescriptor, ControlState, EndInitiator, OpenConnection, OpenedRawConnection,
    PendingRawConnection, RawInputHandle, RawInputMessage, RawOutputHandle,
};
use monoloop_contracts::{DialectBinding, DialectDescriptor, ExternalSessionId};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Factory for Codex ACP process connections.
pub struct CodexConnector {
    descriptor: ConnectorDescriptor,
    default_config: CodexAgentConfig,
}

impl CodexConnector {
    /// Create with default ACP bridge discovery.
    pub fn new() -> Self {
        Self {
            descriptor: ConnectorDescriptor::codex_acp(),
            default_config: CodexAgentConfig::default(),
        }
    }

    /// Create with an explicit base config.
    pub fn with_config(config: CodexAgentConfig) -> Self {
        Self {
            descriptor: ConnectorDescriptor::codex_acp(),
            default_config: config,
        }
    }

    /// High-level connect (preferred for multi-step sessions).
    pub async fn connect(
        &self,
        config: CodexAgentConfig,
    ) -> Result<CodexAgentHandle, CodexConnectorError> {
        CodexAgentHandle::connect(config).await
    }
}

impl Default for CodexConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Connector for CodexConnector {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }

    /// `endpoint_ref`: `codex:stdio` / `stdio` or path to ACP bridge binary.
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
        .strip_prefix("codex:")
        .or_else(|| endpoint_ref.strip_prefix("openai-codex:"))
        .unwrap_or(endpoint_ref);
    if s == "stdio" || s.is_empty() {
        None
    } else {
        Some(std::path::PathBuf::from(s))
    }
}

async fn open_raw(
    config: CodexAgentConfig,
    request: OpenConnection,
    control: ConnectionControlHandle,
    control_state: Arc<ControlState>,
) -> Result<OpenedRawConnection, monoloop_contracts::ConnectorError> {
    let mut agent = CodexAgentHandle::connect(config.clone())
        .await
        .map_err(|e| e.into_connector_error())?;

    let session = if let Some(ref ext) = request.external_session_id {
        agent
            .session_load(ext.as_str(), &config.cwd)
            .await
            .map_err(|e| e.into_connector_error())?
    } else {
        agent
            .session_new(CodexSessionConfig::new(&config.cwd))
            .await
            .map_err(|e| e.into_connector_error())?
    };

    let dialect = DialectBinding::negotiated(DialectDescriptor::codex_acp("1"));
    let max_chunk = request.limits.buffers.max_chunk_bytes.max(64 * 1024);
    let (in_tx, mut in_rx) = mpsc::channel::<RawInputMessage>(32);
    let (out_tx, out_rx) = mpsc::channel(config.max_output_queue);
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

    let mut updates = agent.take_updates();
    let out_pump = out_tx;
    tokio::spawn(async move {
        while let Some(bytes) = updates.recv().await {
            if out_pump.send(bytes).await.is_err() {
                break;
            }
        }
    });

    tokio::spawn(async move {
        let mut bytes_accepted = 0u64;
        while let Some(msg) = in_rx.recv().await {
            match msg {
                RawInputMessage::Bytes(b) => {
                    bytes_accepted += b.len() as u64;
                    let text = String::from_utf8_lossy(&b).into_owned();
                    if text.trim().is_empty() {
                        continue;
                    }
                    let _ = session.prompt_text(text).await;
                }
                RawInputMessage::Finish => break,
            }
        }
        agent.shutdown().await;
        let _ = end_tx.send(ConnectionEnd {
            connection_id: connection_id.clone(),
            kind: ConnectionEndKind::LocalShutdown,
            initiated_by: EndInitiator::LocalControl,
            bytes_accepted,
            bytes_received: 0,
            safe_transport_error: None,
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
