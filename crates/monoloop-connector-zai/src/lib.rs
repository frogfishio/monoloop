//! Z.ai CLI connector profile (`@guizmo-ai/zai-cli` / `zai`).
//!
//! **Not ACP.** Headless mode (`zai -p`) prints OpenAI-compatible chat messages
//! as NDJSON on stdout after the agent turn. Tools run inside the CLI (auto-approved
//! headless). Monoloop maps the transcript via `DialectFamily::ZaiCli`.
//!
//! Auth: CLI settings / `ZAI_API_KEY` — never logged. Prompt is passed on argv via
//! `-p` (CLI contract); do not put secrets in the prompt string.
//!
//! Session correlation: synthetic `zai-<uuid>` per headless run (no ambient resume).

#![deny(missing_docs)]

mod channel_binding;
mod config;
mod error;
mod raw_dump;
mod run;

pub use channel_binding::{zai_channel_binding, ZaiConnectorFactory};
pub use config::ZaiAgentConfig;
pub use error::ZaiConnectorError;
pub use raw_dump::ZaiRawDump;
pub use run::{run_headless_prompt, ZaiRunOutcome};

use monoloop_connector::{
    ConnectionCompletionHandle, ConnectionControlHandle, ConnectionEnd, ConnectionEndKind,
    Connector, ConnectorDescriptor, ControlState, EndInitiator, OpenConnection, OpenedRawConnection,
    PendingRawConnection, RawInputHandle, RawInputMessage, RawOutputHandle,
};
use monoloop_contracts::{DialectBinding, DialectDescriptor};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Factory for Z.ai CLI headless connections.
pub struct ZaiConnector {
    descriptor: ConnectorDescriptor,
    default_config: ZaiAgentConfig,
}

impl ZaiConnector {
    /// Create with default `zai` discovery.
    pub fn new() -> Self {
        Self {
            descriptor: ConnectorDescriptor::zai_cli(),
            default_config: ZaiAgentConfig::default(),
        }
    }

    /// Create with an explicit base config.
    pub fn with_config(config: ZaiAgentConfig) -> Self {
        Self {
            descriptor: ConnectorDescriptor::zai_cli(),
            default_config: config,
        }
    }

    /// Run one headless prompt; stream NDJSON lines on the returned channel.
    pub async fn run_prompt(
        &self,
        config: ZaiAgentConfig,
        prompt: impl AsRef<str>,
    ) -> Result<(mpsc::Receiver<bytes::Bytes>, ZaiRunOutcome), ZaiConnectorError> {
        let capacity = config.max_stdout_bytes.clamp(16, 256);
        let (tx, rx) = mpsc::channel(capacity);
        let outcome = run_headless_prompt(&config, prompt.as_ref(), tx).await?;
        Ok((rx, outcome))
    }
}

impl Default for ZaiConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Connector for ZaiConnector {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }

    /// `endpoint_ref`: `zai:stdio` / `stdio` or path to `zai` binary.
    ///
    /// First `RawInputMessage::Bytes` is the prompt; process runs once then ends.
    fn begin_open(&self, request: OpenConnection) -> PendingRawConnection {
        let mut config = self.default_config.clone();
        if let Some(path) = parse_endpoint(&request.endpoint_ref) {
            config.command = path;
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
        .strip_prefix("zai:")
        .or_else(|| endpoint_ref.strip_prefix("z.ai:"))
        .unwrap_or(endpoint_ref);
    if s == "stdio" || s.is_empty() {
        None
    } else {
        Some(std::path::PathBuf::from(s))
    }
}

async fn open_raw(
    config: ZaiAgentConfig,
    request: OpenConnection,
    control: ConnectionControlHandle,
    control_state: Arc<ControlState>,
) -> Result<OpenedRawConnection, monoloop_contracts::ConnectorError> {
    let dialect = DialectBinding::negotiated(DialectDescriptor::zai_cli("1"));
    let max_chunk = request.limits.buffers.max_chunk_bytes.max(64 * 1024);
    let (in_tx, mut in_rx) = mpsc::channel::<RawInputMessage>(8);
    let (out_tx, out_rx) = mpsc::channel(64);
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
    let external_session_id = request.external_session_id.clone();

    tokio::spawn(async move {
        let mut prompt = String::new();
        let mut bytes_accepted = 0u64;
        while let Some(msg) = in_rx.recv().await {
            match msg {
                RawInputMessage::Bytes(b) => {
                    bytes_accepted += b.len() as u64;
                    prompt.push_str(&String::from_utf8_lossy(&b));
                }
                RawInputMessage::Finish => break,
            }
        }
        let prompt = prompt.trim().to_string();
        if !prompt.is_empty() {
            if let Err(e) = run_headless_prompt(&config, &prompt, out_tx).await {
                tracing::debug!(target: "zai_agent", "run failed: {e}");
            }
        }
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
