//! Claude Code connector profile (`claude` CLI).
//!
//! **Not ACP.** Headless print mode:
//! `claude -p --output-format stream-json --verbose` emits NDJSON events.
//! Tools run inside Claude Code; Monoloop maps the stream via
//! `DialectFamily::ClaudeCode`.
//!
//! Auth: existing Claude Code login / `ANTHROPIC_API_KEY` — never logged.
//! Prompt is a CLI positional argument (Claude contract).
//! Session correlation: `session_id` from stream `system` init when present.

#![deny(missing_docs)]

mod config;
mod error;
mod raw_dump;
mod run;

pub use config::ClaudeAgentConfig;
pub use error::ClaudeConnectorError;
pub use raw_dump::ClaudeRawDump;
pub use run::{run_claude_print, ClaudeRunOutcome};

use monoloop_connector::{
    ConnectionCompletionHandle, ConnectionControlHandle, ConnectionEnd, ConnectionEndKind,
    Connector, ConnectorDescriptor, ControlState, EndInitiator, OpenConnection, OpenedRawConnection,
    PendingRawConnection, RawInputHandle, RawInputMessage, RawOutputHandle,
};
use monoloop_contracts::{DialectBinding, DialectDescriptor};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Factory for Claude Code headless print connections.
pub struct ClaudeConnector {
    descriptor: ConnectorDescriptor,
    default_config: ClaudeAgentConfig,
}

impl ClaudeConnector {
    /// Create with default `claude` discovery.
    pub fn new() -> Self {
        Self {
            descriptor: ConnectorDescriptor::claude_code(),
            default_config: ClaudeAgentConfig::default(),
        }
    }

    /// Create with an explicit base config.
    pub fn with_config(config: ClaudeAgentConfig) -> Self {
        Self {
            descriptor: ConnectorDescriptor::claude_code(),
            default_config: config,
        }
    }

    /// Run one headless print prompt; stream NDJSON lines on the returned channel.
    pub async fn run_prompt(
        &self,
        config: ClaudeAgentConfig,
        prompt: impl AsRef<str>,
    ) -> Result<(mpsc::Receiver<bytes::Bytes>, ClaudeRunOutcome), ClaudeConnectorError> {
        let (tx, rx) = mpsc::channel(config.max_stdout_bytes.min(256).max(16));
        let outcome = run_claude_print(&config, prompt.as_ref(), tx).await?;
        Ok((rx, outcome))
    }
}

impl Default for ClaudeConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Connector for ClaudeConnector {
    fn descriptor(&self) -> &ConnectorDescriptor {
        &self.descriptor
    }

    /// `endpoint_ref`: `claude:stdio` / `stdio` or path to `claude` binary.
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
        .strip_prefix("claude:")
        .or_else(|| endpoint_ref.strip_prefix("claude-code:"))
        .unwrap_or(endpoint_ref);
    if s == "stdio" || s.is_empty() {
        None
    } else {
        Some(std::path::PathBuf::from(s))
    }
}

async fn open_raw(
    config: ClaudeAgentConfig,
    request: OpenConnection,
    control: ConnectionControlHandle,
    control_state: Arc<ControlState>,
) -> Result<OpenedRawConnection, monoloop_contracts::ConnectorError> {
    let dialect = DialectBinding::negotiated(DialectDescriptor::claude_code("1"));
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
            if let Err(e) = run_claude_print(&config, &prompt, out_tx).await {
                tracing::debug!(target: "claude_agent", "run failed: {e}");
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
