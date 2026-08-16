//! Cursor ACP session lifecycle on one agent process.

use crate::config::{CursorAgentConfig, CursorSessionConfig};
use crate::error::CursorConnectorError;
use crate::process::ProcessInner;
use crate::raw_dump::CursorRawDump;
use monoloop_contracts::{
    DialectBinding, DialectDescriptor, ExternalSessionId,
};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Live Cursor ACP process + optional session.
pub struct CursorAgentHandle {
    inner: Arc<ProcessInner>,
    /// Session update NDJSON receiver (complete JSON objects + newline).
    updates: Option<mpsc::Receiver<bytes::Bytes>>,
    dump: Arc<CursorRawDump>,
}

impl CursorAgentHandle {
    /// Spawn `agent acp`, run initialize + authenticate.
    pub async fn connect(config: CursorAgentConfig) -> Result<Self, CursorConnectorError> {
        let dump = Arc::new(CursorRawDump::new(
            config.raw_dump_path.clone(),
            10_000,
        ));
        let (update_tx, updates) = mpsc::channel(config.max_output_queue);
        let inner = ProcessInner::spawn(config.clone(), update_tx, Arc::clone(&dump)).await?;

        let fs = if config.advertise_fs {
            serde_json::json!({ "readTextFile": true, "writeTextFile": true })
        } else {
            serde_json::json!({ "readTextFile": false, "writeTextFile": false })
        };

        inner
            .request(
                "initialize",
                serde_json::json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "fs": fs,
                        "terminal": false
                    },
                    "clientInfo": {
                        "name": config.client_name,
                        "version": config.client_version
                    }
                }),
            )
            .await?;

        // Cursor docs: authenticate with cursor_login (pre-auth via agent login / env).
        let _ = inner
            .request(
                "authenticate",
                serde_json::json!({ "methodId": config.auth_method_id }),
            )
            .await?;

        Ok(Self {
            inner,
            updates: Some(updates),
            dump,
        })
    }

    /// Take the session/update stream (once). Caller owns the receiver.
    pub fn take_updates(&mut self) -> mpsc::Receiver<bytes::Bytes> {
        self.updates
            .take()
            .expect("CursorAgentHandle updates already taken")
    }

    /// Create a new session (`session/new`). Returns Cursor sessionId.
    ///
    /// When `mode_id` / `model_id` are set, applies them via ACP
    /// `session/set_mode` and `session/set_config_option` (no ambient default).
    pub async fn session_new(
        &self,
        config: CursorSessionConfig,
    ) -> Result<CursorSession, CursorConnectorError> {
        let result = self
            .inner
            .request(
                "session/new",
                serde_json::json!({
                    "cwd": config.cwd.to_string_lossy(),
                    "mcpServers": config.mcp_servers,
                }),
            )
            .await?;
        let session_id = result
            .get("sessionId")
            .and_then(|s| s.as_str())
            .ok_or_else(|| CursorConnectorError::session("session/new missing sessionId"))?
            .to_string();
        let session = CursorSession {
            session_id,
            inner: Arc::clone(&self.inner),
        };
        if let Some(mode) = &config.mode_id {
            session.set_mode(mode).await?;
        }
        if let Some(model) = &config.model_id {
            session.set_model(model).await?;
        }
        Ok(session)
    }

    /// Explicit session load (no most-recent heuristic).
    pub async fn session_load(
        &self,
        session_id: impl Into<String>,
        cwd: impl AsRef<std::path::Path>,
    ) -> Result<CursorSession, CursorConnectorError> {
        let session_id = session_id.into();
        let result = self
            .inner
            .request(
                "session/load",
                serde_json::json!({
                    "sessionId": session_id,
                    "cwd": cwd.as_ref().to_string_lossy(),
                    "mcpServers": [],
                }),
            )
            .await?;
        let sid = result
            .get("sessionId")
            .and_then(|s| s.as_str())
            .unwrap_or(&session_id)
            .to_string();
        Ok(CursorSession {
            session_id: sid,
            inner: Arc::clone(&self.inner),
        })
    }

    /// Dialect binding for Interpreter (Cursor ACP NDJSON profile).
    pub fn dialect(&self) -> DialectBinding {
        DialectBinding::negotiated(DialectDescriptor::cursor_acp("1"))
    }

    /// Raw dump snapshot text.
    pub fn raw_dump_text(&self) -> String {
        self.dump.as_text()
    }

    /// Shared dump handle.
    pub fn raw_dump(&self) -> Arc<CursorRawDump> {
        Arc::clone(&self.dump)
    }

    /// Shut down the agent process.
    pub async fn shutdown(self) {
        self.inner.shutdown().await;
    }
}

/// One Cursor session on an agent process.
pub struct CursorSession {
    /// Authoritative Cursor `sessionId`.
    pub session_id: String,
    inner: Arc<ProcessInner>,
}

impl CursorSession {
    /// Opaque external session id for Monoloop envelopes.
    pub fn external_session_id(&self) -> ExternalSessionId {
        ExternalSessionId::new(self.session_id.clone())
    }

    /// Set session mode (`agent` | `plan` | `ask`) via `session/set_mode`.
    pub async fn set_mode(&self, mode_id: impl AsRef<str>) -> Result<(), CursorConnectorError> {
        let mode_id = mode_id.as_ref();
        self.inner
            .request(
                "session/set_mode",
                serde_json::json!({
                    "sessionId": self.session_id,
                    "modeId": mode_id,
                }),
            )
            .await?;
        Ok(())
    }

    /// Set the model config option via `session/set_config_option` (`configId=model`).
    pub async fn set_model(&self, model_id: impl AsRef<str>) -> Result<(), CursorConnectorError> {
        let model_id = model_id.as_ref();
        self.inner
            .request(
                "session/set_config_option",
                serde_json::json!({
                    "sessionId": self.session_id,
                    "configId": "model",
                    "value": model_id,
                }),
            )
            .await?;
        Ok(())
    }

    /// Set an arbitrary advertised config option by id.
    pub async fn set_config_option(
        &self,
        config_id: impl AsRef<str>,
        value: impl Into<serde_json::Value>,
    ) -> Result<serde_json::Value, CursorConnectorError> {
        self.inner
            .request(
                "session/set_config_option",
                serde_json::json!({
                    "sessionId": self.session_id,
                    "configId": config_id.as_ref(),
                    "value": value.into(),
                }),
            )
            .await
    }

    /// Send `session/prompt` and wait for the terminal RPC result (`stopReason`).
    ///
    /// Streaming `session/update` notifications continue on the agent handle's
    /// `updates` channel while this future is pending.
    pub async fn prompt_text(
        &self,
        text: impl Into<String>,
    ) -> Result<serde_json::Value, CursorConnectorError> {
        let text = text.into();
        self.inner
            .request(
                "session/prompt",
                serde_json::json!({
                    "sessionId": self.session_id,
                    "prompt": [{ "type": "text", "text": text }]
                }),
            )
            .await
    }

    /// Cooperative cancel for this session.
    pub async fn cancel(&self) -> Result<(), CursorConnectorError> {
        // ACP cancel is often a notification; send as request if supported.
        let _ = self
            .inner
            .request(
                "session/cancel",
                serde_json::json!({ "sessionId": self.session_id }),
            )
            .await;
        Ok(())
    }
}
