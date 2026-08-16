//! Antigravity ACP session lifecycle on one agent process.

use crate::config::{AgyAgentConfig, AgySessionConfig};
use crate::error::AgyConnectorError;
use crate::process::ProcessInner;
use crate::raw_dump::AgyRawDump;
use monoloop_contracts::{DialectBinding, DialectDescriptor, ExternalSessionId};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Live Antigravity ACP process + optional session.
pub struct AgyAgentHandle {
    inner: Arc<ProcessInner>,
    updates: Option<mpsc::Receiver<bytes::Bytes>>,
    dump: Arc<AgyRawDump>,
}

impl AgyAgentHandle {
    /// Spawn ACP server (`agy-acp` / native), run initialize (+ optional authenticate).
    pub async fn connect(config: AgyAgentConfig) -> Result<Self, AgyConnectorError> {
        let dump = Arc::new(AgyRawDump::new(config.raw_dump_path.clone(), 10_000));
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

        if config.authenticate {
            let _ = inner
                .request(
                    "authenticate",
                    serde_json::json!({ "methodId": config.auth_method_id }),
                )
                .await?;
        }

        Ok(Self {
            inner,
            updates: Some(updates),
            dump,
        })
    }

    /// Take the session/update stream (once).
    pub fn take_updates(&mut self) -> mpsc::Receiver<bytes::Bytes> {
        self.updates
            .take()
            .expect("AgyAgentHandle updates already taken")
    }

    /// Create a new session (`session/new`).
    pub async fn session_new(
        &self,
        config: AgySessionConfig,
    ) -> Result<AgySession, AgyConnectorError> {
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
            .ok_or_else(|| AgyConnectorError::session("session/new missing sessionId"))?
            .to_string();
        let session = AgySession {
            session_id,
            inner: Arc::clone(&self.inner),
        };
        if let Some(mode) = &config.mode_id {
            // Best-effort: some bridges support set_mode; ignore protocol errors.
            let _ = session.set_mode(mode).await;
        }
        Ok(session)
    }

    /// Explicit session load (no most-recent heuristic).
    pub async fn session_load(
        &self,
        session_id: impl Into<String>,
        cwd: impl AsRef<std::path::Path>,
    ) -> Result<AgySession, AgyConnectorError> {
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
        Ok(AgySession {
            session_id: sid,
            inner: Arc::clone(&self.inner),
        })
    }

    /// Dialect binding for Interpreter.
    pub fn dialect(&self) -> DialectBinding {
        DialectBinding::negotiated(DialectDescriptor::agy_acp("1"))
    }

    /// Raw dump snapshot text.
    pub fn raw_dump_text(&self) -> String {
        self.dump.as_text()
    }

    /// Shared dump handle.
    pub fn raw_dump(&self) -> Arc<AgyRawDump> {
        Arc::clone(&self.dump)
    }

    /// Shut down the ACP process.
    pub async fn shutdown(self) {
        self.inner.shutdown().await;
    }
}

/// One Antigravity session on an ACP process.
pub struct AgySession {
    /// Authoritative session id.
    pub session_id: String,
    inner: Arc<ProcessInner>,
}

impl AgySession {
    /// Opaque external session id for Monoloop envelopes.
    pub fn external_session_id(&self) -> ExternalSessionId {
        ExternalSessionId::new(self.session_id.clone())
    }

    /// Set session mode when supported (`default` | `accept-edits` | `plan`).
    pub async fn set_mode(&self, mode_id: impl AsRef<str>) -> Result<(), AgyConnectorError> {
        self.inner
            .request(
                "session/set_mode",
                serde_json::json!({
                    "sessionId": self.session_id,
                    "modeId": mode_id.as_ref(),
                }),
            )
            .await?;
        Ok(())
    }

    /// Send `session/prompt` and wait for terminal RPC result.
    pub async fn prompt_text(
        &self,
        text: impl Into<String>,
    ) -> Result<serde_json::Value, AgyConnectorError> {
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
    pub async fn cancel(&self) -> Result<(), AgyConnectorError> {
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
