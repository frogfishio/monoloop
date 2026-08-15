//! Grok server and session configuration (no prompt bodies).

use crate::raw_dump::RawDumpCollector;
use crate::secret::SecretRef;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// Aggregate connector limits for the Grok profile.
#[derive(Clone, Debug)]
pub struct GrokConnectorLimits {
    /// Connect + handshake deadline.
    pub connect_deadline: Duration,
    /// Per JSON-RPC request deadline.
    pub request_deadline: Duration,
    /// Maximum WebSocket message bytes.
    pub max_message_bytes: usize,
    /// Maximum pending JSON-RPC requests.
    pub max_pending_rpc: usize,
    /// Maximum sessions on this connector instance.
    pub max_sessions: usize,
    /// Maximum queued outbound session messages per session.
    pub max_queued_prompts_per_session: usize,
    /// Maximum queued inbound update messages per session.
    pub max_queued_inbound_per_session: usize,
    /// Maximum concurrent active prompts across sessions.
    pub max_concurrent_prompts: usize,
}

impl Default for GrokConnectorLimits {
    fn default() -> Self {
        Self {
            connect_deadline: Duration::from_secs(30),
            request_deadline: Duration::from_secs(60),
            max_message_bytes: 4 * 1024 * 1024,
            max_pending_rpc: 256,
            max_sessions: 128,
            max_queued_prompts_per_session: 16,
            max_queued_inbound_per_session: 256,
            max_concurrent_prompts: 64,
        }
    }
}

/// Server connection configuration.
#[derive(Clone, Debug)]
pub struct GrokServerConfig {
    /// WebSocket endpoint (`ws://` or `wss://`).
    pub websocket_endpoint: Url,
    /// Authentication secret reference (resolved at connect).
    pub authentication_secret_ref: SecretRef,
    /// Expected ACP protocol version (e.g. `"1"` or `"v1"`).
    pub expected_acp_version: String,
    /// When true, non-loopback endpoints require explicit opt-in.
    pub allow_non_loopback: bool,
    /// Limits.
    pub limits: GrokConnectorLimits,
    /// Optional raw inbound dump (exact WebSocket payloads from Grok).
    ///
    /// Opt-in only. When set, every complete inbound frame is recorded **before**
    /// demux. Secrets used for auth are never stored here.
    pub raw_dump: Option<Arc<RawDumpCollector>>,
}

impl GrokServerConfig {
    /// Build config for a loopback endpoint.
    pub fn loopback(port: u16, secret_ref: SecretRef) -> Result<Self, url::ParseError> {
        // Grok agent serve advertises ws://127.0.0.1:PORT/ws
        let endpoint = Url::parse(&format!("ws://127.0.0.1:{port}/ws"))?;
        Ok(Self {
            websocket_endpoint: endpoint,
            authentication_secret_ref: secret_ref,
            expected_acp_version: "1".into(),
            allow_non_loopback: false,
            limits: GrokConnectorLimits::default(),
            raw_dump: None,
        })
    }

    /// Enable raw inbound dump with default bounds.
    pub fn with_raw_dump(mut self, dump: Arc<RawDumpCollector>) -> Self {
        self.raw_dump = Some(dump);
        self
    }

    /// Validate security policy for the endpoint (fail closed for non-loopback).
    pub fn validate_endpoint_security(&self) -> Result<(), crate::error::GrokConnectorError> {
        let host = self.websocket_endpoint.host_str().ok_or_else(|| {
            crate::error::GrokConnectorError::configuration("websocket endpoint missing host")
        })?;
        let is_loopback = host == "localhost"
            || host.parse::<IpAddr>().map(|ip| ip.is_loopback()).unwrap_or(false);
        if !is_loopback && !self.allow_non_loopback {
            return Err(crate::error::GrokConnectorError::configuration(
                "non-loopback endpoint requires allow_non_loopback=true and authenticated transport policy",
            ));
        }
        let scheme = self.websocket_endpoint.scheme();
        if scheme != "ws" && scheme != "wss" {
            return Err(crate::error::GrokConnectorError::configuration(
                "websocket endpoint must use ws or wss scheme",
            ));
        }
        if !is_loopback && scheme != "wss" && !self.allow_non_loopback {
            return Err(crate::error::GrokConnectorError::configuration(
                "non-loopback requires wss",
            ));
        }
        Ok(())
    }
}

/// Session creation configuration (`session/new` params; no prompt).
#[derive(Clone, Debug, Default)]
pub struct GrokSessionConfig {
    /// Working directory for the session (agent host path).
    pub cwd: Option<String>,
    /// MCP server descriptors (opaque JSON objects, already bounded by caller).
    pub mcp_servers: Vec<serde_json::Value>,
    /// Permission mode label (e.g. `"default"`, `"approve-all"` for tests only when declared).
    pub permission_mode: Option<String>,
    /// Optional agent profile name.
    pub agent_profile: Option<String>,
    /// Extra extension metadata (must be safe/bounded; not logged by connector).
    pub extension_metadata: Option<serde_json::Value>,
}

impl GrokSessionConfig {
    /// Serialize parameters for `session/new` (no prompt field).
    ///
    /// Matches the Grok ACP client shape: `cwd`, `mcpServers` (always present),
    /// and optional `_meta` (yoloMode / agentProfile live under `_meta`, not top-level).
    pub fn to_params(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        if let Some(cwd) = &self.cwd {
            map.insert("cwd".into(), serde_json::Value::String(cwd.clone()));
        }
        // ACP clients always send mcpServers (may be empty).
        map.insert(
            "mcpServers".into(),
            serde_json::Value::Array(self.mcp_servers.clone()),
        );

        let mut meta = match &self.extension_metadata {
            Some(serde_json::Value::Object(m)) => m.clone(),
            Some(other) => {
                let mut m = serde_json::Map::new();
                m.insert("value".into(), other.clone());
                m
            }
            None => serde_json::Map::new(),
        };
        if let Some(mode) = &self.permission_mode {
            // Map friendly labels into Grok's _meta.yoloMode rather than a top-level field.
            let yolo = matches!(
                mode.as_str(),
                "always-approve" | "always_approve" | "yolo" | "bypassPermissions"
            );
            if yolo {
                meta.insert("yoloMode".into(), serde_json::Value::Bool(true));
            }
        }
        if let Some(profile) = &self.agent_profile {
            meta.insert(
                "agentProfile".into(),
                serde_json::Value::String(profile.clone()),
            );
        }
        if !meta.is_empty() {
            map.insert("_meta".into(), serde_json::Value::Object(meta));
        }
        serde_json::Value::Object(map)
    }
}

/// Load configuration for `session/load`.
#[derive(Clone, Debug, Default)]
pub struct GrokSessionLoadConfig {
    /// Optional cwd for load (if required by agent).
    pub cwd: Option<String>,
}

impl GrokSessionLoadConfig {
    /// Serialize parameters for `session/load`.
    pub fn to_params(&self, session_id: &str) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert(
            "sessionId".into(),
            serde_json::Value::String(session_id.to_string()),
        );
        if let Some(cwd) = &self.cwd {
            map.insert("cwd".into(), serde_json::Value::String(cwd.clone()));
        }
        serde_json::Value::Object(map)
    }
}
