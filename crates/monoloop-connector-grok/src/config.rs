//! Grok server and session configuration (no prompt bodies).

use crate::secret::SecretRef;
use std::net::IpAddr;
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
}

impl GrokServerConfig {
    /// Build config for a loopback endpoint.
    pub fn loopback(port: u16, secret_ref: SecretRef) -> Result<Self, url::ParseError> {
        let endpoint = Url::parse(&format!("ws://127.0.0.1:{port}"))?;
        Ok(Self {
            websocket_endpoint: endpoint,
            authentication_secret_ref: secret_ref,
            expected_acp_version: "1".into(),
            allow_non_loopback: false,
            limits: GrokConnectorLimits::default(),
        })
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
    pub fn to_params(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        if let Some(cwd) = &self.cwd {
            map.insert("cwd".into(), serde_json::Value::String(cwd.clone()));
        }
        if !self.mcp_servers.is_empty() {
            map.insert(
                "mcpServers".into(),
                serde_json::Value::Array(self.mcp_servers.clone()),
            );
        }
        if let Some(mode) = &self.permission_mode {
            map.insert(
                "permissionMode".into(),
                serde_json::Value::String(mode.clone()),
            );
        }
        if let Some(profile) = &self.agent_profile {
            map.insert(
                "agentProfile".into(),
                serde_json::Value::String(profile.clone()),
            );
        }
        if let Some(meta) = &self.extension_metadata {
            map.insert("_meta".into(), meta.clone());
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
