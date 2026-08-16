//! Cursor Agent ACP connector configuration.

use std::path::PathBuf;
use std::time::Duration;

/// How to launch the Cursor ACP server process.
#[derive(Clone, Debug)]
pub struct CursorAgentConfig {
    /// Path to the `agent` binary (default: `agent` on PATH, or `~/.local/bin/agent`).
    pub agent_bin: PathBuf,
    /// Working directory for the agent process and default session `cwd`.
    pub cwd: PathBuf,
    /// Extra args before `acp` (e.g. `--api-key`, `--mode ask`).
    pub extra_args: Vec<String>,
    /// Auth method id advertised by Cursor (`cursor_login`).
    pub auth_method_id: String,
    /// Client name reported in `initialize`.
    pub client_name: String,
    /// Client version reported in `initialize`.
    pub client_version: String,
    /// Handshake / RPC deadline.
    pub rpc_deadline: Duration,
    /// Max bytes per NDJSON line (fail-closed).
    pub max_line_bytes: usize,
    /// Bounded output queue for dialect bytes (session/update lines).
    pub max_output_queue: usize,
    /// Auto-answer `session/request_permission` with allow-once (tests / unattended).
    pub auto_allow_permissions: bool,
    /// When true, advertise client fs read/write capabilities (requires host handlers).
    pub advertise_fs: bool,
    /// Optional path to append raw NDJSON lines (test diagnostics only).
    pub raw_dump_path: Option<PathBuf>,
}

impl Default for CursorAgentConfig {
    fn default() -> Self {
        let home = std::env::var_os("HOME").map(PathBuf::from);
        let agent_bin = std::env::var_os("CURSOR_AGENT_BIN")
            .map(PathBuf::from)
            .or_else(|| home.map(|h| h.join(".local/bin/agent")))
            .unwrap_or_else(|| PathBuf::from("agent"));
        Self {
            agent_bin,
            cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            extra_args: Vec::new(),
            auth_method_id: "cursor_login".into(),
            client_name: "monoloop-cursor".into(),
            client_version: env!("CARGO_PKG_VERSION").into(),
            rpc_deadline: Duration::from_secs(60),
            max_line_bytes: 8 * 1024 * 1024,
            max_output_queue: 256,
            // Fail closed: hosts must opt in to auto-approve tool permissions.
            auto_allow_permissions: false,
            advertise_fs: false,
            raw_dump_path: None,
        }
    }
}

impl CursorAgentConfig {
    /// Config for a project directory with optional artifact dump path.
    pub fn for_project(cwd: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            ..Default::default()
        }
    }

    /// Attach a raw NDJSON dump path.
    pub fn with_raw_dump(mut self, path: impl Into<PathBuf>) -> Self {
        self.raw_dump_path = Some(path.into());
        self
    }

    /// Opt in to auto-answering `session/request_permission` with allow-once.
    ///
    /// Only for trusted test sandboxes / unattended qualification. Product hosts
    /// should inject an explicit permission policy instead.
    pub fn with_auto_allow_permissions(mut self) -> Self {
        self.auto_allow_permissions = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_denies_auto_permissions() {
        assert!(!CursorAgentConfig::default().auto_allow_permissions);
    }

    #[test]
    fn opt_in_enables_auto_permissions() {
        assert!(CursorAgentConfig::default()
            .with_auto_allow_permissions()
            .auto_allow_permissions);
    }
}

/// Session create parameters (`session/new` + optional mode/model).
#[derive(Clone, Debug)]
pub struct CursorSessionConfig {
    /// Session working directory.
    pub cwd: PathBuf,
    /// MCP servers (opaque JSON; usually empty for monoloop).
    pub mcp_servers: serde_json::Value,
    /// ACP mode after create: `agent` | `plan` | `ask` (via `session/set_mode`).
    pub mode_id: Option<String>,
    /// Model config option value (via `session/set_config_option` id=`model`).
    pub model_id: Option<String>,
}

impl CursorSessionConfig {
    /// Session in the given cwd with no MCP servers.
    pub fn new(cwd: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            mcp_servers: serde_json::json!([]),
            mode_id: None,
            model_id: None,
        }
    }

    /// Request ask mode (Q&A / no edits).
    pub fn with_ask_mode(mut self) -> Self {
        self.mode_id = Some("ask".into());
        self
    }

    /// Request plan mode (read-only planning).
    pub fn with_plan_mode(mut self) -> Self {
        self.mode_id = Some("plan".into());
        self
    }

    /// Request agent mode (full tools).
    pub fn with_agent_mode(mut self) -> Self {
        self.mode_id = Some("agent".into());
        self
    }

    /// Select a model config value advertised by `session/new` (e.g. `composer-2.5[fast=true]`).
    pub fn with_model(mut self, model_id: impl Into<String>) -> Self {
        self.model_id = Some(model_id.into());
        self
    }
}
